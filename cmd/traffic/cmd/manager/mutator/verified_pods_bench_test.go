package mutator

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"

	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

type syncedBenchmarkInformer struct{ cache.SharedIndexInformer }

func (syncedBenchmarkInformer) HasSynced() bool { return true }

func benchmarkInformer(obj runtime.Object) *observedInformer {
	index := cache.NewSharedIndexInformer(nil, obj, 0, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	return &observedInformer{SharedIndexInformer: syncedBenchmarkInformer{index}, now: time.Now}
}

func BenchmarkVerifiedPods(b *testing.B) {
	for _, tc := range []struct {
		pods, workloads int
		drift           bool
	}{
		{256, 1, false}, {256, 16, false}, {4096, 1, false}, {4096, 16, false}, {4096, 16, true},
	} {
		b.Run(fmt.Sprintf("pods=%d/workloads=%d/drift=%t", tc.pods, tc.workloads, tc.drift), func(b *testing.B) {
			pods := benchmarkInformer(&core.Pod{})
			deployments := benchmarkInformer(&apps.Deployment{})
			replicasets := benchmarkInformer(&apps.ReplicaSet{})
			v := &podInventory{pods: pods, owners: map[k8sapi.Kind]*observedInformer{k8sapi.DeploymentKind: deployments, k8sapi.ReplicaSetKind: replicasets}}
			c := NewWatcher().(*configWatcher)
			c.informers.Store("", &informersWithCancel{inventory: v})
			type target struct {
				wl     k8sapi.Workload
				config string
			}
			targets := make([]target, 0, tc.workloads)
			for n := range tc.workloads {
				name := fmt.Sprintf("work-%d", n)
				wl, obj := reconciliationTestWorkload(k8sapi.DeploymentKind, name, "default")
				dep := obj.(*apps.Deployment)
				dep.Spec.Selector.MatchLabels = map[string]string{"app": name}
				dep.ResourceVersion = "1"
				require.NoError(b, deployments.GetStore().Add(dep))
				config := reconciliationTestConfig(wl, agentconfig.ReplacePolicyIntercept)
				c.Store(config)
				encoded, err := agentconfig.MarshalTight(config)
				require.NoError(b, err)
				targets = append(targets, target{wl, encoded})
				if tc.drift && n == tc.workloads-1 {
					encoded = ""
				}
				pod := reconciliationTestPod(wl, "0", encoded, true)
				pod.Labels = dep.Spec.Selector.MatchLabels
				rs := &apps.ReplicaSet{ObjectMeta: meta.ObjectMeta{Name: name + "-rs", Namespace: "default", UID: types.UID(name + "-rs"), OwnerReferences: pod.OwnerReferences}}
				pod.OwnerReferences = []meta.OwnerReference{*meta.NewControllerRef(rs, apps.SchemeGroupVersion.WithKind("ReplicaSet"))}
				require.NoError(b, replicasets.GetStore().Add(rs))
				require.NoError(b, pods.GetStore().Add(pod))
			}
			for n := tc.workloads; n < tc.pods; n++ {
				require.NoError(b, pods.GetStore().Add(&core.Pod{ObjectMeta: meta.ObjectMeta{Name: fmt.Sprintf("other-%d", n), Namespace: "default", Labels: map[string]string{"app": "other"}}}))
			}
			for _, i := range []*observedInformer{pods, deployments, replicasets} {
				i.position = streamPosition{epoch: 1, version: "9", bookmark: time.Now()}
				i.GetStore().Bookmark(i.position.version)
			}

			b.ReportAllocs()
			hits := 0
			for b.Loop() {
				for _, target := range targets {
					if len(c.VerifiedPods(target.wl, target.config)) != 0 {
						hits++
					}
				}
			}
			hitsPerOp := tc.workloads
			if tc.drift {
				hitsPerOp--
			}
			require.Equal(b, hitsPerOp*b.N, hits)
			b.ReportMetric(float64(hits)/float64(b.N), "verified/op")
		})
	}
}
