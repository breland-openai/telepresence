package mutator

import (
	"context"
	"sync"
	"time"

	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/telepresenceio/telepresence/v2/pkg/annotation"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

const maxBookmarkAge = 90 * time.Second

type streamPosition struct {
	epoch, events uint64
	version       string
	bookmark      time.Time
}

type observedInformer struct {
	cache.SharedIndexInformer
	mu       sync.Mutex
	position streamPosition
	now      func() time.Time
}

func (i *observedInformer) checkpoint() (streamPosition, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	p := i.position
	return p, p.version != "" && !p.bookmark.IsZero() && i.now().Sub(p.bookmark) <= maxBookmarkAge &&
		i.HasSynced() && p.version == i.GetStore().LastStoreSyncResourceVersion()
}

func (i *observedInformer) observe(ctx context.Context, upstream watch.Interface, initialEvents bool) watch.Interface {
	i.mu.Lock()
	epoch := i.position.epoch + 1
	i.position = streamPosition{epoch: epoch}
	i.mu.Unlock()

	events := make(chan watch.Event)
	proxy := watch.NewProxyWatcher(events)
	go func() {
		defer close(events)
		defer upstream.Stop()
		defer func() {
			i.mu.Lock()
			if i.position.epoch == epoch {
				i.position.version = ""
			}
			i.mu.Unlock()
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-proxy.StopChan():
				return
			case event, ok := <-upstream.ResultChan():
				if !ok {
					return
				}
				i.mu.Lock()
				if i.position.epoch == epoch {
					p := &i.position
					p.events++
					obj, ok := event.Object.(meta.Object)
					if !ok || event.Type == watch.Error {
						p.version = ""
						p.bookmark = time.Time{}
					} else {
						p.version = obj.GetResourceVersion()
						if event.Type == watch.Bookmark {
							if obj.GetAnnotations()[meta.InitialEventsAnnotationKey] == "true" {
								initialEvents = false
							}
							if !initialEvents {
								p.bookmark = i.now()
							}
						}
					}
				}
				i.mu.Unlock()
				select {
				case <-ctx.Done():
					return
				case <-proxy.StopChan():
					return
				case events <- event:
				}
			}
		}
	}()
	return proxy
}

type listWatchResource[L runtime.Object] interface {
	List(context.Context, meta.ListOptions) (L, error)
	Watch(context.Context, meta.ListOptions) (watch.Interface, error)
}

// Register with the shared factory before any consumer requests this type.
// An already registered informer cannot provide stream positions.
func observeResource[L runtime.Object](factory informers.SharedInformerFactory, obj runtime.Object, resource listWatchResource[L]) *observedInformer {
	var observed *observedInformer
	factory.InformerFor(obj, func(client kubernetes.Interface, period time.Duration) cache.SharedIndexInformer {
		observed = &observedInformer{now: time.Now}
		lw := &cache.ListWatch{
			ListWithContextFunc: func(ctx context.Context, opts meta.ListOptions) (runtime.Object, error) {
				return resource.List(ctx, opts)
			},
			WatchFuncWithContext: func(ctx context.Context, opts meta.ListOptions) (watch.Interface, error) {
				w, err := resource.Watch(ctx, opts)
				if err != nil {
					return nil, err
				}
				return observed.observe(ctx, w, opts.SendInitialEvents != nil && *opts.SendInitialEvents), nil
			},
		}
		observed.SharedIndexInformer = cache.NewSharedIndexInformer(cache.ToListWatcherWithWatchListSemantics(lw, client),
			obj, period, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
		return observed.SharedIndexInformer
	})
	return observed
}

type podInventory struct {
	pods   *observedInformer
	owners map[k8sapi.Kind]*observedInformer
}

func observeInventory(factory informers.SharedInformerFactory, ki kubernetes.Interface, ns string, kinds k8sapi.Kinds) *podInventory {
	v := &podInventory{
		pods:   observeResource(factory, &core.Pod{}, ki.CoreV1().Pods(ns)),
		owners: make(map[k8sapi.Kind]*observedInformer),
	}
	for _, kind := range kinds {
		switch kind {
		case k8sapi.DeploymentKind:
			v.owners[kind] = observeResource(factory, &apps.Deployment{}, ki.AppsV1().Deployments(ns))
		case k8sapi.ReplicaSetKind:
			v.owners[kind] = observeResource(factory, &apps.ReplicaSet{}, ki.AppsV1().ReplicaSets(ns))
		case k8sapi.StatefulSetKind:
			v.owners[kind] = observeResource(factory, &apps.StatefulSet{}, ki.AppsV1().StatefulSets(ns))
		}
	}
	return v
}

func (v *podInventory) owned(wl k8sapi.Workload, pod *core.Pod) bool {
	ref := meta.GetControllerOf(pod)
	if ref == nil {
		return false
	}
	kind, ok := liveControllerKind(*ref)
	if ok && kind == k8sapi.ReplicaSetKind && wl.GetKind() == k8sapi.DeploymentKind {
		rs, found, err := v.owners[kind].GetStore().GetByKey(pod.Namespace + "/" + ref.Name)
		if err != nil || !found || rs.(*apps.ReplicaSet).UID != ref.UID || rs.(*apps.ReplicaSet).DeletionTimestamp != nil {
			return false
		}
		ref = meta.GetControllerOf(rs.(*apps.ReplicaSet))
		if ref == nil {
			return false
		}
		kind, ok = liveControllerKind(*ref)
	}
	return ok && kind == wl.GetKind() && ref.Name == wl.GetName() && ref.UID == wl.GetUID()
}

func (v *podInventory) matching(wl k8sapi.Workload, config string) map[types.UID]string {
	guard := []*observedInformer{v.pods, v.owners[wl.GetKind()]}
	if wl.GetKind() == k8sapi.DeploymentKind {
		guard = append(guard, v.owners[k8sapi.ReplicaSetKind])
	}
	before := make([]streamPosition, len(guard))
	for n, informer := range guard {
		if informer == nil {
			return nil
		}
		p, ok := informer.checkpoint()
		if !ok {
			return nil
		}
		before[n] = p
	}
	raw, exists, err := guard[1].GetStore().GetByKey(wl.GetNamespace() + "/" + wl.GetName())
	if err != nil || !exists {
		return nil
	}
	current, err := k8sapi.WrapWorkload(raw.(runtime.Object))
	if err != nil || current.GetUID() != wl.GetUID() || current.GetResourceVersion() != wl.GetResourceVersion() ||
		current.GetDeletionTimestamp() != nil || workloadRolloutInProgress(current) {
		return nil
	}
	desired := k8sapi.DesiredReplicas(current)
	selector, err := current.Selector()
	if err != nil || desired < 1 {
		return nil
	}
	pods := make(map[types.UID]string)
	complete := true
	err = cache.ListAllByNamespace(v.pods.GetIndexer(), wl.GetNamespace(), selector, func(o any) {
		pod := o.(*core.Pod)
		if !podIsPendingOrRunning(pod) {
			return
		}
		if pod.UID == "" || pod.DeletionTimestamp != nil || pod.Annotations[annotation.Config] != config || !v.owned(current, pod) {
			complete = false
			return
		}
		pods[pod.UID] = pod.Name
	})
	if err != nil || !complete || len(pods) != int(desired) {
		return nil
	}
	for n, informer := range guard {
		if p, ok := informer.checkpoint(); !ok || p != before[n] {
			return nil
		}
	}
	return pods
}

// VerifiedPods returns physical pod identities only for a complete matching inventory.
// An empty result requires authoritative reconciliation.
func (c *configWatcher) VerifiedPods(wl k8sapi.Workload, config string) map[types.UID]string {
	if wl.GetUID() == "" || config == "" {
		return nil
	}
	iwc, ok := c.informers.Load(wl.GetNamespace())
	if !ok {
		iwc, ok = c.informers.Load("")
	}
	if !ok || iwc.inventory == nil {
		return nil
	}
	key := evictionKey(wl)
	state := c.lockEvictionState(key)
	defer state.Unlock()
	if _, queued := c.evictionRequests.Load(key); queued || state.acceptedReplacement != nil || state.replacementPending {
		return nil
	}
	current, err := c.currentEvictionConfig(key)
	if err != nil || current != config {
		return nil
	}
	return iwc.inventory.matching(wl, config)
}
