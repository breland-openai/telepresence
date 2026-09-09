package routeintent

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes/fake"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
	clienttesting "k8s.io/client-go/testing"

	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
)

var (
	testKey       = Key{Namespace: "apps", Owner: "stable-user", Name: "web"}
	testPredicate = Predicate{
		Namespace: "apps", WorkloadKind: "Deployment", Workload: "web", ContainerPort: 8080,
		Service: "web", ServiceUID: "original-uid", ServicePort: 80, RoutingKey: "developer-key",
	}
)

func TestKubernetesAcknowledgmentsPersistAcrossManagersAndFenceRecreations(t *testing.T) {
	ctx := t.Context()
	client := resourceVersionClient()
	first := NewKubernetes(client.CoreV1().ConfigMaps("manager"))
	record, err := first.Create(ctx, testKey, "incarnation-one", testPredicate, time.Time{})
	require.NoError(t, err)
	second := NewKubernetes(client.CoreV1().ConfigMaps("manager"))
	const count = 12
	var group sync.WaitGroup
	errors := make(chan error, count)
	for index := range count {
		group.Go(func() { errors <- second.Acknowledge(ctx, testKey, record.Revision, fmt.Sprintf("pod:uid-%d", index)) })
	}
	group.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	require.NoError(t, first.Acknowledge(ctx, testKey, record.Revision, "gateway:controller"))
	restarted := NewKubernetes(client.CoreV1().ConfigMaps("manager"))
	acks, err := restarted.Acknowledgments(ctx, testKey, record.Revision)
	require.NoError(t, err)
	require.Len(t, acks, count+1)
	for index := range count {
		require.True(t, acks[fmt.Sprintf("pod:uid-%d", index)])
	}
	require.True(t, acks["gateway:controller"])
	current, err := restarted.Get(ctx, testKey)
	require.NoError(t, err)
	require.Equal(t, record.Revision, current.Revision, "ACK metadata must not manufacture a new routing version")
	removed, err := restarted.Transition(ctx, testKey, current.Revision, Change{State: Removed})
	require.NoError(t, err)
	replacement, err := restarted.Transition(ctx, testKey, removed.Revision, Change{State: Desired, Incarnation: "incarnation-two", Predicate: testPredicate})
	require.NoError(t, err)
	require.ErrorIs(t, first.Acknowledge(ctx, testKey, record.Revision, "pod:late"), ErrConflict)
	acks, err = first.Acknowledgments(ctx, testKey, replacement.Revision)
	require.NoError(t, err)
	require.Empty(t, acks, "old pod or gateway evidence cannot activate the replacement")
	require.NoError(t, first.Acknowledge(ctx, testKey, replacement.Revision, "pod:uid-0"))
	acks, err = second.Acknowledgments(ctx, testKey, replacement.Revision)
	require.NoError(t, err)
	require.Equal(t, map[string]bool{"pod:uid-0": true}, acks)
}

func TestGatewayTopologyTransitionsFenceOldProofAcrossRestartAndRestoration(t *testing.T) {
	ctx := t.Context()
	client := resourceVersionClient()
	store := NewKubernetes(client.CoreV1().ConfigMaps("manager"))
	record, err := store.Create(ctx, testKey, "one", testPredicate, time.Time{})
	require.NoError(t, err)
	const controller = "gateway:system:serviceaccount:default:controller"
	const pod = "pod:current:container:process"
	require.NoError(t, store.Acknowledge(ctx, testKey, record.Revision, pod))
	require.NoError(t, store.Acknowledge(ctx, testKey, record.Revision, controller)) // an older manager's bare proof
	a, b := strings.Repeat("a", 64), strings.Repeat("b", 64)
	tokenA, err := store.ObserveGatewayTopology(ctx, testKey, record.Revision, a)
	require.NoError(t, err)
	require.Len(t, tokenA, 64)
	acks, err := store.Acknowledgments(ctx, testKey, record.Revision)
	require.NoError(t, err)
	require.Equal(t, map[string]bool{pod: true}, acks)
	require.NoError(t, store.AcknowledgeGateway(ctx, testKey, record.Revision, controller, tokenA))
	restarted := NewKubernetes(client.CoreV1().ConfigMaps("manager"))
	unchanged, err := restarted.ObserveGatewayTopology(ctx, testKey, record.Revision, a)
	require.NoError(t, err)
	require.Equal(t, tokenA, unchanged)
	tokenB, err := restarted.ObserveGatewayTopology(ctx, testKey, record.Revision, b)
	require.NoError(t, err)
	require.NotEqual(t, tokenA, tokenB)
	require.ErrorIs(t, store.AcknowledgeGateway(ctx, testKey, record.Revision, controller, tokenA), ErrConflict)
	acks, err = store.Acknowledgments(ctx, testKey, record.Revision)
	require.NoError(t, err)
	require.Equal(t, map[string]bool{pod: true}, acks)
	tokenARestored, err := store.ObserveGatewayTopology(ctx, testKey, record.Revision, a)
	require.NoError(t, err)
	require.NotEqual(t, tokenA, tokenARestored)
	require.NotEqual(t, tokenB, tokenARestored)
	require.ErrorIs(t, restarted.AcknowledgeGateway(ctx, testKey, record.Revision, controller, tokenA), ErrConflict)
	require.ErrorIs(t, restarted.AcknowledgeGateway(ctx, testKey, record.Revision, controller, tokenB), ErrConflict)
	require.NoError(t, restarted.AcknowledgeGateway(ctx, testKey, record.Revision, controller, tokenARestored))
	acks, err = store.Acknowledgments(ctx, testKey, record.Revision)
	require.NoError(t, err)
	require.Equal(t, map[string]bool{pod: true, controller + ":" + tokenARestored: true}, acks)
}

func TestKubernetesAcknowledgmentPruningPreservesConcurrentJoinAndLatestProcess(t *testing.T) {
	ctx := t.Context()
	client := resourceVersionClient()
	store := NewKubernetes(client.CoreV1().ConfigMaps("manager"))
	record, err := store.Create(ctx, testKey, "incarnation", testPredicate, time.Time{})
	require.NoError(t, err)
	for _, consumer := range []string{"pod:old:container:process", "pod:live:container:process-one", "gateway:controller"} {
		require.NoError(t, store.Acknowledge(ctx, testKey, record.Revision, consumer))
	}
	require.NoError(t, store.Acknowledge(ctx, testKey, record.Revision, "pod:live:container:process-two"))
	initial, err := store.Acknowledgments(ctx, testKey, record.Revision)
	require.NoError(t, err)
	require.NotContains(t, initial, "pod:live:container:process-one")
	firstList, release := make(chan struct{}), make(chan struct{})
	var lists atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- store.PruneAcknowledgments(ctx, testKey, record.Revision, func(context.Context) (map[string]bool, error) {
			if lists.Add(1) == 1 {
				close(firstList)
				<-release
				return map[string]bool{"live": true}, nil
			}
			return map[string]bool{"live": true, "joining": true}, nil
		})
	}()
	<-firstList
	require.NoError(t, store.Acknowledge(ctx, testKey, record.Revision, "pod:joining:container:process"))
	close(release)
	require.NoError(t, <-done)
	require.GreaterOrEqual(t, lists.Load(), int32(2))
	acks, err := store.Acknowledgments(ctx, testKey, record.Revision)
	require.NoError(t, err)
	require.Equal(t, map[string]bool{"pod:live:container:process-two": true, "pod:joining:container:process": true, "gateway:controller": true}, acks)
}

func TestKubernetesRestartAndRecreation(t *testing.T) {
	ctx := t.Context()
	client := resourceVersionClient()
	store := NewKubernetes(client.CoreV1().ConfigMaps("traffic-manager"))
	lease := time.Date(2026, time.September, 8, 12, 0, 0, 0, time.FixedZone("offset", 3600))
	original, err := store.Create(ctx, testKey, "first", testPredicate, lease)
	require.NoError(t, err)
	assert.Equal(t, Revision{Incarnation: "first", Version: 1}, original.Revision)
	assert.Equal(t, lease.UTC(), original.LeaseUntil)

	restarted := NewKubernetes(client.CoreV1().ConfigMaps("traffic-manager"))
	all, err := restarted.List(ctx)
	require.NoError(t, err)
	require.Equal(t, []Record{original}, all)
	otherNamespace := NewKubernetes(client.CoreV1().ConfigMaps("another-manager"))
	all, err = otherNamespace.List(ctx)
	require.NoError(t, err)
	assert.Empty(t, all)

	renewed, err := restarted.Transition(ctx, testKey, original.Revision, Change{State: Desired, LeaseUntil: lease.Add(time.Hour)})
	require.NoError(t, err)
	assert.Equal(t, uint64(2), renewed.Revision.Version)
	assert.Equal(t, lease.Add(time.Hour).UTC(), renewed.LeaseUntil)
	duplicate, err := store.Create(ctx, testKey, "first", testPredicate, lease)
	require.NoError(t, err)
	assert.Equal(t, renewed, duplicate, "late create retry must not undo lease renewal")

	_, err = restarted.Transition(ctx, testKey, original.Revision, Change{State: Removed})
	require.ErrorIs(t, err, ErrConflict)
	removed, err := restarted.Transition(ctx, testKey, renewed.Revision, Change{State: Removed})
	require.NoError(t, err)
	assert.Equal(t, uint64(3), removed.Revision.Version)
	assert.Equal(t, Removed, removed.State)
	assert.Equal(t, testPredicate, removed.Predicate)
	assert.True(t, removed.LeaseUntil.IsZero())
	_, err = restarted.Create(ctx, testKey, "first", testPredicate, time.Time{})
	require.ErrorIs(t, err, ErrConflict)
	_, err = restarted.Create(ctx, testKey, "next", testPredicate, time.Time{})
	require.ErrorIs(t, err, ErrConflict, "fresh create cannot overwrite the tombstone")
	_, err = restarted.Transition(ctx, testKey, removed.Revision, Change{State: Desired, Incarnation: "first", Predicate: testPredicate})
	require.ErrorIs(t, err, ErrConflict, "old incarnation cannot resurrect itself")
	duplicate, err = restarted.Transition(ctx, testKey, removed.Revision, Change{State: Removed, Incarnation: "first"})
	require.NoError(t, err)
	assert.Equal(t, removed, duplicate)

	nextPredicate := testPredicate
	nextPredicate.RoutingKey = "next-developer"
	recreated, err := restarted.Transition(ctx, testKey, removed.Revision, Change{State: Desired, Incarnation: "next", Predicate: nextPredicate})
	require.NoError(t, err)
	assert.Equal(t, Revision{Incarnation: "next", Version: 4}, recreated.Revision)
	assert.Equal(t, nextPredicate, recreated.Predicate)
	_, err = restarted.Transition(ctx, testKey, removed.Revision, Change{State: Removed, Incarnation: "first"})
	require.ErrorIs(t, err, ErrConflict, "late leave cannot remove the replacement")
	_, err = restarted.Transition(ctx, testKey, recreated.Revision, Change{State: Removed, Incarnation: "first"})
	require.ErrorIs(t, err, ErrConflict, "the claimed old incarnation cannot remove the replacement even with a newer version")
	_, err = restarted.Create(ctx, testKey, "first", testPredicate, time.Time{})
	require.ErrorIs(t, err, ErrConflict, "late reconnect cannot replace the replacement")
	actual, err := store.Get(ctx, testKey)
	require.NoError(t, err)
	assert.Equal(t, recreated, actual)
}

func TestKubernetesConcurrentWriters(t *testing.T) {
	ctx := t.Context()
	client := resourceVersionClient()
	store := NewKubernetes(client.CoreV1().ConfigMaps("manager"))
	const writes = 16
	var wg sync.WaitGroup
	var created atomic.Int32
	for i := range writes {
		wg.Go(func() {
			_, err := store.Create(ctx, testKey, fmt.Sprintf("create-%d", i), testPredicate, time.Time{})
			if err == nil {
				created.Add(1)
			} else {
				assert.ErrorIs(t, err, ErrConflict)
			}
		})
	}
	wg.Wait()
	require.EqualValues(t, 1, created.Load())
	current, err := store.Get(ctx, testKey)
	require.NoError(t, err)

	// Force competing manager instances to read the same Kubernetes resource
	// version. Their subsequent updates must also be fenced by the API server.
	blockedReads := &barrierConfigMaps{ConfigMapInterface: client.CoreV1().ConfigMaps("manager"), allRead: make(chan struct{}), writes: writes}
	var transitioned atomic.Int32
	for i := range writes {
		wg.Go(func() {
			change := Change{State: Desired, LeaseUntil: time.Unix(int64(100+i), 0)}
			if i%2 == 0 {
				change = Change{State: Removed}
			}
			manager := NewKubernetes(blockedReads)
			_, err := manager.Transition(ctx, testKey, current.Revision, change)
			if err == nil {
				transitioned.Add(1)
			} else {
				assert.ErrorIs(t, err, ErrConflict)
			}
		})
	}
	wg.Wait()
	require.EqualValues(t, 1, transitioned.Load())
	latest, err := store.Get(ctx, testKey)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), latest.Revision.Version)
}

func TestKubernetesListFailsClosedAndExcludesUnrelatedObjects(t *testing.T) {
	ctx := t.Context()
	client := resourceVersionClient()
	maps := client.CoreV1().ConfigMaps("manager")
	store := NewKubernetes(maps)
	_, err := store.Create(ctx, testKey, "first", testPredicate, time.Time{})
	require.NoError(t, err)
	_, err = maps.Create(ctx, &core.ConfigMap{ObjectMeta: meta.ObjectMeta{Name: "unrelated"}, Data: map[string]string{configMapData: "garbage"}}, meta.CreateOptions{})
	require.NoError(t, err)
	all, err := store.List(ctx)
	require.NoError(t, err)
	require.Len(t, all, 1)

	object, err := maps.Get(ctx, configMapName(testKey), meta.GetOptions{})
	require.NoError(t, err)
	assert.NotContains(t, object.Name, "stable-user")
	assert.NotContains(t, object.Name, "web")
	assert.Empty(t, validation.IsDNS1123Subdomain(object.Name))
	assert.NotEqual(t, configMapName(Key{Namespace: "ab", Owner: "c", Name: "d"}), configMapName(Key{Namespace: "a", Owner: "bc", Name: "d"}))
	assert.NotEqual(t, configMapName(testKey), configMapName(Key{Namespace: testKey.Namespace, Owner: "another-user", Name: testKey.Name}))
	object.Data[configMapData] = strings.Replace(object.Data[configMapData], `"schema":1`, `"schema":99`, 1)
	_, err = maps.Update(ctx, object, meta.UpdateOptions{})
	require.NoError(t, err)
	all, err = store.List(ctx)
	require.ErrorIs(t, err, ErrInvalid)
	assert.Nil(t, all)
	_, err = store.Get(ctx, testKey)
	require.ErrorIs(t, err, ErrInvalid)

	client.PrependReactor("list", "configmaps", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, context.DeadlineExceeded
	})
	all, err = store.List(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Nil(t, all)
}

func TestKubernetesMissingKeyAndPredicateImmutability(t *testing.T) {
	ctx := t.Context()
	store := NewKubernetes(resourceVersionClient().CoreV1().ConfigMaps("manager"))
	_, err := store.Get(ctx, testKey)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = store.Transition(ctx, testKey, Revision{Incarnation: "first", Version: 1}, Change{State: Removed})
	require.ErrorIs(t, err, ErrNotFound)
	_, err = store.Create(ctx, Key{Namespace: "elsewhere", Owner: "owner", Name: "name"}, "first", testPredicate, time.Time{})
	require.ErrorIs(t, err, ErrInvalid)
	created, err := store.Create(ctx, testKey, "first", testPredicate, time.Time{})
	require.NoError(t, err)
	changed := testPredicate
	changed.RoutingKey = "unexpected"
	_, err = store.Transition(ctx, testKey, created.Revision, Change{State: Desired, Predicate: changed})
	require.ErrorIs(t, err, ErrInvalid)
	_, err = store.Transition(ctx, testKey, created.Revision, Change{State: Removed, Predicate: testPredicate})
	require.ErrorIs(t, err, ErrInvalid)
	_, err = store.Transition(ctx, testKey, created.Revision, Change{State: Desired, LeaseUntil: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)})
	require.ErrorIs(t, err, ErrInvalid)
	unchanged, err := store.Get(ctx, testKey)
	require.NoError(t, err)
	assert.Equal(t, created, unchanged)
}

func TestPredicateFromSpecRejectsAnythingOutsideExactSupportedRoute(t *testing.T) {
	makeSpec := func() *rpc.InterceptSpec {
		return &rpc.InterceptSpec{
			Namespace: "apps", WorkloadKind: "Deployment", Agent: "web", ContainerPort: 8080,
			ServiceName: "web", ServiceUid: "original-uid", ServicePort: 80, Protocol: "TCP",
			HeaderFilters: map[string]string{"x-local-routing-key": "developer-key"},
			Metadata:      map[string]string{"secret": "must-not-be-persisted"},
		}
	}
	predicate, ok := PredicateFromSpec(makeSpec())
	require.True(t, ok)
	assert.Equal(t, testPredicate, predicate)
	wire, err := encode(Record{Key: testKey, Revision: Revision{Incarnation: "first", Version: 1}, State: Desired, Predicate: predicate})
	require.NoError(t, err)
	assert.NotContains(t, wire, "secret")
	assert.NotContains(t, wire, "metadata")
	for name, change := range map[string]func(*rpc.InterceptSpec){
		"additional sensitive header": func(s *rpc.InterceptSpec) { s.HeaderFilters["Authorization"] = "bearer secret" },
		"another header":              func(s *rpc.InterceptSpec) { s.HeaderFilters = map[string]string{"X-Another": "developer-key"} },
		"same header repeated":        func(s *rpc.InterceptSpec) { s.HeaderFilters["X-Local-Routing-Key"] = "different" },
		"regex key":                   func(s *rpc.InterceptSpec) { s.HeaderFilters["x-local-routing-key"] = "developer.*" },
		"empty key":                   func(s *rpc.InterceptSpec) { s.HeaderFilters["x-local-routing-key"] = "" },
		"path condition":              func(s *rpc.InterceptSpec) { s.PathFilters = []string{":path-prefix:/specific"} },
		"unresolved workload":         func(s *rpc.InterceptSpec) { s.WorkloadKind = "" },
		"unresolved container port":   func(s *rpc.InterceptSpec) { s.ContainerPort = 0 },
		"unresolved service port":     func(s *rpc.InterceptSpec) { s.ServicePort = 0 },
		"wiretap":                     func(s *rpc.InterceptSpec) { s.Wiretap = true },
		"replace":                     func(s *rpc.InterceptSpec) { s.Replace = true },
		"UDP":                         func(s *rpc.InterceptSpec) { s.Protocol = "UDP" },
	} {
		t.Run(name, func(t *testing.T) {
			spec := makeSpec()
			change(spec)
			got, ok := PredicateFromSpec(spec)
			assert.False(t, ok)
			assert.Equal(t, Predicate{}, got)
		})
	}
}

type barrierConfigMaps struct {
	typedcore.ConfigMapInterface
	readers atomic.Int32
	allRead chan struct{}
	writes  int32
}

func (b *barrierConfigMaps) Get(ctx context.Context, name string, options meta.GetOptions) (*core.ConfigMap, error) {
	object, err := b.ConfigMapInterface.Get(ctx, name, options)
	if b.readers.Add(1) == b.writes {
		close(b.allRead)
	}
	select {
	case <-b.allRead:
		return object, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// The standard fake Kubernetes tracker does not enforce resourceVersion on
// updates. These reactors add the API-server semantics the store depends on.
func resourceVersionClient() *fake.Clientset {
	client := fake.NewClientset()
	var mu sync.Mutex
	var version uint64
	configmaps := schema.GroupResource{Resource: "configmaps"}
	client.PrependReactor("create", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		mu.Lock()
		defer mu.Unlock()
		create := action.(clienttesting.CreateAction)
		object := create.GetObject().(*core.ConfigMap).DeepCopy()
		object.Namespace = action.GetNamespace()
		version++
		object.ResourceVersion = strconv.FormatUint(version, 10)
		err := client.Tracker().Create(action.GetResource(), object, action.GetNamespace())
		if err != nil {
			return true, nil, err
		}
		return true, object.DeepCopy(), nil
	})
	client.PrependReactor("update", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		mu.Lock()
		defer mu.Unlock()
		update := action.(clienttesting.UpdateAction)
		object := update.GetObject().(*core.ConfigMap).DeepCopy()
		raw, err := client.Tracker().Get(action.GetResource(), action.GetNamespace(), object.Name)
		if err != nil {
			return true, nil, err
		}
		current := raw.(*core.ConfigMap)
		if object.ResourceVersion == "" || object.ResourceVersion != current.ResourceVersion {
			return true, nil, kerrors.NewConflict(configmaps, object.Name, errors.New("resource version changed"))
		}
		version++
		object.ResourceVersion = strconv.FormatUint(version, 10)
		if err := client.Tracker().Update(action.GetResource(), object, action.GetNamespace()); err != nil {
			return true, nil, err
		}
		return true, object.DeepCopy(), nil
	})
	return client
}
