package manager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	stderrors "errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	empty "google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	core "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/telepresenceio/clog"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/auth"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/routeintent"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/state"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
)

const (
	routeIntentPollInterval = 2 * time.Second
	routeIntentReadTimeout  = 5 * time.Second
)

type routeIntentManager struct {
	store          routeintent.Store
	controllers    []string
	namespaces     []string
	wake           chan struct{}
	refreshMu      sync.Mutex
	mu             sync.Mutex
	changed        chan struct{}
	records        []routeintent.Record
	ready          bool
	ownersMu       sync.Mutex
	ownerLocks     map[string]*routeIntentOwnerLock
	requireGateway bool
	reconcile      func(context.Context)
}

type routeIntentOwnerLock struct {
	taken chan struct{}
	users int
}

func newRouteIntentManager(store routeintent.Store, controllers []string, namespaces ...string) *routeIntentManager {
	return &routeIntentManager{
		store: store, controllers: slices.Clone(controllers), namespaces: slices.Clone(namespaces),
		wake: make(chan struct{}, 1), changed: make(chan struct{}), ownerLocks: make(map[string]*routeIntentOwnerLock), requireGateway: true,
	}
}

func (m *routeIntentManager) managesNamespace(namespace string) bool {
	return m != nil && (len(m.namespaces) == 0 || slices.Contains(m.namespaces, namespace))
}

func (s *service) usesDurableRoute(spec *rpc.InterceptSpec) bool {
	return s.routeIntents.managesNamespace(spec.GetNamespace()) && usesLocalRoutingKey(spec)
}

func (s *service) validateRouteAgentProcess(agent *rpc.AgentInfo, principal *auth.Principal) error {
	if !s.routeIntents.managesNamespace(agent.GetNamespace()) || agent.GetRouteGuardInstance() == "" {
		return nil
	}
	if principal == nil {
		return status.Error(codes.Unauthenticated, "authoritative route agents require their pod-bound authenticated identity")
	}
	if len(agent.RouteGuardInstance) > 128 || agent.RouteGuardStartedAt == nil || agent.RouteGuardStartedAt.CheckValid() != nil ||
		agent.RouteGuardStartedAt.AsTime().After(time.Now().Add(5*time.Second)) {
		return status.Error(codes.InvalidArgument, "authoritative route agent requires a valid current process identity")
	}
	return nil
}

func (s *service) reconnectMayUseDurableRoutes(info *rpc.ReconnectClientRequest) bool {
	return s.routeIntents.managesNamespace(info.GetClient().GetNamespace()) ||
		slices.ContainsFunc(info.GetIntercepts(), func(info *rpc.InterceptInfo) bool { return s.usesDurableRoute(info.GetSpec()) })
}

func (s *service) shouldLookupDurableRemoval(ctx context.Context, clientNamespace string, live *state.Intercept, incarnation string) bool {
	if live != nil {
		return s.routeIntents.managesNamespace(live.Spec.GetNamespace()) && (live.RouteIncarnation != "" || incarnation != "")
	}
	// A missing legacy request has no target namespace in this RPC. A scoped
	// manager only consults durable state for a scoped client, or when an
	// authenticated cross-namespace caller supplies an actual incarnation.
	return s.routeIntents.managesNamespace(clientNamespace) || (s.routeIntents != nil && incarnation != "" && auth.PrincipalFrom(ctx) != nil)
}

func (m *routeIntentManager) run(ctx context.Context) error {
	ticker := time.NewTicker(routeIntentPollInterval)
	defer ticker.Stop()
	for {
		m.refresh(ctx)
		if m.reconcile != nil {
			m.reconcile(ctx)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-m.wake:
		}
	}
}

func (m *routeIntentManager) notify() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func (m *routeIntentManager) refresh(ctx context.Context) {
	// All publications are ordered, including a new subscriber's mandatory
	// fresh read. Otherwise a pre-subscription list could publish stale state
	// after the subscriber's more recent initial list.
	m.refreshMu.Lock()
	defer m.refreshMu.Unlock()
	if ctx.Err() != nil {
		return
	}
	readCtx, cancel := context.WithTimeout(ctx, routeIntentReadTimeout)
	defer cancel()
	records, err := m.store.List(readCtx)
	if ctx.Err() != nil {
		// A closing individual watcher cannot revoke authority for everyone.
		return
	}
	if err != nil {
		clog.Warnf(ctx, "Unable to synchronize durable route intents: %v", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ready = err == nil
	if err == nil {
		m.records = records
	}
	// Also wake on a list with unchanged records: a Service selector may have
	// changed without a route intent change. Individual consumers avoid sending
	// duplicate projections after rechecking their Service membership.
	close(m.changed)
	m.changed = make(chan struct{})
}

func (m *routeIntentManager) snapshot() ([]routeintent.Record, bool, <-chan struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.records, m.ready, m.changed
}

func (m *routeIntentManager) lockOwner(ctx context.Context, owner string) (func(), error) {
	m.ownersMu.Lock()
	lock := m.ownerLocks[owner]
	if lock == nil {
		lock = &routeIntentOwnerLock{taken: make(chan struct{}, 1)}
		m.ownerLocks[owner] = lock
	}
	lock.users++
	m.ownersMu.Unlock()
	release := func() {
		m.ownersMu.Lock()
		defer m.ownersMu.Unlock()
		lock.users--
		if lock.users == 0 {
			delete(m.ownerLocks, owner)
		}
	}
	select {
	case <-ctx.Done():
		release()
		return nil, status.FromContextError(ctx.Err()).Err()
	case lock.taken <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-lock.taken
			release()
			return nil, status.FromContextError(err).Err()
		}
		return func() {
			<-lock.taken
			release()
		}, nil
	}
}

func durableRouteOwner(ctx context.Context, session *rpc.SessionInfo) (string, error) {
	principal := auth.PrincipalFrom(ctx)
	if principal == nil || principal.Username == "" {
		if auth.AuthUnavailable(ctx) {
			return "", status.Error(codes.Unavailable, "unable to authenticate durable route ownership")
		}
		return "", status.Error(codes.Unauthenticated, "durable routes require an authenticated client")
	}
	sid := session.GetSessionId()
	if sid == "" || strings.Contains(sid, "/") {
		return "", status.Error(codes.InvalidArgument, "durable routes require a valid client session")
	}
	hash := sha256.Sum256([]byte(principal.Username + "\x00" + principal.UID))
	// A ReconnectClient request carries the same client session UUID across
	// manager restart. Separating intentional new sessions also isolates a stale
	// departure from a newly connected instance owned by the same principal.
	return hex.EncodeToString(hash[:]) + "/" + sid, nil
}

func durableRouteInterceptID(record routeintent.Record) string {
	_, sid, ok := strings.Cut(record.Key.Owner, "/")
	if !ok {
		return ""
	}
	return sid + ":" + record.Key.Name
}

func durableRouteError(err error) error {
	switch {
	case stderrors.Is(err, routeintent.ErrConflict):
		return status.Error(codes.FailedPrecondition, "durable route incarnation or revision has changed")
	case stderrors.Is(err, routeintent.ErrInvalid):
		return status.Errorf(codes.InvalidArgument, "invalid durable route: %v", err)
	case stderrors.Is(err, routeintent.ErrNotFound):
		return status.Error(codes.NotFound, "durable route not found")
	case stderrors.Is(err, context.Canceled), stderrors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	default:
		return status.Error(codes.Unavailable, "durable route store is unavailable")
	}
}

func (m *routeIntentManager) create(ctx context.Context, key routeintent.Key, incarnation string, predicate routeintent.Predicate) (routeintent.Record, error) {
	if incarnation == "" {
		return routeintent.Record{}, status.Error(codes.FailedPrecondition, "this manager requires a client that supplies durable route incarnations")
	}
	current, err := m.store.Get(ctx, key)
	switch {
	case stderrors.Is(err, routeintent.ErrNotFound):
		current, err = m.store.Create(ctx, key, incarnation, predicate, time.Time{})
	case err != nil:
	case current.State == routeintent.Removed:
		current, err = m.store.Transition(ctx, key, current.Revision, routeintent.Change{State: routeintent.Desired, Incarnation: incarnation, Predicate: predicate})
	case current.Revision.Incarnation != incarnation || current.Predicate != predicate:
		err = routeintent.ErrConflict
	}
	if err != nil {
		return routeintent.Record{}, durableRouteError(err)
	}
	m.notify()
	return current, nil
}

func (m *routeIntentManager) remove(ctx context.Context, record routeintent.Record, incarnation string) error {
	if incarnation == "" || record.Revision.Incarnation != incarnation {
		return status.Error(codes.FailedPrecondition, "removing a durable route requires its current incarnation")
	}
	if record.State != routeintent.Removed {
		_, err := m.store.Transition(ctx, record.Key, record.Revision, routeintent.Change{State: routeintent.Removed, Incarnation: incarnation})
		if err != nil {
			return durableRouteError(err)
		}
		m.notify()
	}
	return nil
}

func (m *routeIntentManager) findOwned(ctx context.Context, owner, name string) (*routeintent.Record, error) {
	records, err := m.store.List(ctx)
	if err != nil {
		return nil, durableRouteError(err)
	}
	var result *routeintent.Record
	for _, record := range records {
		if !m.managesNamespace(record.Key.Namespace) || record.Key.Owner != owner || record.Key.Name != name {
			continue
		}
		// The existing live protocol forbids duplicate intercept names within a
		// session, even across namespaces; historical tombstones can coexist.
		if result == nil || (result.State == routeintent.Removed && record.State == routeintent.Desired) {
			recordCopy := record
			result = &recordCopy
		} else if result.State == routeintent.Desired && record.State == routeintent.Desired {
			return nil, status.Error(codes.FailedPrecondition, "multiple durable routes exist for this session and intercept name")
		}
	}
	return result, nil
}

func usesLocalRoutingKey(spec *rpc.InterceptSpec) bool {
	for key := range spec.GetHeaderFilters() {
		if strings.EqualFold(key, routeintent.RoutingHeader) {
			return true
		}
	}
	return false
}

func durableRoutePredicate(spec *rpc.InterceptSpec) (routeintent.Predicate, bool) {
	// Extra child port intercepts currently have their own runtime IDs and no
	// individually persisted target records. Do not acknowledge broader coverage.
	if spec == nil || len(spec.PodPorts) != 0 || (spec.Mechanism != "" && spec.Mechanism != "http") {
		return routeintent.Predicate{}, false
	}
	return routeintent.PredicateFromSpec(spec)
}

func (s *service) durableRestoredIntercepts(ctx context.Context, session *rpc.SessionInfo, intercepts []*rpc.InterceptInfo) ([]*rpc.InterceptInfo, error) {
	var owner string
	out := make([]*rpc.InterceptInfo, 0, len(intercepts))
	for _, original := range intercepts {
		if !s.usesDurableRoute(original.GetSpec()) {
			if original.RouteIncarnation != "" {
				original = proto.Clone(original).(*rpc.InterceptInfo)
				original.RouteIncarnation = ""
			}
			out = append(out, original)
			continue
		}
		if owner == "" {
			var err error
			if owner, err = durableRouteOwner(ctx, session); err != nil {
				return nil, err
			}
		}
		predicate, supported := durableRoutePredicate(original.Spec)
		if !supported || original.RouteIncarnation == "" {
			return nil, status.Error(codes.FailedPrecondition, "cannot restore a local routing key without a supported durable incarnation")
		}
		if original.GetClientSession().GetSessionId() != session.GetSessionId() || original.Id != session.GetSessionId()+":"+original.Spec.Name {
			return nil, status.Error(codes.PermissionDenied, "the restored durable route does not belong to this client session")
		}
		key := routeintent.Key{Namespace: predicate.Namespace, Owner: owner, Name: original.Spec.Name}
		current, err := s.routeIntents.store.Get(ctx, key)
		if stderrors.Is(err, routeintent.ErrNotFound) {
			return nil, status.Error(codes.FailedPrecondition, "a restored local routing key has no durable route")
		}
		if err != nil {
			return nil, durableRouteError(err)
		}
		if current.State != routeintent.Desired || current.Revision.Incarnation != original.RouteIncarnation {
			// An explicit removal or a later creation won. The client-supplied
			// snapshot is never authority to recreate the previous route.
			continue
		}
		if current.Predicate != predicate {
			return nil, status.Error(codes.FailedPrecondition, "restored route differs from its durable predicate")
		}
		out = append(out, original)
	}
	return out, nil
}

func (s *service) departDurableRoutes(ctx context.Context, session *rpc.SessionInfo) error {
	owner, err := durableRouteOwner(ctx, session)
	if err != nil {
		return err
	}
	unlock, err := s.routeIntents.lockOwner(ctx, owner)
	if err != nil {
		return err
	}
	defer unlock()
	records, err := s.routeIntents.store.List(ctx)
	if err != nil {
		return durableRouteError(err)
	}
	for _, record := range records {
		if s.routeIntents.managesNamespace(record.Key.Namespace) && record.Key.Owner == owner && record.State == routeintent.Desired {
			if err = s.routeIntents.remove(ctx, record, record.Revision.Incarnation); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *service) WatchRouteIntents(session *rpc.SessionInfo, stream grpc.ServerStreamingServer[rpc.RouteIntentSnapshot]) error {
	if s.routeIntents == nil {
		return status.Error(codes.Unimplemented, "authoritative durable route intents are disabled")
	}
	ctx := managerutil.WithSessionInfo(stream.Context(), session)
	principal := auth.PrincipalFrom(ctx)
	if principal == nil || principal.Username == "" {
		if auth.AuthUnavailable(ctx) {
			return status.Error(codes.Unavailable, "unable to authenticate the route-intent watcher")
		}
		return status.Error(codes.Unauthenticated, "watching durable routes requires an authenticated caller")
	}
	var agent *state.AgentSession
	var done <-chan struct{}
	if sid := tunnel.SessionID(session.GetSessionId()); sid != "" {
		var err error
		agent = s.state.GetAgent(sid)
		if agent == nil {
			return status.Error(codes.PermissionDenied, "only traffic-agents can watch workload-scoped durable routes")
		}
		if verified, _ := verifiedAgentPrincipal(ctx, agent.AgentInfo); verified == nil {
			return status.Error(codes.PermissionDenied, "watching workload routes requires a bound token for this agent pod")
		}
		if err = agentOwnershipError(ctx, sid, agent); err != nil {
			return err
		}
		if done, err = s.state.SessionDone(sid); err != nil {
			return err
		}
	} else if !strings.HasPrefix(principal.Username, "system:serviceaccount:") || !slices.Contains(s.routeIntents.controllers, principal.Username) {
		return status.Error(codes.PermissionDenied, "this service account is not authorized to watch all durable routes")
	}
	initial := &rpc.RouteIntentSnapshot{SyncState: rpc.RouteIntentSnapshot_SYNCHRONIZING, ManagerEpoch: s.id}
	if err := stream.Send(initial); err != nil {
		return err
	}
	s.routeIntents.refresh(ctx)
	previous := initial
	for {
		records, ready, changed := s.routeIntents.snapshot()
		current := &rpc.RouteIntentSnapshot{SyncState: rpc.RouteIntentSnapshot_SYNCHRONIZING, ManagerEpoch: s.id}
		if ready {
			projectCtx, cancel := context.WithTimeout(ctx, routeIntentReadTimeout)
			intents, err := s.projectRouteIntents(projectCtx, records, agent)
			cancel()
			if err == nil {
				current.SyncState = rpc.RouteIntentSnapshot_AUTHORITATIVE
				current.Intents = intents
				if agent == nil {
					current.EligibleTargets = s.routeIntentEligibleTargets(ctx)
				}
			} else if ctx.Err() == nil {
				clog.Warnf(ctx, "Unable to project durable route intents: %v", err)
			}
		}
		if !routeIntentSnapshotsEqual(previous, current) {
			if err := stream.Send(current); err != nil {
				return err
			}
			previous = current
		}
		select {
		case <-ctx.Done():
			return nil
		case <-done:
			return nil
		case <-changed:
		}
	}
}

func routeIntentSnapshotsEqual(a, b *rpc.RouteIntentSnapshot) bool {
	return proto.Equal(a, b)
}

func (s *service) projectRouteIntents(ctx context.Context, records []routeintent.Record, agent *state.AgentSession) ([]*rpc.RouteIntent, error) {
	var out []*rpc.RouteIntent
	var inventory []*rpc.RouteIntentTarget
	if agent == nil && s.routeIntents != nil {
		inventory = s.routeIntentEligibleTargets(ctx)
	}
	for _, record := range records {
		route, err := s.projectRouteIntent(ctx, record, agent, inventory)
		if err != nil {
			return nil, err
		}
		if route != nil {
			out = append(out, route)
		}
	}
	return out, nil
}

func (s *service) projectRouteIntent(
	ctx context.Context, record routeintent.Record, agent *state.AgentSession, inventory []*rpc.RouteIntentTarget,
) (*rpc.RouteIntent, error) {
	if (s.routeIntents != nil && !s.routeIntents.managesNamespace(record.Key.Namespace)) || !s.state.ManagesNamespace(ctx, record.Key.Namespace) {
		return nil, nil
	}
	predicate := record.Predicate
	target := &rpc.RouteIntentTarget{
		Namespace: predicate.Namespace, WorkloadKind: predicate.WorkloadKind, WorkloadName: predicate.Workload,
		ContainerPort: predicate.ContainerPort, ServiceName: predicate.Service, ServiceUid: predicate.ServiceUID, ServicePort: predicate.ServicePort,
	}
	targets := []*rpc.RouteIntentTarget{target}
	if agent != nil {
		included, err := routeIntentTargetForAgent(ctx, record, agent, target)
		if err != nil || !included {
			return nil, err
		}
	} else if predicate.ServiceUID != "" && record.State == routeintent.Desired {
		var err error
		targets, err = globalRouteIntentTargets(ctx, predicate, target)
		if err != nil {
			return nil, err
		}
	}
	for _, member := range targets {
		s.populateRouteIntentTarget(member, agent, inventory)
	}
	id := durableRouteInterceptID(record)
	if id == "" {
		return nil, fmt.Errorf("invalid durable route owner for %s/%s", record.Key.Namespace, record.Key.Name)
	}
	route := &rpc.RouteIntent{
		InterceptId: id, Incarnation: record.Revision.Incarnation, Revision: record.Revision.Version,
		State: rpc.RouteIntent_DESIRED, RoutingKey: predicate.RoutingKey, Targets: targets,
	}
	if record.State == routeintent.Desired && agent == nil && s.routeIntents != nil {
		guarded, _, err := s.routeActivationEvidence(ctx, record)
		if err != nil {
			return nil, err
		}
		route.AgentsGuarded = guarded
		route.GatewayTopology, err = s.gatewayRouteToken(ctx, record)
		if err != nil {
			return nil, err
		}
	}
	if record.State == routeintent.Removed {
		route.State = rpc.RouteIntent_REMOVED
	}
	if !record.LeaseUntil.IsZero() {
		route.LeaseUntil = timestamppb.New(record.LeaseUntil)
	}
	return route, nil
}

func routeIntentTargetForAgent(ctx context.Context, record routeintent.Record, agent *state.AgentSession, target *rpc.RouteIntentTarget) (bool, error) {
	predicate := record.Predicate
	if agent.Namespace != predicate.Namespace {
		return false, nil
	}
	if agent.Name == predicate.Workload && agent.Kind == predicate.WorkloadKind {
		return true, nil
	}
	if predicate.ServiceUID == "" {
		return false, nil
	}
	var port int32
	for _, advertised := range agent.InterceptTargets {
		if advertised.ServiceUid == predicate.ServiceUID && advertised.ServicePort == predicate.ServicePort &&
			strings.EqualFold(advertised.Protocol, "TCP") {
			port = advertised.ContainerPort
			break
		}
	}
	if port == 0 {
		return false, nil
	}
	if record.State == routeintent.Desired {
		member, err := routeIntentServiceMember(ctx, predicate, agent)
		if err != nil || !member {
			return false, err
		}
	}
	target.WorkloadKind, target.WorkloadName, target.ContainerPort = agent.Kind, agent.Name, port
	return true, nil
}

func (s *service) populateRouteIntentTarget(member *rpc.RouteIntentTarget, agent *state.AgentSession, inventory []*rpc.RouteIntentTarget) {
	if agent != nil {
		for _, advertised := range agent.InterceptTargets {
			if routeIntentAdvertisementMatches(member, advertised.ContainerPort, advertised.ServiceUid, advertised.ServicePort) {
				member.AgentPort, member.AppProtocol = advertised.AgentPort, advertised.AppProtocol
				break
			}
		}
		return
	}
	for _, advertised := range inventory {
		if advertised.Namespace == member.Namespace && advertised.WorkloadKind == member.WorkloadKind && advertised.WorkloadName == member.WorkloadName &&
			routeIntentAdvertisementMatches(member, advertised.ContainerPort, advertised.ServiceUid, advertised.ServicePort) {
			// The current Service declaration takes precedence over a stale agent
			// advertisement, especially while HTTP is changing to an unsupported protocol.
			if member.AppProtocol != "" && !strings.EqualFold(member.AppProtocol, advertised.AppProtocol) {
				return
			}
			member.AgentPort = advertised.AgentPort
			if member.AppProtocol == "" {
				member.AppProtocol = advertised.AppProtocol
			}
			break
		}
	}
	if member.AgentPort == 0 {
		s.routeIntentTargetAdvertisement(member)
	}
}

func routeIntentAdvertisementMatches(target *rpc.RouteIntentTarget, containerPort int32, serviceUID string, servicePort int32) bool {
	return target.ContainerPort == containerPort && (target.ServiceUid == "" || (target.ServiceUid == serviceUID && target.ServicePort == servicePort))
}

func routeIntentServiceMember(ctx context.Context, predicate routeintent.Predicate, agent *state.AgentSession) (bool, error) {
	client := k8sapi.GetK8sInterface(ctx)
	svc, err := client.CoreV1().Services(predicate.Namespace).Get(ctx, predicate.Service, metav1.GetOptions{})
	if kerrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if string(svc.UID) != predicate.ServiceUID || len(svc.Spec.Selector) == 0 {
		return false, nil
	}
	workload, err := k8sapi.GetWorkload(ctx, agent.Name, agent.Namespace, k8sapi.Kind(agent.Kind))
	if kerrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return labels.SelectorFromSet(svc.Spec.Selector).Matches(labels.Set(workload.GetPodTemplate().Labels)), nil
}

func globalRouteIntentTargets(ctx context.Context, predicate routeintent.Predicate, primary *rpc.RouteIntentTarget) ([]*rpc.RouteIntentTarget, error) {
	targets := []*rpc.RouteIntentTarget{primary}
	svc, err := k8sapi.GetK8sInterface(ctx).CoreV1().Services(predicate.Namespace).Get(ctx, predicate.Service, metav1.GetOptions{})
	if kerrors.IsNotFound(err) || (err == nil && string(svc.UID) != predicate.ServiceUID) {
		return targets, nil
	}
	if err != nil {
		return nil, err
	}
	var targetPort intstr.IntOrString
	for _, port := range svc.Spec.Ports {
		if port.Port == predicate.ServicePort && (port.Protocol == "" || port.Protocol == core.ProtocolTCP) {
			if port.AppProtocol != nil {
				primary.AppProtocol = *port.AppProtocol
			}
			targetPort = port.TargetPort
			if targetPort == (intstr.IntOrString{}) {
				targetPort = intstr.FromInt32(port.Port)
			}
			break
		}
	}
	if targetPort == (intstr.IntOrString{}) {
		return targets, nil
	}
	spec := &rpc.InterceptSpec{
		Namespace: predicate.Namespace, Agent: predicate.Workload, ServiceName: predicate.Service,
		ServiceUid: predicate.ServiceUID, ServicePort: predicate.ServicePort, Protocol: "TCP",
	}
	workloads, known, err := state.DiscoverRouteIntentServiceWorkloads(ctx, spec, svc)
	if err != nil {
		return nil, err
	}
	if !known {
		return nil, fmt.Errorf("workloads for durable Service route %s/%s are not synchronized", predicate.Namespace, predicate.Service)
	}
	for _, workload := range workloads {
		if workload.GetName() == primary.WorkloadName && string(workload.GetKind()) == primary.WorkloadKind {
			continue
		}
		port := targetPort.IntVal
		if targetPort.Type == intstr.String {
			for _, container := range workload.GetPodTemplate().Spec.Containers {
				for _, candidate := range container.Ports {
					if candidate.Name == targetPort.StrVal && (candidate.Protocol == "" || candidate.Protocol == core.ProtocolTCP) {
						if port != 0 && port != candidate.ContainerPort {
							return nil, fmt.Errorf("workload %s has conflicting ports for durable Service target %s", workload.GetName(), targetPort.StrVal)
						}
						port = candidate.ContainerPort
					}
				}
			}
			if port == 0 {
				return nil, fmt.Errorf("workload %s has no named port for durable Service target %s", workload.GetName(), targetPort.StrVal)
			}
		}
		targets = append(targets, &rpc.RouteIntentTarget{
			Namespace: workload.GetNamespace(), WorkloadKind: string(workload.GetKind()), WorkloadName: workload.GetName(),
			ContainerPort: port, ServiceName: predicate.Service, ServiceUid: predicate.ServiceUID,
			ServicePort: predicate.ServicePort, AppProtocol: primary.AppProtocol,
		})
	}
	return targets, nil
}

// AcknowledgeRouteIntents persists exact applied revisions. Agent pod identity is
// taken from its TokenReview-bound session, never from caller supplied fields.
func (s *service) AcknowledgeRouteIntents(ctx context.Context, request *rpc.RouteIntentAckRequest) (*empty.Empty, error) {
	if s.routeIntents == nil {
		return nil, status.Error(codes.Unimplemented, "authoritative durable route intents are disabled")
	}
	caller, err := s.routeAcknowledgmentCaller(ctx, request)
	if err != nil {
		return nil, err
	}
	if caller == nil {
		return &empty.Empty{}, nil
	}
	if len(request.GetApplied()) > 2048 {
		return nil, status.Error(codes.ResourceExhausted, "too many route acknowledgments")
	}
	if len(request.GetApplied()) == 0 {
		return &empty.Empty{}, nil
	}
	records, err := s.routeIntents.store.List(ctx)
	if err != nil {
		return nil, durableRouteError(err)
	}
	byID := make(map[string]routeintent.Record, len(records))
	for _, record := range records {
		if s.routeIntents.managesNamespace(record.Key.Namespace) {
			byID[durableRouteInterceptID(record)] = record
		}
	}
	for _, ack := range request.GetApplied() {
		if ack.GetInterceptId() == "" || ack.GetIncarnation() == "" || ack.GetRevision() == 0 {
			return nil, status.Error(codes.InvalidArgument, "route ID, incarnation, and revision are required")
		}
		record, exists := byID[ack.GetInterceptId()]
		if !exists {
			continue
		} // retired while a previously authoritative snapshot was in flight
		revision := routeintent.Revision{Incarnation: ack.Incarnation, Version: ack.Revision}
		if record.Revision != revision {
			continue
		} // a stale acknowledgment cannot mutate a newer revision
		if err = s.applyRouteAcknowledgment(ctx, caller, record, ack); err != nil {
			if stderrors.Is(err, routeintent.ErrConflict) || stderrors.Is(err, routeintent.ErrNotFound) {
				continue
			}
			if _, ok := status.FromError(err); ok {
				return nil, err
			}
			return nil, durableRouteError(err)
		}
	}
	s.routeIntents.notify()
	return &empty.Empty{}, nil
}

func (s *service) applyRouteAcknowledgment(
	ctx context.Context, caller *routeAcknowledgmentCaller, record routeintent.Record, ack *rpc.RouteIntentAck,
) error {
	if record.State != routeintent.Desired {
		return nil
	}
	if caller.agent != nil {
		if err := caller.verifyAgentScope(ctx, s, record); err != nil {
			return err
		}
		return s.routeIntents.store.Acknowledge(ctx, record.Key, record.Revision, caller.consumer)
	}
	if ack.GetGatewayTopology() == "" {
		return nil // Old controllers must not certify a topology they never received.
	}
	token, err := s.gatewayRouteToken(ctx, record)
	if err != nil || token == "" || token != ack.GatewayTopology {
		return err // A previously authoritative snapshot may already be stale.
	}
	return s.routeIntents.store.AcknowledgeGateway(ctx, record.Key, record.Revision, caller.consumer, token)
}

type routeAcknowledgmentCaller struct {
	agent     *state.AgentSession
	consumer  string
	appliedAt time.Time
}

func (s *service) routeAcknowledgmentCaller(ctx context.Context, request *rpc.RouteIntentAckRequest) (*routeAcknowledgmentCaller, error) {
	principal := auth.PrincipalFrom(ctx)
	if principal == nil || principal.Username == "" {
		if auth.AuthUnavailable(ctx) {
			return nil, status.Error(codes.Unavailable, "unable to authenticate the route acknowledgment")
		}
		return nil, status.Error(codes.Unauthenticated, "route acknowledgments require an authenticated caller")
	}
	caller := &routeAcknowledgmentCaller{}
	sid := tunnel.SessionID(request.GetSession().GetSessionId())
	if sid == "" {
		if !strings.HasPrefix(principal.Username, "system:serviceaccount:") || !slices.Contains(s.routeIntents.controllers, principal.Username) {
			return nil, status.Error(codes.PermissionDenied, "service account cannot acknowledge gateway routes")
		}
		caller.consumer = "gateway:" + principal.Username
		return caller, nil
	}
	caller.agent = s.state.GetAgent(sid)
	if caller.agent == nil {
		return nil, status.Error(codes.PermissionDenied, "only agents may acknowledge pod route guards")
	}
	if verified, _ := verifiedAgentPrincipal(ctx, caller.agent.AgentInfo); verified == nil {
		return nil, status.Error(codes.PermissionDenied, "a pod-bound agent token is required")
	}
	if err := agentOwnershipError(ctx, sid, caller.agent); err != nil {
		return nil, err
	}
	if caller.agent.PodUid == "" || caller.agent.RouteGuardInstance == "" || len(caller.agent.RouteGuardInstance) > 128 {
		return nil, status.Error(codes.PermissionDenied, "agent pod UID is required")
	}
	if request.GetAgentGuardInstance() == "" || request.GetAppliedAt() == nil || request.GetAppliedAt().CheckValid() != nil {
		return nil, status.Error(codes.InvalidArgument, "agent guard instance and valid application timestamp are required")
	}
	if request.GetAgentGuardInstance() != caller.agent.RouteGuardInstance {
		return nil, nil
	}
	caller.appliedAt = request.GetAppliedAt().AsTime()
	if caller.appliedAt.After(time.Now().Add(5 * time.Second)) {
		return nil, status.Error(codes.InvalidArgument, "agent route timestamp is in the future")
	}
	return caller, nil
}

func (c *routeAcknowledgmentCaller) verifyAgentScope(ctx context.Context, s *service, record routeintent.Record) error {
	projected, err := s.projectRouteIntents(ctx, []routeintent.Record{record}, c.agent)
	if err != nil {
		return status.Error(codes.Unavailable, "unable to verify agent route scope")
	}
	if len(projected) == 0 {
		return status.Error(codes.PermissionDenied, "agent cannot acknowledge another workload's route")
	}
	if c.consumer != "" {
		return nil
	}
	var containerStartedAt time.Time
	c.consumer, containerStartedAt, err = routeAgentConsumerState(ctx, c.agent)
	if err != nil {
		return status.Error(codes.Unavailable, "unable to identify current traffic-agent container for route acknowledgment")
	}
	// Kubernetes metav1.Time writes seconds precision. Waiting until the following
	// second also rejects old buffered requests when container lifetimes share it.
	if !c.appliedAt.After(containerStartedAt.Truncate(time.Second).Add(time.Second)) {
		return status.Error(codes.Unavailable, "waiting for proof newer than the traffic-agent container start")
	}
	return nil
}

func (s *service) reconcileRouteActivations(ctx context.Context) {
	records, ready, _ := s.routeIntents.snapshot()
	for _, record := range records {
		if !s.routeIntents.managesNamespace(record.Key.Namespace) {
			continue
		}
		allowed := false
		if ready && record.State == routeintent.Desired {
			checkCtx, cancel := context.WithTimeout(ctx, routeIntentReadTimeout)
			agents, gateway, err := s.routeActivationEvidence(checkCtx, record)
			cancel()
			if err != nil && ctx.Err() == nil {
				clog.Debugf(ctx, "Route activation remains waiting: %v", err)
			}
			allowed = err == nil && agents && (gateway || !s.routeIntents.requireGateway)
		}
		s.state.SetRouteActivation(durableRouteInterceptID(record), record.Revision.Incarnation, allowed)
	}
}

func (s *service) routeActivationEvidence(ctx context.Context, record routeintent.Record) (agents, gateway bool, err error) {
	topology, err := s.gatewayRouteToken(ctx, record)
	if err != nil && s.routeIntents.requireGateway {
		return false, false, err
	}
	activationTopology := topology
	if !s.routeIntents.requireGateway {
		activationTopology = routeintent.AgentOnlyActivation
	}
	acks, established, err := s.routeIntents.store.ActivationEvidence(ctx, record.Key, record.Revision, activationTopology)
	if err != nil {
		return false, false, err
	}
	eligibility, err := currentRoutePodEligibility(ctx, record.Predicate)
	if err != nil {
		return false, false, err
	}
	if len(acks) > 512 {
		livePods := func(ctx context.Context) (map[string]bool, error) { return routableRoutePodUIDs(ctx, record.Predicate) }
		if pruneErr := s.routeIntents.store.PruneAcknowledgments(ctx, record.Key, record.Revision, livePods); pruneErr != nil && ctx.Err() == nil {
			clog.Debugf(ctx, "Unable to prune old route acknowledgment evidence: %v", pruneErr)
		}
	}
	eligible := eligibility.selected
	if established {
		eligible = eligibility.serving
	}
	consumers, agents := s.acknowledgedRoutePods(ctx, record.Predicate.Namespace, eligible, eligibility.podNames, acks, established)
	var gatewayConsumers []string
	for _, name := range s.routeIntents.controllers {
		consumer := "gateway:" + name + ":" + topology
		if topology != "" && acks[consumer] {
			gatewayConsumers = append(gatewayConsumers, consumer)
		}
	}
	gateway = len(gatewayConsumers) != 0
	if !established && agents && (gateway || !s.routeIntents.requireGateway) {
		err = s.routeIntents.store.EstablishActivation(ctx, record.Key, record.Revision, activationTopology, consumers, gatewayConsumers)
	}
	return agents, gateway, err
}

func (s *service) acknowledgedRoutePods(
	ctx context.Context, namespace string, eligible map[string]bool, podNames map[string]string, acks map[string]bool, established bool,
) ([]string, bool) {
	if len(eligible) == 0 {
		return nil, false
	}
	consumers := make([]string, 0, len(eligible))
	for uid := range eligible {
		consumer, ok := s.acknowledgedRoutePod(ctx, namespace, podNames[uid], uid, acks, established)
		if !ok {
			return nil, false
		}
		consumers = append(consumers, consumer)
	}
	return consumers, true
}

func routeAgentConsumer(ctx context.Context, agent *state.AgentSession) (string, error) {
	consumer, _, err := routeAgentConsumerState(ctx, agent)
	return consumer, err
}

func routeAgentConsumerState(ctx context.Context, agent *state.AgentSession) (string, time.Time, error) {
	if agent == nil || agent.PodUid == "" || agent.PodName == "" || agent.RouteGuardInstance == "" {
		return "", time.Time{}, fmt.Errorf("agent route process identity is missing")
	}
	pod, err := k8sapi.GetK8sInterface(ctx).CoreV1().Pods(agent.Namespace).Get(ctx, agent.PodName, metav1.GetOptions{})
	if err != nil {
		return "", time.Time{}, err
	}
	if string(pod.UID) != agent.PodUid {
		return "", time.Time{}, fmt.Errorf("agent pod UID changed")
	}
	prefix, started, err := runningRouteAgentContainer(pod)
	if err != nil {
		return "", time.Time{}, err
	}
	return prefix + agent.RouteGuardInstance, started, nil
}

// Actual EndpointSlices include terminating endpoints that may still serve.
// The complete initial barrier also includes nonterminal selector-matching Pods,
// even if Pending: they may have loaded an empty snapshot before route creation.
func routableRoutePodUIDs(ctx context.Context, predicate routeintent.Predicate) (map[string]bool, error) {
	eligibility, err := currentRoutePodEligibility(ctx, predicate)
	return eligibility.selected, err
}

type routePodEligibility struct {
	selected map[string]bool
	serving  map[string]bool
	podNames map[string]string
}

func (e routePodEligibility) addEndpointPod(uid, name string) bool {
	if name != "" {
		if previous := e.podNames[uid]; previous != "" && previous != name {
			return false
		}
		e.podNames[uid] = name
	}
	e.selected[uid], e.serving[uid] = true, true
	return true
}

func currentRoutePodEligibility(ctx context.Context, predicate routeintent.Predicate) (routePodEligibility, error) {
	client := k8sapi.GetK8sInterface(ctx)
	out := routePodEligibility{selected: make(map[string]bool), serving: make(map[string]bool), podNames: make(map[string]string)}
	addSelectedPods := func(selector string) error {
		pods, err := client.CoreV1().Pods(predicate.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			return err
		}
		for _, pod := range pods.Items {
			if pod.Status.Phase == core.PodSucceeded || pod.Status.Phase == core.PodFailed {
				continue
			}
			if pod.UID == "" {
				return fmt.Errorf("selected pod %s/%s has no UID", predicate.Namespace, pod.Name)
			}
			uid := string(pod.UID)
			out.selected[uid] = true
			out.podNames[uid] = pod.Name
			for _, condition := range pod.Status.Conditions {
				if condition.Type == core.PodReady && condition.Status == core.ConditionTrue {
					out.serving[uid] = true
					break
				}
			}
		}
		return nil
	}
	if predicate.ServiceUID != "" {
		svc, err := client.CoreV1().Services(predicate.Namespace).Get(ctx, predicate.Service, metav1.GetOptions{})
		if err != nil {
			return out, err
		}
		if string(svc.UID) != predicate.ServiceUID {
			return out, fmt.Errorf("durable Service identity changed for %s/%s", predicate.Namespace, predicate.Service)
		}
		if len(svc.Spec.Selector) == 0 {
			return out, fmt.Errorf("durable Service %s/%s has no verifiable pod selector", predicate.Namespace, predicate.Service)
		}
		if err = addSelectedPods(labels.Set(svc.Spec.Selector).String()); err != nil {
			return out, err
		}
		options := metav1.ListOptions{LabelSelector: labels.Set{discovery.LabelServiceName: predicate.Service}.String()}
		slices, err := client.DiscoveryV1().EndpointSlices(predicate.Namespace).List(ctx, options)
		if err != nil {
			return out, err
		}
		for _, slice := range slices.Items {
			for _, endpoint := range slice.Endpoints {
				if endpoint.Conditions.Ready != nil && !*endpoint.Conditions.Ready && (endpoint.Conditions.Serving == nil || !*endpoint.Conditions.Serving) {
					continue
				}
				ref := endpoint.TargetRef
				if ref == nil || ref.Kind != "Pod" || ref.UID == "" || (ref.Namespace != "" && ref.Namespace != predicate.Namespace) {
					return out, fmt.Errorf("service %s/%s has a routable endpoint with no verifiable Pod UID", predicate.Namespace, predicate.Service)
				}
				uid := string(ref.UID)
				if !out.addEndpointPod(uid, ref.Name) {
					return out, fmt.Errorf("service %s/%s has conflicting names for Pod UID %s", predicate.Namespace, predicate.Service, uid)
				}
			}
		}
		return out, nil
	}
	wl, err := k8sapi.GetWorkload(ctx, predicate.Workload, predicate.Namespace, k8sapi.Kind(predicate.WorkloadKind))
	if err != nil {
		return out, err
	}
	selector, err := wl.Selector()
	if err != nil {
		return out, err
	}
	if err = addSelectedPods(selector.String()); err != nil {
		return out, err
	}
	return out, nil
}

func routeIntentHTTPProtocol(protocol string) bool {
	switch strings.ToLower(protocol) {
	case "http", "kubernetes.io/http", "http1", "http1.0", "http1.1", "http/1.0", "http/1.1", "ws", "kubernetes.io/ws",
		"h2c", "kubernetes.io/h2c", "https", "http2", "wss", "kubernetes.io/wss", "grpc":
		return true
	default:
		return false
	}
}

func routeIntentGatewayHTTPProtocol(protocol string) bool {
	switch strings.ToLower(protocol) {
	case "http", "kubernetes.io/http", "http1", "http1.0", "http1.1", "http/1.0", "http/1.1", "ws", "kubernetes.io/ws":
		return true
	default:
		return false
	}
}

func (s *service) routeIntentTargetAdvertisement(target *rpc.RouteIntentTarget) {
	if s.state == nil {
		return
	}
	conflict := false
	s.state.EachAgent(func(_ tunnel.SessionID, agent *state.AgentSession) bool {
		if agent.Namespace != target.Namespace || agent.Kind != target.WorkloadKind || agent.Name != target.WorkloadName {
			return true
		}
		for _, advertised := range agent.InterceptTargets {
			if !routeIntentAdvertisementMatches(target, advertised.ContainerPort, advertised.ServiceUid, advertised.ServicePort) {
				continue
			}
			if advertised.AgentPort <= 0 || advertised.AppProtocol == "" {
				continue
			}
			if (target.AgentPort != 0 && target.AgentPort != advertised.AgentPort) ||
				(target.AppProtocol != "" && !strings.EqualFold(target.AppProtocol, advertised.AppProtocol)) {
				conflict = true
				return false
			}
			target.AgentPort, target.AppProtocol = advertised.AgentPort, advertised.AppProtocol
		}
		return true
	})
	if conflict {
		target.AgentPort = 0
	}
}

func validateDurableDeclaredProtocol(ctx context.Context, predicate routeintent.Predicate) error {
	if predicate.ServiceUID == "" {
		return nil
	} // no Kubernetes workload-port application protocol field
	svc, err := k8sapi.GetK8sInterface(ctx).CoreV1().Services(predicate.Namespace).Get(ctx, predicate.Service, metav1.GetOptions{})
	if err != nil {
		return status.Error(codes.Unavailable, "unable to verify the local-route Service application protocol")
	}
	if string(svc.UID) != predicate.ServiceUID {
		return status.Error(codes.FailedPrecondition, "the local-route Service identity has changed")
	}
	if len(svc.Spec.Selector) == 0 {
		return status.Error(codes.FailedPrecondition, "a durable local-route Service requires a pod selector to fence newly joining agents")
	}
	for _, port := range svc.Spec.Ports {
		if port.Port == predicate.ServicePort && (port.Protocol == "" || port.Protocol == core.ProtocolTCP) {
			if port.AppProtocol != nil && *port.AppProtocol != "" && !routeIntentHTTPProtocol(*port.AppProtocol) {
				return status.Errorf(codes.FailedPrecondition, "application protocol %q cannot safely mediate an exact local HTTP route", *port.AppProtocol)
			}
			return nil // missing inference or TLS certificates must be proved by actual agent ACKs
		}
	}
	return status.Error(codes.FailedPrecondition, "the local-route Service no longer contains the selected TCP port")
}

func (s *service) routeIntentEligibleTargets(ctx context.Context) []*rpc.RouteIntentTarget {
	if s.state == nil {
		return nil
	}
	byKey := make(map[string]*rpc.RouteIntentTarget)
	s.state.EachAgent(func(_ tunnel.SessionID, agent *state.AgentSession) bool {
		if !s.routeIntents.managesNamespace(agent.Namespace) || !s.state.ManagesNamespace(ctx, agent.Namespace) {
			return true
		}
		for _, target := range agent.InterceptTargets {
			if !routeIntentGatewayHTTPProtocol(target.AppProtocol) || target.AgentPort <= 0 || !strings.EqualFold(target.Protocol, "TCP") {
				continue
			}
			key := fmt.Sprintf("%s/%s/%s/%d/%s/%d", agent.Namespace, agent.Kind, agent.Name, target.ContainerPort, target.ServiceUid, target.ServicePort)
			if existing := byKey[key]; existing != nil && existing.AgentPort != target.AgentPort {
				existing.AgentPort = 0
				continue
			}
			if _, exists := byKey[key]; !exists {
				byKey[key] = &rpc.RouteIntentTarget{
					Namespace: agent.Namespace, WorkloadKind: agent.Kind, WorkloadName: agent.Name,
					ContainerPort: target.ContainerPort, ServiceName: target.ServiceName, ServiceUid: target.ServiceUid,
					ServicePort: target.ServicePort, AgentPort: target.AgentPort, AppProtocol: target.AppProtocol,
				}
			}
		}
		return true
	})
	keys := make([]string, 0, len(byKey))
	for key, target := range byKey {
		if target.AgentPort > 0 {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	out := make([]*rpc.RouteIntentTarget, 0, len(keys))
	for _, key := range keys {
		out = append(out, byKey[key])
	}
	return out
}
