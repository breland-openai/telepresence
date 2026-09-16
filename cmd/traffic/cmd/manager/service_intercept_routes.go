package manager

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"maps"
	"slices"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"k8s.io/apimachinery/pkg/util/validation"

	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/auth"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/state"
	"github.com/telepresenceio/telepresence/v2/pkg/cache"
	"github.com/telepresenceio/telepresence/v2/pkg/grpc/errors"
)

const (
	interceptRouteWatchDuration       = 5 * time.Minute
	interceptRouteWatchNamespaceLimit = 256
)

func (s *service) WatchInterceptRoutes(request *rpc.WatchInterceptRoutesRequest, stream grpc.ServerStreamingServer[rpc.InterceptRouteSnapshot]) error {
	return s.watchInterceptRoutes(request, stream, interceptRouteWatchDuration)
}

func (s *service) watchInterceptRoutes(request *rpc.WatchInterceptRoutesRequest, stream grpc.ServerStreamingServer[rpc.InterceptRouteSnapshot], duration time.Duration) error {
	ctx := stream.Context()
	principal := auth.PrincipalFrom(ctx)
	if principal == nil {
		if auth.AuthUnavailable(ctx) {
			return errors.Error(codes.Unavailable, "unable to authenticate the intercept routing observer")
		}
		return errors.Error(codes.Unauthenticated, "watching intercept routes requires an authenticated caller")
	}
	requested := request.GetNamespaces()
	if len(requested) == 0 {
		return errors.Error(codes.InvalidArgument, "at least one explicit namespace is required to watch intercept routes")
	}
	if len(requested) > interceptRouteWatchNamespaceLimit {
		return errors.Errorf(codes.InvalidArgument, "intercept route watches are limited to %d namespaces per request", interceptRouteWatchNamespaceLimit)
	}
	requested = slices.Compact(slices.Sorted(slices.Values(requested)))
	for _, namespace := range requested {
		if len(validation.IsDNS1123Label(namespace)) != 0 {
			return errors.Errorf(codes.InvalidArgument, "invalid namespace %q for intercept routes", namespace)
		}
	}
	namespaces := make(map[string]struct{}, len(requested))
	for _, namespace := range requested {
		allowed, err := s.authorizer.CanWatchInterceptRoutes(ctx, principal, namespace)
		if err != nil {
			return errors.Errorf(codes.Unavailable, "unable to determine whether %s may watch interceptroutes.telepresence.io in namespace %s: %v", principal.Username, namespace, err)
		}
		if !allowed {
			return errors.Errorf(codes.PermissionDenied, "%s is not permitted to watch interceptroutes.telepresence.io in namespace %s", principal.Username, namespace)
		}
		namespaces[namespace] = struct{}{}
	}
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	deltas := s.state.WatchIntercepts(ctx, func(_ string, intercept *state.Intercept) bool {
		spec := intercept.GetSpec()
		if spec == nil || intercept.GetDisposition() == rpc.InterceptDispositionType_REMOVED || state.IsChildIntercept(spec) {
			return false
		}
		_, allowed := namespaces[spec.GetNamespace()]
		return allowed
	})
	snapshot := cache.NewClientMap[string, *state.Intercept]()
	return snapshot.Watch(ctx.Done(), deltas, func() error {
		routes := make([]*rpc.InterceptRoute, 0, snapshot.Size())
		snapshot.Range(func(_ string, intercept *state.Intercept) bool {
			routes = append(routes, interceptRoute(intercept.InterceptInfo, namespaces))
			return true
		})
		slices.SortFunc(routes, func(a, b *rpc.InterceptRoute) int { return cmp.Compare(a.GetId(), b.GetId()) })
		return stream.Send(&rpc.InterceptRouteSnapshot{Routes: routes})
	})
}

func interceptRoute(info *rpc.InterceptInfo, namespaces map[string]struct{}) *rpc.InterceptRoute {
	spec := info.GetSpec()
	id := sha256.Sum256([]byte(info.GetId()))
	route := &rpc.InterceptRoute{
		Id:            hex.EncodeToString(id[:]),
		Name:          spec.GetName(),
		Namespace:     spec.GetNamespace(),
		WorkloadName:  spec.GetAgent(),
		WorkloadKind:  spec.GetWorkloadKind(),
		Mechanism:     spec.GetMechanism(),
		ContainerPort: spec.GetContainerPort(),
		Disposition:   info.GetDisposition(),
		Wiretap:       spec.GetWiretap(),
		HeaderFilters: maps.Clone(spec.GetHeaderFilters()),
		PathFilters:   slices.Clone(spec.GetPathFilters()),
	}
	for _, workload := range info.GetServiceWorkloads() {
		namespace := workload.GetNamespace()
		if namespace == "" {
			namespace = spec.GetNamespace()
		}
		if _, allowed := namespaces[namespace]; allowed {
			route.ServiceWorkloads = append(route.ServiceWorkloads, &rpc.InterceptRouteWorkload{
				Namespace:    namespace,
				WorkloadKind: workload.GetWorkloadKind(),
				WorkloadName: workload.GetWorkloadName(),
			})
		}
	}
	return route
}
