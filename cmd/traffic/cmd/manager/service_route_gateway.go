package manager

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/routeintent"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/state"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
)

type gatewayTopologyTarget struct {
	namespace, kind, name string
	container, agent      int32
	protocol              string
}

// gatewayRouteToken recomputes the current workload/port/protocol mapping and
// persistently invalidates gateway proof on every known change. A temporarily
// unknown supported mapping blocks activation without destroying valid evidence
// for an unchanged topology across a manager or agent restart.
func (s *service) gatewayRouteToken(ctx context.Context, record routeintent.Record) (string, error) {
	topology, supported, err := s.currentGatewayTopology(ctx, record.Predicate)
	if err != nil || topology == "" {
		return "", err
	}
	token, err := s.routeIntents.store.ObserveGatewayTopology(ctx, record.Key, record.Revision, topology)
	if err != nil || !supported {
		return "", err
	}
	return token, nil
}

func (s *service) currentGatewayTopology(ctx context.Context, predicate routeintent.Predicate) (string, bool, error) {
	primary := &rpc.RouteIntentTarget{
		Namespace: predicate.Namespace, WorkloadKind: predicate.WorkloadKind, WorkloadName: predicate.Workload,
		ContainerPort: predicate.ContainerPort, ServiceName: predicate.Service, ServiceUid: predicate.ServiceUID, ServicePort: predicate.ServicePort,
	}
	targets := []*rpc.RouteIntentTarget{primary}
	serviceScoped := predicate.ServiceUID != ""
	if serviceScoped {
		service, err := k8sapi.GetK8sInterface(ctx).CoreV1().Services(predicate.Namespace).Get(ctx, predicate.Service, metav1.GetOptions{})
		if err != nil {
			return "", false, err
		}
		if string(service.UID) != predicate.ServiceUID || len(service.Spec.Selector) == 0 {
			return "", false, fmt.Errorf("gateway Service identity or selector is not valid for %s/%s", predicate.Namespace, predicate.Service)
		}
		targets, err = globalRouteIntentTargets(ctx, predicate, primary)
		if err != nil {
			return "", false, err
		}
	}
	allSupported, unknown := true, false
	entries := make([]gatewayTopologyTarget, 0, len(targets))
	for _, target := range targets {
		port, observedProtocol, seen, ambiguous := s.gatewayTargetAdvertisement(target)
		protocol := strings.ToLower(target.AppProtocol)
		if !serviceScoped {
			if !seen {
				unknown = true
			}
			protocol = observedProtocol
			if ambiguous {
				protocol = "!ambiguous:" + observedProtocol
			}
		}
		if !routeIntentGatewayHTTPProtocol(protocol) {
			allSupported = false
		} else if !seen || ambiguous || port == 0 || observedProtocol != protocol {
			unknown = true
		}
		entries = append(entries, gatewayTopologyTarget{target.Namespace, target.WorkloadKind, target.WorkloadName, target.ContainerPort, port, protocol})
	}
	if len(entries) == 0 || (allSupported && unknown) || (!serviceScoped && unknown) {
		return "", false, nil
	}
	return hashGatewayTopology(entries), allSupported, nil
}

func (s *service) gatewayTargetAdvertisement(target *rpc.RouteIntentTarget) (int32, string, bool, bool) {
	if s.state == nil {
		return 0, "", false, false
	}
	var port int32
	seen, ambiguous := false, false
	protocols := make(map[string]bool)
	s.state.EachAgent(func(_ tunnel.SessionID, agent *state.AgentSession) bool {
		if agent.Namespace != target.Namespace || agent.Kind != target.WorkloadKind || agent.Name != target.WorkloadName {
			return true
		}
		for _, advertised := range agent.InterceptTargets {
			if !routeIntentAdvertisementMatches(target, advertised.ContainerPort, advertised.ServiceUid, advertised.ServicePort) ||
				!strings.EqualFold(advertised.Protocol, "TCP") || advertised.AgentPort <= 0 {
				continue
			}
			seen = true
			protocols[strings.ToLower(advertised.AppProtocol)] = true
			if port != 0 && port != advertised.AgentPort {
				ambiguous = true
			} else if port == 0 {
				port = advertised.AgentPort
			}
		}
		return true
	})
	values := make([]string, 0, len(protocols))
	for protocol := range protocols {
		values = append(values, protocol)
	}
	slices.Sort(values)
	if len(values) > 1 || ambiguous {
		return 0, strings.Join(values, ","), seen, true
	}
	return port, strings.Join(values, ""), seen, false
}

func hashGatewayTopology(targets []gatewayTopologyTarget) string {
	rows := make([]string, 0, len(targets))
	for _, target := range targets {
		parts := []string{target.namespace, target.kind, target.name, strconv.Itoa(int(target.container)), strconv.Itoa(int(target.agent)), target.protocol}
		var row []byte
		for _, part := range parts {
			row = binary.BigEndian.AppendUint64(row, uint64(len(part)))
			row = append(row, part...)
		}
		rows = append(rows, string(row))
	}
	slices.Sort(rows)
	rows = slices.Compact(rows)
	hash := sha256.New()
	_, _ = hash.Write([]byte("telepresence-gateway-targets-v1\x00"))
	for _, row := range rows {
		_, _ = hash.Write(binary.BigEndian.AppendUint64(nil, uint64(len(row))))
		_, _ = hash.Write([]byte(row))
	}
	return hex.EncodeToString(hash.Sum(nil))
}
