// Package routeintent persists the desired state of supported local HTTP routes.
package routeintent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/matcher"
)

const RoutingHeader = "X-Local-Routing-Key"

var (
	ErrNotFound = errors.New("route intent not found")
	ErrConflict = errors.New("route intent revision conflicts")
	ErrInvalid  = errors.New("invalid route intent")
)

// Key names a route within a stable, manager-authenticated owner's namespace.
// Owner must survive an ordinary reconnect; an ephemeral manager session ID is
// unsuitable. The caller is responsible for authenticating ownership.
type Key struct {
	Namespace string `json:"namespace"`
	Owner     string `json:"owner"`
	Name      string `json:"name"`
}

// Predicate contains only the exact local-routing header and workload/service
// targeting. It deliberately has no arbitrary headers, tokens, environment,
// client connection details, or other InterceptSpec/InterceptInfo data.
type Predicate struct {
	Namespace     string `json:"namespace"`
	WorkloadKind  string `json:"workloadKind"`
	Workload      string `json:"workload"`
	ContainerPort int32  `json:"containerPort"`
	Service       string `json:"service,omitempty"`
	ServiceUID    string `json:"serviceUID,omitempty"`
	ServicePort   int32  `json:"servicePort,omitempty"`
	RoutingKey    string `json:"routingKey"`
}

// PredicateFromSpec returns only the request predicate supported by this store:
// exactly one X-Local-Routing-Key equality filter and no additional header or
// path conditions. Unsupported specs must not be made durable by broadening
// their predicate.
func PredicateFromSpec(spec *rpc.InterceptSpec) (Predicate, bool) {
	if spec == nil || spec.Wiretap || spec.Replace || spec.NoDefaultPort ||
		len(spec.HeaderFilters) != 1 || len(spec.PathFilters) != 0 ||
		(spec.Protocol != "" && !strings.EqualFold(spec.Protocol, "TCP")) {
		return Predicate{}, false
	}
	var key string
	for name, value := range spec.HeaderFilters {
		if !strings.EqualFold(name, RoutingHeader) || value == "" || matcher.NewValue(value).Op() != matcher.ValueOpEqual {
			return Predicate{}, false
		}
		key = value
	}
	p := Predicate{
		Namespace:     spec.Namespace,
		WorkloadKind:  spec.WorkloadKind,
		Workload:      spec.Agent,
		ContainerPort: spec.ContainerPort,
		Service:       spec.ServiceName,
		ServiceUID:    spec.ServiceUid,
		ServicePort:   spec.ServicePort,
		RoutingKey:    key,
	}
	if p.validate() != nil {
		return Predicate{}, false
	}
	return p, true
}

// State is the durable owner intent. An absent owner connection is never an
// implicit transition from Desired to Removed.
type State string

const (
	Desired State = "desired"
	Removed State = "removed"
)

// Revision fences modifications to the observed incarnation and version.
// Version increases over a route's whole lifetime, including recreation.
type Revision struct {
	Incarnation string `json:"incarnation"`
	Version     uint64 `json:"version"`
}

// Record is the complete durable state of one named route. A Removed record is
// a tombstone and keeps the predicate so consumers can retire the correct route.
// LeaseUntil is optional; expiration by itself does not change State.
type Record struct {
	Key        Key       `json:"key"`
	Revision   Revision  `json:"revision"`
	State      State     `json:"state"`
	Predicate  Predicate `json:"predicate"`
	LeaseUntil time.Time `json:"leaseUntil,omitzero"`
}

// Change transitions an observed revision. For Desired, a new incarnation and
// complete predicate are required to recreate a Removed route. The predicate
// cannot change within a single incarnation; lease renewal can supply a zero
// predicate, and LeaseUntil replaces the previous value. For Removed, leave
// Predicate and LeaseUntil zero; the previous predicate is preserved.
type Change struct {
	State       State
	Incarnation string
	Predicate   Predicate
	LeaseUntil  time.Time
}

type Store interface {
	Create(context.Context, Key, string, Predicate, time.Time) (Record, error)
	Get(context.Context, Key) (Record, error)
	List(context.Context) ([]Record, error)
	Transition(context.Context, Key, Revision, Change) (Record, error)
	Acknowledge(context.Context, Key, Revision, string) error
	ObserveGatewayTopology(context.Context, Key, Revision, string) (string, error)
	AcknowledgeGateway(context.Context, Key, Revision, string, string) error
	Acknowledgments(context.Context, Key, Revision) (map[string]bool, error)
	ActivationEvidence(context.Context, Key, Revision, string) (map[string]bool, bool, error)
	EstablishActivation(context.Context, Key, Revision, string, []string, []string) error
	PruneAcknowledgments(context.Context, Key, Revision, func(context.Context) (map[string]bool, error)) error
}

// AgentOnlyActivation separates deployments that explicitly disable gateway
// proof from the persistent topology-bound proof of deployments that require it.
const AgentOnlyActivation = "agents-only"

func (k Key) validate() error {
	if strings.TrimSpace(k.Namespace) == "" || strings.TrimSpace(k.Owner) == "" || strings.TrimSpace(k.Name) == "" {
		return fmt.Errorf("%w: namespace, owner, and name are required", ErrInvalid)
	}
	return nil
}

func (p Predicate) validate() error {
	if strings.TrimSpace(p.Namespace) == "" || strings.TrimSpace(p.WorkloadKind) == "" || strings.TrimSpace(p.Workload) == "" ||
		p.ContainerPort < 1 || p.ContainerPort > 65535 || p.ServicePort < 0 || p.ServicePort > 65535 ||
		p.RoutingKey == "" || matcher.NewValue(p.RoutingKey).Op() != matcher.ValueOpEqual {
		return fmt.Errorf("%w: predicate must contain a namespace, workload kind and name, valid port, and an exact local routing key", ErrInvalid)
	}
	if (p.ServiceUID == "") != (p.ServicePort == 0) || (p.ServiceUID != "" && p.Service == "") {
		return fmt.Errorf("%w: service-backed predicates require service name, UID, and numeric port", ErrInvalid)
	}
	return nil
}

func (r Record) validate() error {
	if err := r.Key.validate(); err != nil {
		return err
	}
	if strings.TrimSpace(r.Revision.Incarnation) == "" || r.Revision.Version == 0 {
		return fmt.Errorf("%w: incarnation and positive version are required", ErrInvalid)
	}
	if r.State != Desired && r.State != Removed {
		return fmt.Errorf("%w: unsupported state %q", ErrInvalid, r.State)
	}
	if err := r.Predicate.validate(); err != nil {
		return err
	}
	if r.Key.Namespace != r.Predicate.Namespace {
		return fmt.Errorf("%w: route and predicate namespaces differ", ErrInvalid)
	}
	if r.State == Removed && !r.LeaseUntil.IsZero() {
		return fmt.Errorf("%w: removed route cannot have a lease", ErrInvalid)
	}
	return nil
}

func (r Record) change(expected Revision, c Change) (Record, error) {
	if r.Revision != expected || (c.Incarnation != "" && r.Revision.Incarnation != c.Incarnation && r.State != Removed) {
		return Record{}, fmt.Errorf("%w: route is no longer at the expected incarnation and version", ErrConflict)
	}
	if c.State == Removed && (c.Predicate != (Predicate{}) || !c.LeaseUntil.IsZero()) {
		return Record{}, fmt.Errorf("%w: removal must not replace the predicate or lease", ErrInvalid)
	}
	if c.State != Desired && c.State != Removed {
		return Record{}, fmt.Errorf("%w: unsupported target state %q", ErrInvalid, c.State)
	}
	next := r
	next.State = c.State
	next.LeaseUntil = c.LeaseUntil.UTC()
	if r.State == Removed {
		if c.State == Removed {
			if c.Incarnation != "" && c.Incarnation != r.Revision.Incarnation {
				return Record{}, fmt.Errorf("%w: removal targets a different incarnation", ErrConflict)
			}
			return r, nil
		}
		if strings.TrimSpace(c.Incarnation) == "" || c.Incarnation == r.Revision.Incarnation {
			return Record{}, fmt.Errorf("%w: recreation must use a new incarnation", ErrConflict)
		}
		next.Revision.Incarnation = c.Incarnation
		next.Predicate = c.Predicate
	} else if c.Predicate != (Predicate{}) && c.Predicate != r.Predicate {
		return Record{}, fmt.Errorf("%w: predicate cannot change within an incarnation", ErrInvalid)
	}
	if next == r {
		return r, nil
	}
	if next.Revision.Version == ^uint64(0) {
		return Record{}, fmt.Errorf("%w: route version exhausted", ErrInvalid)
	}
	next.Revision.Version++
	if err := next.validate(); err != nil {
		return Record{}, err
	}
	return next, nil
}
