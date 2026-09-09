package routeintent

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/base32"
	"encoding/binary"
	"encoding/hex"
	"encoding/json/v2"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/util/retry"
)

const (
	configMapPrefix    = "tp-route-"
	configMapLabel     = "telepresence.io/route-intent"
	configMapData      = "route.json"
	configMapAcks      = "acknowledgments.json"
	currentWireVersion = 1
)

// Kubernetes stores one named route per ConfigMap. Callers must provide the
// typed ConfigMap client scoped to the traffic manager's namespace. A
// Kubernetes resourceVersion compare-and-set protects each transition across
// simultaneously active manager pods.
type Kubernetes struct {
	maps typedcore.ConfigMapInterface
}

var _ Store = (*Kubernetes)(nil)

func NewKubernetes(maps typedcore.ConfigMapInterface) *Kubernetes {
	return &Kubernetes{maps: maps}
}

type wireRecord struct {
	Schema int    `json:"schema"`
	Record Record `json:"record"`
}

type wireAcknowledgments struct {
	Revision           Revision        `json:"revision"`
	Consumers          map[string]bool `json:"consumers"`
	GatewayTopology    string          `json:"gatewayTopology,omitempty"`
	GatewayGeneration  uint64          `json:"gatewayGeneration,omitempty"`
	ActivationTopology string          `json:"activationTopology,omitempty"`
}

func (s *Kubernetes) Acknowledgments(ctx context.Context, key Key, revision Revision) (map[string]bool, error) {
	consumers, _, err := s.ActivationEvidence(ctx, key, revision, "")
	return consumers, err
}

// ActivationEvidence returns route proof and whether the complete initial Pod
// and gateway barrier was previously established for this exact generation.
func (s *Kubernetes) ActivationEvidence(ctx context.Context, key Key, revision Revision, topology string) (map[string]bool, bool, error) {
	if err := key.validate(); err != nil {
		return nil, false, err
	}
	cm, err := s.maps.Get(ctx, configMapName(key), meta.GetOptions{})
	if kerrors.IsNotFound(err) {
		return nil, false, fmt.Errorf("%w: %w", ErrNotFound, err)
	}
	if err != nil {
		return nil, false, fmt.Errorf("get route acknowledgments: %w", err)
	}
	record, err := decode(cm, &key)
	if err != nil {
		return nil, false, err
	}
	if record.Revision != revision {
		return nil, false, ErrConflict
	}
	acks, err := decodeAcknowledgments(cm)
	if err != nil {
		return nil, false, err
	}
	if acks.Revision != revision {
		return map[string]bool{}, false, nil
	}
	return maps.Clone(acks.Consumers), topology != "" && acks.ActivationTopology == topology, nil
}

func (s *Kubernetes) Acknowledge(ctx context.Context, key Key, revision Revision, consumer string) error {
	return s.acknowledge(ctx, key, revision, consumer, "")
}

// AcknowledgeGateway accepts only proof for the topology generation currently
// stored. A topology change racing this write either invalidates it or makes this
// CAS retry and reject the old token.
func (s *Kubernetes) AcknowledgeGateway(ctx context.Context, key Key, revision Revision, consumer, token string) error {
	if !strings.HasPrefix(consumer, "gateway:") || token == "" {
		return fmt.Errorf("%w: gateway consumer and topology token are required", ErrInvalid)
	}
	return s.acknowledge(ctx, key, revision, consumer+":"+token, token)
}

func (s *Kubernetes) acknowledge(ctx context.Context, key Key, revision Revision, consumer, topologyToken string) error {
	if err := key.validate(); err != nil {
		return err
	}
	if revision.Incarnation == "" || revision.Version == 0 || strings.TrimSpace(consumer) == "" || len(consumer) > 512 {
		return fmt.Errorf("%w: route revision and bounded consumer are required", ErrInvalid)
	}
	backoff := retry.DefaultRetry
	backoff.Steps = 12
	return retry.RetryOnConflict(backoff, func() error {
		cm, err := s.maps.Get(ctx, configMapName(key), meta.GetOptions{})
		if kerrors.IsNotFound(err) {
			return fmt.Errorf("%w: %w", ErrNotFound, err)
		}
		if err != nil {
			return err
		}
		record, err := decode(cm, &key)
		if err != nil {
			return err
		}
		if record.Revision != revision {
			return ErrConflict
		}
		acks, err := decodeAcknowledgments(cm)
		if err != nil {
			return err
		}
		if acks.Revision != revision {
			acks = wireAcknowledgments{Revision: revision, Consumers: make(map[string]bool)}
		}
		if topologyToken != "" && gatewayTopologyToken(acks.GatewayTopology, acks.GatewayGeneration) != topologyToken {
			return ErrConflict
		}
		if acks.Consumers[consumer] {
			return nil
		}
		if acks.Consumers == nil {
			acks.Consumers = make(map[string]bool)
		}
		if uid, pod := podConsumerUID(consumer); pod {
			for old := range acks.Consumers {
				if oldUID, oldPod := podConsumerUID(old); oldPod && uid == oldUID {
					delete(acks.Consumers, old)
				}
			}
		}
		acks.Consumers[consumer] = true
		content, err := json.Marshal(acks)
		if err != nil {
			return err
		}
		cm = cm.DeepCopy()
		cm.Data[configMapAcks] = string(content)
		_, err = s.maps.Update(ctx, cm, meta.UpdateOptions{})
		return err
	})
}

// ObserveGatewayTopology persistently fences transitions such as A -> B -> A.
// Reobserving the same topology (including across manager restarts) keeps its
// token. Any change clears gateway proofs but retains all current agent proofs.
func (s *Kubernetes) ObserveGatewayTopology(ctx context.Context, key Key, revision Revision, topology string) (string, error) {
	if err := key.validate(); err != nil {
		return "", err
	}
	decoded, err := hex.DecodeString(topology)
	if err != nil || len(decoded) != sha256.Size || strings.ToLower(topology) != topology {
		return "", fmt.Errorf("%w: gateway topology must be a lowercase SHA256 digest", ErrInvalid)
	}
	var token string
	backoff := retry.DefaultRetry
	backoff.Steps = 12
	err = retry.RetryOnConflict(backoff, func() error {
		cm, err := s.maps.Get(ctx, configMapName(key), meta.GetOptions{})
		if kerrors.IsNotFound(err) {
			return fmt.Errorf("%w: %w", ErrNotFound, err)
		}
		if err != nil {
			return err
		}
		record, err := decode(cm, &key)
		if err != nil {
			return err
		}
		if record.Revision != revision || record.State != Desired {
			return ErrConflict
		}
		acks, err := decodeAcknowledgments(cm)
		if err != nil {
			return err
		}
		if acks.Revision != revision {
			acks = wireAcknowledgments{Revision: revision, Consumers: make(map[string]bool)}
		}
		if acks.GatewayTopology == topology && acks.GatewayGeneration != 0 {
			token = gatewayTopologyToken(topology, acks.GatewayGeneration)
			return nil
		}
		if acks.GatewayGeneration == ^uint64(0) {
			return fmt.Errorf("%w: gateway topology generation exhausted", ErrInvalid)
		}
		acks.GatewayTopology = topology
		acks.GatewayGeneration++
		if acks.ActivationTopology != AgentOnlyActivation {
			acks.ActivationTopology = ""
		}
		for consumer := range acks.Consumers {
			if strings.HasPrefix(consumer, "gateway:") {
				delete(acks.Consumers, consumer)
			}
		}
		token = gatewayTopologyToken(topology, acks.GatewayGeneration)
		content, err := json.Marshal(acks)
		if err != nil {
			return err
		}
		cm = cm.DeepCopy()
		cm.Data[configMapAcks] = string(content)
		_, err = s.maps.Update(ctx, cm, meta.UpdateOptions{})
		return err
	})
	return token, err
}

func gatewayTopologyToken(topology string, generation uint64) string {
	if topology == "" || generation == 0 {
		return ""
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("telepresence-gateway-topology-v1\x00"))
	_, _ = hash.Write(binary.BigEndian.AppendUint64(nil, generation))
	_, _ = hash.Write([]byte(topology))
	return hex.EncodeToString(hash.Sum(nil))
}

// PruneAcknowledgments removes only departed Pod UIDs. The live-pod reader is
// called after every ConfigMap read, and a concurrent ACK makes the update CAS
// fail and repeat both reads; an ACK created after a stale Pod list cannot be lost.
func (s *Kubernetes) PruneAcknowledgments(ctx context.Context, key Key, revision Revision, livePods func(context.Context) (map[string]bool, error)) error {
	if err := key.validate(); err != nil {
		return err
	}
	backoff := retry.DefaultRetry
	backoff.Steps = 12
	return retry.RetryOnConflict(backoff, func() error {
		cm, err := s.maps.Get(ctx, configMapName(key), meta.GetOptions{})
		if err != nil {
			return err
		}
		record, err := decode(cm, &key)
		if err != nil {
			return err
		}
		if record.Revision != revision {
			return ErrConflict
		}
		acks, err := decodeAcknowledgments(cm)
		if err != nil {
			return err
		}
		if acks.Revision != revision {
			return nil
		}
		live, err := livePods(ctx)
		if err != nil {
			return err
		}
		changed := false
		for consumer := range acks.Consumers {
			if uid, pod := podConsumerUID(consumer); pod && !live[uid] {
				delete(acks.Consumers, consumer)
				changed = true
			}
		}
		if !changed {
			return nil
		}
		content, err := json.Marshal(acks)
		if err != nil {
			return err
		}
		cm = cm.DeepCopy()
		cm.Data[configMapAcks] = string(content)
		_, err = s.maps.Update(ctx, cm, meta.UpdateOptions{})
		return err
	})
}

func podConsumerUID(consumer string) (string, bool) {
	if !strings.HasPrefix(consumer, "pod:") {
		return "", false
	}
	uid, _, _ := strings.Cut(strings.TrimPrefix(consumer, "pod:"), ":")
	return uid, uid != ""
}

func decodeAcknowledgments(cm *core.ConfigMap) (wireAcknowledgments, error) {
	var acks wireAcknowledgments
	if data := cm.Data[configMapAcks]; data != "" {
		if err := json.Unmarshal([]byte(data), &acks); err != nil {
			return acks, fmt.Errorf("%w: route acknowledgments cannot be decoded: %w", ErrInvalid, err)
		}
	}
	return acks, nil
}

// Create initially creates a route. An identical retry of the same active
// incarnation returns its current state without overwriting its lease; an
// earlier call retried after removal or recreation returns ErrConflict.
func (s *Kubernetes) Create(ctx context.Context, key Key, incarnation string, predicate Predicate, lease time.Time) (Record, error) {
	record := Record{
		Key: key, Revision: Revision{Incarnation: incarnation, Version: 1},
		State: Desired, Predicate: predicate, LeaseUntil: lease.UTC(),
	}
	if err := record.validate(); err != nil {
		return Record{}, err
	}
	data, err := encode(record)
	if err != nil {
		return Record{}, err
	}
	cm, err := s.maps.Create(ctx, &core.ConfigMap{
		ObjectMeta: meta.ObjectMeta{Name: configMapName(key), Labels: map[string]string{configMapLabel: "true"}},
		Data:       map[string]string{configMapData: data},
	}, meta.CreateOptions{})
	if kerrors.IsAlreadyExists(err) {
		current, getErr := s.Get(ctx, key)
		if getErr == nil && current.State == Desired && current.Revision.Incarnation == incarnation && current.Predicate == predicate {
			return current, nil
		}
		if getErr != nil {
			return Record{}, getErr
		}
		return Record{}, fmt.Errorf("%w: %w", ErrConflict, err)
	}
	if err != nil {
		return Record{}, fmt.Errorf("create route intent: %w", err)
	}
	return decode(cm, &key)
}

func (s *Kubernetes) Get(ctx context.Context, key Key) (Record, error) {
	if err := key.validate(); err != nil {
		return Record{}, err
	}
	cm, err := s.maps.Get(ctx, configMapName(key), meta.GetOptions{})
	if kerrors.IsNotFound(err) {
		return Record{}, fmt.Errorf("%w: %w", ErrNotFound, err)
	}
	if err != nil {
		return Record{}, fmt.Errorf("get route intent: %w", err)
	}
	return decode(cm, &key)
}

// List is the startup synchronization barrier. It returns all desired routes
// and tombstones from one Kubernetes list snapshot. No partial data is returned
// on an API or decoding failure, including data from a newer schema that this
// manager cannot safely consider authoritative.
func (s *Kubernetes) List(ctx context.Context) ([]Record, error) {
	cms, err := s.maps.List(ctx, meta.ListOptions{LabelSelector: labels.Set{configMapLabel: "true"}.String()})
	if err != nil {
		return nil, fmt.Errorf("list route intents: %w", err)
	}
	routes := make([]Record, 0, len(cms.Items))
	for i := range cms.Items {
		route, err := decode(&cms.Items[i], nil)
		if err != nil {
			return nil, err
		}
		routes = append(routes, route)
	}
	slices.SortFunc(routes, func(a, b Record) int {
		return cmp.Or(cmp.Compare(a.Key.Namespace, b.Key.Namespace), cmp.Compare(a.Key.Owner, b.Key.Owner), cmp.Compare(a.Key.Name, b.Key.Name))
	})
	return routes, nil
}

// Transition uses both the application's incarnation/version and Kubernetes'
// resource version. The caller must reread and make a new explicit decision
// after ErrConflict; the store never retries a write against a new generation.
func (s *Kubernetes) Transition(ctx context.Context, key Key, expected Revision, change Change) (Record, error) {
	if err := key.validate(); err != nil {
		return Record{}, err
	}
	if strings.TrimSpace(expected.Incarnation) == "" || expected.Version == 0 {
		return Record{}, fmt.Errorf("%w: expected incarnation and positive version are required", ErrInvalid)
	}
	var result Record
	backoff := retry.DefaultRetry
	backoff.Steps = 12
	err := retry.RetryOnConflict(backoff, func() error {
		var err error
		result, err = s.transitionOnce(ctx, key, expected, change)
		return err
	})
	if kerrors.IsConflict(err) {
		return Record{}, fmt.Errorf("%w: %w", ErrConflict, err)
	}
	return result, err
}

func (s *Kubernetes) transitionOnce(ctx context.Context, key Key, expected Revision, change Change) (Record, error) {
	cm, err := s.maps.Get(ctx, configMapName(key), meta.GetOptions{})
	if kerrors.IsNotFound(err) {
		return Record{}, fmt.Errorf("%w: %w", ErrNotFound, err)
	}
	if err != nil {
		return Record{}, fmt.Errorf("get route intent for transition: %w", err)
	}
	current, err := decode(cm, &key)
	if err != nil {
		return Record{}, err
	}
	next, err := current.change(expected, change)
	if err != nil || current == next {
		return next, err
	}
	data, err := encode(next)
	if err != nil {
		return Record{}, err
	}
	cm = cm.DeepCopy()
	cm.Data[configMapData] = data
	cm, err = s.maps.Update(ctx, cm, meta.UpdateOptions{})
	if kerrors.IsConflict(err) {
		return Record{}, err
	} // possibly only ACK metadata; re-read and re-check logical revision
	if kerrors.IsNotFound(err) {
		return Record{}, fmt.Errorf("%w: %w", ErrConflict, err)
	}
	if err != nil {
		return Record{}, fmt.Errorf("update route intent: %w", err)
	}
	return decode(cm, &key)
}

func configMapName(key Key) string {
	hash := sha256.New()
	for _, part := range []string{key.Namespace, key.Owner, key.Name} {
		_, _ = hash.Write(binary.BigEndian.AppendUint64(nil, uint64(len(part))))
		_, _ = hash.Write([]byte(part))
	}
	return configMapPrefix + strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(hash.Sum(nil)))
}

func encode(record Record) (string, error) {
	data, err := json.Marshal(wireRecord{Schema: currentWireVersion, Record: record})
	if err != nil {
		return "", fmt.Errorf("%w: route intent cannot be encoded: %w", ErrInvalid, err)
	}
	return string(data), nil
}

func decode(cm *core.ConfigMap, expectedKey *Key) (Record, error) {
	if cm.Labels[configMapLabel] != "true" {
		return Record{}, fmt.Errorf("%w: ConfigMap %s is not labeled as a route intent", ErrInvalid, cm.Name)
	}
	var wire wireRecord
	if err := json.Unmarshal([]byte(cm.Data[configMapData]), &wire); err != nil {
		return Record{}, fmt.Errorf("%w: ConfigMap %s cannot be decoded: %w", ErrInvalid, cm.Name, err)
	}
	if wire.Schema != currentWireVersion {
		return Record{}, fmt.Errorf("%w: ConfigMap %s has unsupported schema %d", ErrInvalid, cm.Name, wire.Schema)
	}
	record := wire.Record
	if err := record.validate(); err != nil {
		return Record{}, fmt.Errorf("ConfigMap %s: %w", cm.Name, err)
	}
	if (expectedKey != nil && *expectedKey != record.Key) || configMapName(record.Key) != cm.Name {
		return Record{}, fmt.Errorf("%w: route key does not match ConfigMap %s", ErrInvalid, cm.Name)
	}
	return record, nil
}
