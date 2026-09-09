package routeintent

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"slices"
	"strings"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
)

// EstablishActivation persists that the manager verified the complete initial
// selected-Pod barrier. The compare-and-set rechecks every supplied exact Pod
// process proof and an authorized gateway proof for the current topology; a
// racing process replacement or topology change cannot establish stale proof.
func (s *Kubernetes) EstablishActivation(ctx context.Context, key Key, revision Revision, topology string, pods, gateways []string) error {
	if err := key.validate(); err != nil {
		return err
	}
	if topology == "" || len(pods) == 0 || slices.ContainsFunc(pods, func(name string) bool {
		_, ok := podConsumerUID(name)
		return !ok
	}) {
		return fmt.Errorf("%w: current activation topology and nonempty Pod proof are required", ErrInvalid)
	}
	if topology != AgentOnlyActivation && (len(gateways) == 0 || slices.ContainsFunc(gateways, func(name string) bool {
		return !strings.HasPrefix(name, "gateway:") || !strings.HasSuffix(name, ":"+topology)
	})) {
		return fmt.Errorf("%w: at least one authorized gateway is required", ErrInvalid)
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
		if record.Revision != revision || record.State != Desired {
			return ErrConflict
		}
		acks, err := decodeAcknowledgments(cm)
		if err != nil {
			return err
		}
		if acks.Revision != revision || (topology != AgentOnlyActivation && gatewayTopologyToken(acks.GatewayTopology, acks.GatewayGeneration) != topology) {
			return ErrConflict
		}
		if acks.ActivationTopology == topology {
			return nil
		}
		if slices.ContainsFunc(pods, func(name string) bool { return !acks.Consumers[name] }) ||
			(topology != AgentOnlyActivation && !slices.ContainsFunc(gateways, func(name string) bool { return acks.Consumers[name] })) {
			return ErrConflict
		}
		acks.ActivationTopology = topology
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
