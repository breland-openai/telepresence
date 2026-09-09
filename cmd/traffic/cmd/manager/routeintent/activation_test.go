package routeintent

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEstablishedActivationRequiresExactStoredEvidenceAndSurvivesRestart(t *testing.T) {
	ctx := t.Context()
	client := resourceVersionClient()
	store := NewKubernetes(client.CoreV1().ConfigMaps("manager"))
	record, err := store.Create(ctx, testKey, "one", testPredicate, time.Time{})
	require.NoError(t, err)
	a, b := strings.Repeat("a", 64), strings.Repeat("b", 64)
	tokenA, err := store.ObserveGatewayTopology(ctx, testKey, record.Revision, a)
	require.NoError(t, err)
	const fast, slow, newer = "pod:fast:container:process", "pod:slow:container:process", "pod:slow:new-container:process"
	const controller = "gateway:system:serviceaccount:default:gateway"
	gateways := []string{controller + ":" + tokenA}
	_, established, err := store.ActivationEvidence(ctx, testKey, record.Revision, tokenA)
	require.NoError(t, err)
	require.False(t, established)
	require.NoError(t, store.Acknowledge(ctx, testKey, record.Revision, fast))
	require.ErrorIs(t, store.EstablishActivation(ctx, testKey, record.Revision, tokenA, []string{fast, slow}, gateways), ErrConflict)
	require.NoError(t, store.Acknowledge(ctx, testKey, record.Revision, slow))
	require.ErrorIs(t, store.EstablishActivation(ctx, testKey, record.Revision, tokenA, []string{fast, slow}, gateways), ErrConflict)
	require.NoError(t, store.AcknowledgeGateway(ctx, testKey, record.Revision, controller, tokenA))
	require.NoError(t, store.Acknowledge(ctx, testKey, record.Revision, newer))
	require.ErrorIs(t, store.EstablishActivation(ctx, testKey, record.Revision, tokenA, []string{fast, slow}, gateways), ErrConflict)
	require.NoError(t, store.EstablishActivation(ctx, testKey, record.Revision, tokenA, []string{fast, newer}, gateways))
	restarted := NewKubernetes(client.CoreV1().ConfigMaps("manager"))
	_, established, err = restarted.ActivationEvidence(ctx, testKey, record.Revision, tokenA)
	require.NoError(t, err)
	require.True(t, established)
	_, established, err = restarted.ActivationEvidence(ctx, testKey, record.Revision, AgentOnlyActivation)
	require.NoError(t, err)
	require.False(t, established)
	tokenB, err := restarted.ObserveGatewayTopology(ctx, testKey, record.Revision, b)
	require.NoError(t, err)
	_, established, err = store.ActivationEvidence(ctx, testKey, record.Revision, tokenA)
	require.NoError(t, err)
	require.False(t, established)
	require.ErrorIs(t, store.EstablishActivation(ctx, testKey, record.Revision, tokenA, []string{fast, newer}, gateways), ErrConflict)
	_, established, err = store.ActivationEvidence(ctx, testKey, record.Revision, tokenB)
	require.NoError(t, err)
	require.False(t, established)
	tokenAgain, err := store.ObserveGatewayTopology(ctx, testKey, record.Revision, a)
	require.NoError(t, err)
	require.NotEqual(t, tokenA, tokenAgain)
	_, established, err = store.ActivationEvidence(ctx, testKey, record.Revision, tokenAgain)
	require.NoError(t, err)
	require.False(t, established)
	require.NoError(t, store.AcknowledgeGateway(ctx, testKey, record.Revision, controller, tokenAgain))
	require.NoError(t, store.EstablishActivation(ctx, testKey, record.Revision, tokenAgain, []string{fast, newer}, []string{controller + ":" + tokenAgain}))
	removed, err := store.Transition(ctx, testKey, record.Revision, Change{State: Removed})
	require.NoError(t, err)
	recreated, err := store.Transition(ctx, testKey, removed.Revision, Change{State: Desired, Incarnation: "two", Predicate: testPredicate})
	require.NoError(t, err)
	_, established, err = restarted.ActivationEvidence(ctx, testKey, recreated.Revision, tokenAgain)
	require.NoError(t, err)
	require.False(t, established, "a fresh initial activation must not inherit another incarnation")
}

func TestAgentOnlyActivationIsSeparateFromRequiredGateway(t *testing.T) {
	ctx := t.Context()
	client := resourceVersionClient()
	store := NewKubernetes(client.CoreV1().ConfigMaps("manager"))
	record, err := store.Create(ctx, testKey, "one", testPredicate, time.Time{})
	require.NoError(t, err)
	const pod = "pod:uid:container:process"
	require.ErrorIs(t, store.EstablishActivation(ctx, testKey, record.Revision, AgentOnlyActivation, nil, nil), ErrInvalid)
	require.ErrorIs(t, store.EstablishActivation(ctx, testKey, record.Revision, AgentOnlyActivation, []string{pod}, nil), ErrConflict)
	require.NoError(t, store.Acknowledge(ctx, testKey, record.Revision, pod))
	require.NoError(t, store.EstablishActivation(ctx, testKey, record.Revision, AgentOnlyActivation, []string{pod}, nil))
	token, err := store.ObserveGatewayTopology(ctx, testKey, record.Revision, strings.Repeat("a", 64))
	require.NoError(t, err)
	_, established, err := store.ActivationEvidence(ctx, testKey, record.Revision, AgentOnlyActivation)
	require.NoError(t, err)
	require.True(t, established)
	_, established, err = store.ActivationEvidence(ctx, testKey, record.Revision, token)
	require.NoError(t, err)
	require.False(t, established)
}
