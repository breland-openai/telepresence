package agent

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/telepresenceio/clog/testutil"
)

func TestDrainMainRejectsInvalidDurations(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "missing duration", want: "exactly one positive duration"},
		{name: "extra arguments", args: []string{"1s", "2s"}, want: "exactly one positive duration"},
		{name: "malformed duration", args: []string{"soon"}, want: "invalid agent-drain duration"},
		{name: "missing duration unit", args: []string{"5"}, want: "invalid agent-drain duration"},
		{name: "zero duration", args: []string{"0s"}, want: "duration must be positive"},
		{name: "negative duration", args: []string{"-1s"}, want: "duration must be positive"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := DrainMain(testutil.NewContext(t, false), tt.args...)
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestDrainMainWaitsForDuration(t *testing.T) {
	const duration = 5 * time.Millisecond
	started := time.Now()

	require.NoError(t, DrainMain(testutil.NewContext(t, false), duration.String()))
	require.GreaterOrEqual(t, time.Since(started), duration)
}

func TestDrainMainStopsWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(testutil.NewContext(t, false))
	cancel()

	started := time.Now()
	require.ErrorIs(t, DrainMain(ctx, "1h"), context.Canceled)
	require.Less(t, time.Since(started), time.Second)
}

func TestDrainMainStopsWhenContextDeadlineExpires(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.NewContext(t, false), 5*time.Millisecond)
	defer cancel()

	started := time.Now()
	require.ErrorIs(t, DrainMain(ctx, "1h"), context.DeadlineExceeded)
	require.Less(t, time.Since(started), time.Second)
}
