package agent

import (
	"context"
	"fmt"
	"time"

	"github.com/telepresenceio/clog"
)

// DrainMain delays container termination while the existing agent process keeps
// serving its active intercepts. Kubernetes runs it as a separate preStop
// process, so it must not alter the agent's readiness or manager session.
func DrainMain(ctx context.Context, args ...string) error {
	if len(args) != 1 {
		return fmt.Errorf("agent-drain requires exactly one positive duration argument; got %d", len(args))
	}

	duration, err := time.ParseDuration(args[0])
	if err != nil {
		return fmt.Errorf("invalid agent-drain duration %q: %w", args[0], err)
	}
	if duration <= 0 {
		return fmt.Errorf("agent-drain duration must be positive; got %s", duration)
	}

	clog.Infof(ctx, "Delaying traffic-agent termination for %s while active intercepts drain", duration)
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-timer.C:
		clog.Infof(ctx, "Traffic-agent drain completed after %s", duration)
		return nil
	case <-ctx.Done():
		clog.Infof(ctx, "Traffic-agent drain canceled before %s elapsed: %v", duration, ctx.Err())
		return ctx.Err()
	}
}
