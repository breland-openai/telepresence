package connect

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"regexp"
	"syscall" //nolint:depguard // ECONNREFUSED is used on all supported platforms.
	"time"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/telepresenceio/telepresence/rpc/v2/connector"
	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/daemon"
	"github.com/telepresenceio/telepresence/v2/pkg/errcat"
	"github.com/telepresenceio/telepresence/v2/pkg/shellquote"
)

type hostOwnership struct {
	host    *daemon.HostInfo
	desired string
	commit  bool
}

type hostOwnershipKey struct{}

func withHostOwnership(ctx context.Context, desired string, commit bool) (context.Context, error) {
	host, err := daemon.ReadHostInfo(ctx)
	if err != nil {
		return ctx, err
	}
	info := daemon.MustGetUserClient(ctx).DaemonInfo()
	if info.DaemonPort != host.Info.DaemonPort || info.HostID != host.Info.HostID {
		return ctx, errors.New("host daemon ownership changed while connecting; retry the connection")
	}
	return context.WithValue(ctx, hostOwnershipKey{}, &hostOwnership{host: host, desired: desired, commit: commit}), nil
}

func commitHostOwnership(ctx context.Context, ci *connector.ConnectInfo) error {
	owner, _ := ctx.Value(hostOwnershipKey{}).(*hostOwnership)
	if owner == nil || !owner.commit {
		return nil
	}
	if ci == nil || ci.GetSessionInfo() == nil || ci.GetManagerVersion() == nil || ci.GetConnectionName() != owner.desired {
		return errcat.User.Newf("host daemon did not confirm connection %q; its existing ownership was preserved", owner.desired)
	}
	if err := owner.host.Update(ctx, ci.GetConnectionName(), ci.GetClusterContext(), ci.GetNamespace()); err != nil {
		return errcat.NoDaemonLogs.New(err)
	}
	return nil
}

func hostConnectorOrMissing(ctx context.Context, id *daemon.Identifier, cr *daemon.Request) (context.Context, bool, error) {
	host, err := daemon.ReadHostInfo(ctx)
	if errors.Is(err, fs.ErrNotExist) {
		return ctx, true, nil
	}
	if err != nil {
		return ctx, false, errcat.NoDaemonLogs.Errorf(err, "unable to verify the existing host daemon; its entry was preserved")
	}
	if err = probeHostPort(ctx, host.Info.DaemonPort); err != nil {
		var stale bool
		stale, err = hostDefinitelyStale(ctx, host, err, daemon.HostInfoGrace)
		if !stale {
			return ctx, false, hostUncertain(host, err)
		}
		if err = host.Delete(ctx); err != nil {
			return ctx, false, hostUncertain(host, err)
		}
		return ctx, true, nil
	}
	quick, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	conn, err := daemon.DialHostInfo(quick, host)
	if err != nil {
		return ctx, false, hostUncertain(host, err)
	}
	rc, err := newUserDaemon(ctx, conn, host.Info)
	if err != nil {
		_ = conn.Close()
		return ctx, false, hostUncertain(host, err)
	}
	if !cr.Implicit {
		if err = verifyHostStatus(quick, daemon.MustGetUserClient(rc), host, id.Name); err != nil {
			_ = conn.Close()
			return ctx, false, err
		}
	}
	return context.WithValue(rc, hostOwnershipKey{}, &hostOwnership{host: host, desired: id.Name, commit: true}), false, nil
}

func verifyHostStatus(ctx context.Context, user daemon.UserClient, host *daemon.HostInfo, desired string) error {
	ci, err := user.Status(ctx, &emptypb.Empty{})
	if err != nil || ci == nil {
		return hostUncertain(host, err)
	}
	if ci.GetSessionInfo() != nil && ci.GetManagerVersion() != nil && ci.GetConnectionName() != "" {
		if ci.GetConnectionName() != desired {
			return errcat.User.Newf("host connection %q is already active; disconnect it with %s before connecting as %q",
				ci.GetConnectionName(), scopedHostQuit(ci.GetConnectionName()), desired)
		}
		return nil
	}
	root := ci.GetDaemonStatus()
	if ci.GetSessionInfo() != nil || ci.GetManagerVersion() != nil || root == nil || root.GetOutboundConfig() != nil {
		return hostUncertain(host, errors.New("the user daemon or shared root has an active or unconfirmed connection"))
	}
	return nil
}

func scopedHostQuit(name string) fmt.Stringer {
	return shellquote.ShellString("telepresence", []string{"quit", "--use", "^" + regexp.QuoteMeta(name) + "$"})
}

func hostUncertain(host *daemon.HostInfo, err error) error {
	if err == nil {
		err = errors.New("daemon status is incomplete")
	}
	return errcat.User.Errorf(err, "unable to verify host connection %q; its daemon was preserved. Retry or disconnect it with %s",
		host.Info.Name, scopedHostQuit(host.Info.Name))
}

func probeHostPort(ctx context.Context, port uint16) error {
	d := net.Dialer{Timeout: 500 * time.Millisecond}
	conn, err := d.DialContext(ctx, "tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err == nil {
		_ = conn.Close()
	}
	return err
}

func hostDefinitelyStale(ctx context.Context, host *daemon.HostInfo, initialErr error, grace time.Duration) (bool, error) {
	if !errors.Is(initialErr, syscall.ECONNREFUSED) || time.Since(host.Stat.ModTime()) <= grace {
		return false, initialErr
	}
	ticker := time.NewTicker(min(200*time.Millisecond, grace))
	defer ticker.Stop()
	timer := time.NewTimer(grace)
	defer timer.Stop()
	for {
		final := false
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-timer.C:
			final = true
		case <-ticker.C:
		}
		current, err := daemon.ReadHostInfo(ctx)
		if err != nil {
			return false, err
		}
		if !host.SameOwner(current) || !host.Stat.ModTime().Equal(current.Stat.ModTime()) {
			return false, errors.New("host daemon ownership or heartbeat changed")
		}
		if err = probeHostPort(ctx, host.Info.DaemonPort); !errors.Is(err, syscall.ECONNREFUSED) {
			return false, err
		}
		if final {
			return true, nil
		}
	}
}
