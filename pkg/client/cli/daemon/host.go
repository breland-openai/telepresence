package daemon

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/rogpeppe/go-internal/lockedfile"
	"google.golang.org/grpc"

	"github.com/telepresenceio/telepresence/v2/pkg/dos"
	"github.com/telepresenceio/telepresence/v2/pkg/filelocation"
)

const HostInfoGrace = maxNoSignOfLife

var errHostOwnershipChanged = errors.New("host daemon ownership changed")

type HostInfo struct {
	Info *Info
	Stat fs.FileInfo
}

type hostFile interface {
	dos.File
	Fd() uintptr
}

func ReadHostInfo(ctx context.Context) (*HostInfo, error) {
	path := filepath.Join(filelocation.AppUserCacheDir(ctx), daemonsDirName, InfoFileName)
	f, err := dos.OpenFile(dos.WithLockedFs(ctx), path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return readOpenHostInfo(f, path)
}

func readOpenHostInfo(f dos.File, path string) (*HostInfo, error) {
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	var info Info
	if err = json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("unable to parse host daemon info %s: %w", path, err)
	}
	if info.DaemonPort == 0 || info.InDocker() {
		return nil, fmt.Errorf("invalid host daemon info %s", path)
	}
	return &HostInfo{Info: &info, Stat: st}, nil
}

func touchHostInfo(ctx context.Context, host *HostInfo, touch func(hostFile) error) error {
	path := filepath.Join(filelocation.AppUserCacheDir(ctx), daemonsDirName, InfoFileName)
	f, err := lockedfile.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	current, err := readOpenHostInfo(f, path)
	if err != nil {
		return err
	}
	if !host.SameOwner(current) {
		return errHostOwnershipChanged
	}
	return touch(f)
}

func touchHostFile(f hostFile) error {
	return touchHostDescriptor(f.Fd(), time.Now())
}

func (h *HostInfo) SameOwner(other *HostInfo) bool {
	return h != nil && other != nil && os.SameFile(h.Stat, other.Stat) &&
		h.Info.HostID == other.Info.HostID && h.Info.DaemonPort == other.Info.DaemonPort
}

func (h *HostInfo) Update(ctx context.Context, name, kubeContext, namespace string) error {
	current, err := ReadHostInfo(ctx)
	if err != nil {
		return err
	}
	if !h.SameOwner(current) {
		return fmt.Errorf("%w while connecting; retry the connection", errHostOwnershipChanged)
	}
	if current.Info.Name != name || current.Info.KubeContext != kubeContext || current.Info.Namespace != namespace {
		current.Info.SetConnectionInfo(name, kubeContext, namespace)
		if err = NewUserInfoLoader(ctx).SaveInfo(current.Info, InfoFileName); err != nil {
			return err
		}
	}
	h.Info = current.Info
	return nil
}

func (h *HostInfo) Delete(ctx context.Context) error {
	current, err := ReadHostInfo(ctx)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !h.SameOwner(current) {
		return fmt.Errorf("%w; preserved the new entry", errHostOwnershipChanged)
	}
	return NewUserInfoLoader(ctx).DeleteInfo(InfoFileName)
}

func DialHostInfo(ctx context.Context, host *HostInfo) (*grpc.ClientConn, error) {
	return dialDaemon(ctx, "user", host.Info.DaemonPort)
}

// LockHost holds the same inode across host launches and connection commits.
func LockHost(ctx context.Context) (func(), error) {
	path := filepath.Join(filelocation.AppUserCacheDir(ctx), "host-daemon.lock")
	if err := dos.MkdirAll(ctx, filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	type result struct {
		f   dos.File
		err error
	}
	ready := make(chan result)
	go func() {
		f, err := dos.OpenFile(dos.WithLockedFs(ctx), path, os.O_CREATE|os.O_RDWR, 0o600)
		select {
		case ready <- result{f, err}:
		case <-ctx.Done():
			if f != nil {
				_ = f.Close()
			}
		}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-ready:
		if r.err != nil {
			return nil, r.err
		}
		return func() { _ = r.f.Close() }, nil
	}
}

func KeepHostInfoAlive(ctx context.Context, port uint16, hostID string, cancel context.CancelFunc) error {
	ticker := time.NewTicker(keepAliveInterval)
	defer ticker.Stop()
	var host *HostInfo
	var err error
	for {
		host, err = ReadHostInfo(ctx)
		if err == nil {
			break
		}
		if errors.Is(err, fs.ErrNotExist) {
			if hostID != "" {
				cancel()
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
	if host.Info.DaemonPort != port || (hostID != "" && host.Info.HostID != hostID) {
		cancel()
		return nil
	}
	for {
		if err = touchHostInfo(ctx, host, touchHostFile); errors.Is(err, fs.ErrNotExist) || errors.Is(err, errHostOwnershipChanged) {
			cancel()
			return nil
		}
		select {
		case <-ctx.Done():
			cleanup := context.WithoutCancel(ctx)
			unlock, lockErr := LockHost(cleanup)
			if lockErr != nil {
				return lockErr
			}
			defer unlock()
			_ = host.Delete(cleanup)
			return nil
		case <-ticker.C:
		}
	}
}
