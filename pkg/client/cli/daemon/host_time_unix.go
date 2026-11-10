//go:build !windows

package daemon

import (
	"time"

	"golang.org/x/sys/unix"
)

func touchHostDescriptor(fd uintptr, now time.Time) error {
	tv := unix.NsecToTimeval(now.UnixNano())
	return unix.Futimes(int(fd), []unix.Timeval{tv, tv})
}
