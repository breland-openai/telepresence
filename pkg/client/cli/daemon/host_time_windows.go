package daemon

import (
	"time"

	"golang.org/x/sys/windows"
)

func touchHostDescriptor(fd uintptr, now time.Time) error {
	ft := windows.NsecToFiletime(now.UnixNano())
	return windows.SetFileTime(windows.Handle(fd), nil, &ft, &ft)
}
