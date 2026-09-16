//go:build !windows

package connect

import (
	"errors"
	"time"

	"golang.org/x/sys/unix"
)

const hostPortProbeTimeout = 500 * time.Millisecond

func hostPortConnectionRefused(err error) bool {
	return errors.Is(err, unix.ECONNREFUSED)
}
