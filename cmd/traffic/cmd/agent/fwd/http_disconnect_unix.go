//go:build unix

package fwd

import (
	"errors"

	"golang.org/x/sys/unix"
)

func httpInterceptSocketConnectionLost(err error) bool {
	return errors.Is(err, unix.ECONNRESET) || errors.Is(err, unix.ECONNABORTED) || errors.Is(err, unix.EPIPE)
}
