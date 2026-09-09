//go:build unix

package fwd

import (
	"fmt"
	"testing"

	"golang.org/x/sys/unix"
)

func TestHTTPInterceptUnixConnectionLost(t *testing.T) {
	for _, cause := range []error{unix.ECONNRESET, unix.ECONNABORTED, unix.EPIPE} {
		if !httpInterceptSocketConnectionLost(fmt.Errorf("HTTP transport: %w", cause)) {
			t.Errorf("socket disconnect not classified: %v", cause)
		}
	}
	if httpInterceptSocketConnectionLost(unix.EINVAL) {
		t.Error("unrelated system error classified as a disconnect")
	}
}
