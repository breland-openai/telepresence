package fwd

import (
	"fmt"
	"testing"

	"golang.org/x/sys/windows"
)

func TestHTTPInterceptWindowsConnectionLost(t *testing.T) {
	for _, cause := range []error{
		windows.WSAECONNRESET, windows.WSAECONNABORTED, windows.WSAESHUTDOWN, windows.ERROR_BROKEN_PIPE, windows.ERROR_NO_DATA,
	} {
		if !httpInterceptSocketConnectionLost(fmt.Errorf("HTTP transport: %w", cause)) {
			t.Errorf("socket disconnect not classified: %v", cause)
		}
	}
	if httpInterceptSocketConnectionLost(windows.ERROR_INVALID_PARAMETER) {
		t.Error("unrelated system error classified as a disconnect")
	}
}
