package fwd

import (
	"errors"

	"golang.org/x/sys/windows"
)

func httpInterceptSocketConnectionLost(err error) bool {
	return errors.Is(err, windows.WSAECONNRESET) || errors.Is(err, windows.WSAECONNABORTED) ||
		errors.Is(err, windows.WSAESHUTDOWN) || errors.Is(err, windows.ERROR_BROKEN_PIPE) || errors.Is(err, windows.ERROR_NO_DATA)
}
