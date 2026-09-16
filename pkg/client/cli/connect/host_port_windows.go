package connect

import (
	"errors"
	"time"

	"golang.org/x/sys/windows"
)

const hostPortProbeTimeout = 2 * time.Second

func hostPortConnectionRefused(err error) bool {
	return errors.Is(err, windows.WSAECONNREFUSED)
}
