//go:build !unix && !windows

package fwd

func httpInterceptSocketConnectionLost(error) bool {
	return false
}
