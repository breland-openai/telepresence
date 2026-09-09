package daemon

import (
	"context"
	"crypto/rand"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/telepresenceio/telepresence/v2/pkg/client/k8s"
)

type managerTokenCallback struct {
	file       string
	negotiated func(context.Context, string, string) (string, error)
}

// RegisterManagerTokenCallback binds a capability to an already-selected
// connection path. The path is only opened as this unprivileged daemon's user.
func (s *service) RegisterManagerTokenCallback(ctx context.Context, path string) ([]byte, func(), error) {
	if _, err := k8s.ReadManagerTokenFile(ctx, path); err != nil {
		return nil, nil, errors.New("unable to register the traffic-manager credential callback")
	}
	return s.registerManagerTokenCallback(managerTokenCallback{file: path})
}

// RegisterNegotiatedManagerTokenCallback binds a capability without resolving
// a credential before a manager advertises its supported Devbox audience.
func (s *service) RegisterNegotiatedManagerTokenCallback(provider func(context.Context, string, string) (string, error)) ([]byte, func(), error) {
	if provider == nil {
		return nil, nil, errors.New("unable to register the traffic-manager credential callback")
	}
	return s.registerManagerTokenCallback(managerTokenCallback{negotiated: provider})
}

func (s *service) registerManagerTokenCallback(callback managerTokenCallback) ([]byte, func(), error) {
	capability := make([]byte, k8s.ManagerTokenCallbackCapabilitySize)
	if _, err := rand.Read(capability); err != nil {
		return nil, nil, errors.New("unable to create the traffic-manager credential callback")
	}
	key := string(capability)
	s.managerTokenCallbackLock.Lock()
	if s.managerTokenCallbacks == nil {
		s.managerTokenCallbacks = make(map[string]managerTokenCallback)
	}
	s.managerTokenCallbacks[key] = callback
	s.managerTokenCallbackLock.Unlock()
	revoke := func() {
		s.managerTokenCallbackLock.Lock()
		delete(s.managerTokenCallbacks, key)
		s.managerTokenCallbackLock.Unlock()
	}
	return capability, revoke, nil
}

func (s *service) ManagerToken(ctx context.Context, capability []byte, managerPod, audience string) (string, error) {
	if len(capability) != k8s.ManagerTokenCallbackCapabilitySize {
		return "", status.Error(codes.PermissionDenied, "invalid traffic-manager credential callback")
	}
	s.managerTokenCallbackLock.Lock()
	callback, exists := s.managerTokenCallbacks[string(capability)]
	s.managerTokenCallbackLock.Unlock()
	if !exists {
		return "", status.Error(codes.PermissionDenied, "invalid traffic-manager credential callback")
	}
	var token string
	var err error
	if callback.negotiated != nil {
		if managerPod == "" || audience == "" {
			return "", status.Error(codes.PermissionDenied, "invalid negotiated traffic-manager credential callback")
		}
		token, err = callback.negotiated(ctx, managerPod, audience)
	} else {
		if managerPod != "" || audience != "" {
			return "", status.Error(codes.PermissionDenied, "invalid explicit traffic-manager credential callback")
		}
		token, err = k8s.ReadManagerTokenFile(ctx, callback.file)
	}
	if err != nil || token == "" {
		return "", status.Error(codes.Unauthenticated, "the traffic-manager credential is unavailable")
	}
	return token, nil
}
