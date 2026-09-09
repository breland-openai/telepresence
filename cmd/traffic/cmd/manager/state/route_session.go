package state

import (
	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
)

// CreatedByThisManager excludes sessions reconstructed from client snapshots.
func (cs *ClientSession) CreatedByThisManager() bool {
	return cs.createdHere
}

func (s *State) HasClientDurableIntercept(sessionID tunnel.SessionID) (found bool) {
	s.intercepts.Range(func(_ string, intercept *Intercept) bool {
		found = intercept.RouteIncarnation != "" && intercept.Disposition != rpc.InterceptDispositionType_REMOVED &&
			tunnel.SessionID(intercept.GetClientSession().GetSessionId()) == sessionID
		return !found
	})
	return found
}
