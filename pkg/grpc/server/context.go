package server

import (
	"context"
	"sync"
	"time"
)

func NewCombinedContext(a, b context.Context) context.Context {
	causeContext, cancelCause := context.WithCancelCause(context.WithoutCancel(a))
	notification, notify := context.WithCancel(context.Background())
	c := &combinedContext{
		a: a, b: b, causeContext: causeContext, cancelCause: cancelCause,
		notification: notification, notify: notify,
	}
	c.syncParents()
	stopA := context.AfterFunc(a, func() { c.cancelFrom(a) })
	stopB := context.AfterFunc(b, func() { c.cancelFrom(b) })
	context.AfterFunc(notification, func() {
		stopA()
		stopB()
	})
	c.syncParents()
	return c
}

type combinedContext struct {
	a, b         context.Context
	causeContext context.Context
	cancelCause  context.CancelCauseFunc
	notification context.Context
	notify       context.CancelFunc
	mu           sync.Mutex
	err          error
}

func (c *combinedContext) Deadline() (time.Time, bool) {
	if dla, ok := c.a.Deadline(); ok {
		if dlb, ok := c.b.Deadline(); ok {
			if dlb.Before(dla) {
				return dlb, ok
			}
		}
		return dla, ok
	}
	return c.b.Deadline()
}

func (c *combinedContext) Done() <-chan struct{} {
	c.syncParents()
	return c.notification.Done()
}

func (c *combinedContext) Err() error {
	c.syncParents()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *combinedContext) Value(key any) any {
	c.syncParents()
	v := c.causeContext.Value(key)
	if v == nil {
		v = c.b.Value(key)
	}
	return v
}

// Distinct cause and notification contexts make derived contexts use AfterFunc
// and inherit the selected parent's cancellation error and cause.
func (c *combinedContext) AfterFunc(f func()) func() bool {
	c.syncParents()
	return context.AfterFunc(c.notification, f)
}

func (c *combinedContext) syncParents() {
	c.cancelFrom(c.a)
	c.cancelFrom(c.b)
}

func (c *combinedContext) cancelFrom(parent context.Context) {
	err := parent.Err()
	if err == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err == nil {
		c.err = err
		c.cancelCause(context.Cause(parent))
		c.notify()
	}
}
