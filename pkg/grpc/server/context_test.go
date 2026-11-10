package server

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestCombinedContextDoneAndImmediateError(t *testing.T) {
	for _, secondary := range []bool{false, true} {
		t.Run(map[bool]string{false: "primary", true: "secondary"}[secondary], func(t *testing.T) {
			a, cancelA := context.WithCancel(t.Context())
			defer cancelA()
			b, cancelB := context.WithCancel(t.Context())
			defer cancelB()
			c := NewCombinedContext(a, b)
			done := c.Done()
			if done != c.Done() || c.Err() != nil {
				t.Fatal("uncanceled context must return a stable Done channel and no error")
			}
			if secondary {
				cancelB()
			} else {
				cancelA()
			}
			if c.Err() != context.Canceled {
				t.Fatalf("error immediately after cancellation: %v", c.Err())
			}
			select {
			case <-done:
			default:
				t.Fatal("Done was not closed after Err observed cancellation")
			}
			if done != c.Done() {
				t.Fatal("Done changed after cancellation")
			}
		})
	}
	b, cancel := context.WithCancel(t.Context())
	cancel()
	c := NewCombinedContext(t.Context(), b)
	if c.Err() != context.Canceled {
		t.Fatalf("already-canceled secondary error: %v", c.Err())
	}
}

func TestCombinedContextDeadlineAndCause(t *testing.T) {
	manual, cancelManual := context.WithCancelCause(t.Context())
	defer cancelManual(nil)
	secondary, cancelSecondary := context.WithCancelCause(t.Context())
	defer cancelSecondary(nil)
	manualCombined := NewCombinedContext(manual, secondary)
	manualChild, cancelChild := context.WithCancel(manualCombined)
	defer cancelChild()
	cancelManual(context.DeadlineExceeded)
	if manualCombined.Err() != context.Canceled || context.Cause(manualCombined) != context.DeadlineExceeded {
		t.Fatalf("manual cancel with deadline cause: %v / %v", manualCombined.Err(), context.Cause(manualCombined))
	}
	<-manualChild.Done()
	if manualChild.Err() != context.Canceled || context.Cause(manualChild) != context.DeadlineExceeded {
		t.Fatalf("derived manual cause: %v / %v", manualChild.Err(), context.Cause(manualChild))
	}
	cancelSecondary(errors.New("secondary must not replace first cancellation"))
	if manualCombined.Err() != context.Canceled || context.Cause(manualCombined) != context.DeadlineExceeded {
		t.Fatal("secondary replaced the first cancellation")
	}

	for _, secondaryFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "primary_deadline", true: "secondary_deadline"}[secondaryFirst], func(t *testing.T) {
			cause := errors.New("configured deadline cause")
			deadline := time.Now().Add(20 * time.Millisecond)
			short, cancelShort := context.WithDeadlineCause(t.Context(), deadline, cause)
			defer cancelShort()
			long, cancelLong := context.WithTimeout(t.Context(), time.Second)
			defer cancelLong()
			a, b := short, long
			if secondaryFirst {
				a, b = b, a
			}
			c := NewCombinedContext(a, b)
			if got, ok := c.Deadline(); !ok || !got.Equal(deadline) {
				t.Fatalf("earlier deadline: %v / %t", got, ok)
			}
			child, cancelDerived := context.WithCancel(c)
			defer cancelDerived()
			<-c.Done()
			<-child.Done()
			if c.Err() != context.DeadlineExceeded || context.Cause(c) != cause {
				t.Fatalf("merged deadline cause: %v / %v", c.Err(), context.Cause(c))
			}
			if child.Err() != context.DeadlineExceeded || context.Cause(child) != cause {
				t.Fatalf("derived deadline cause: %v / %v", child.Err(), context.Cause(child))
			}
		})
	}
}

func TestCombinedContextImmediateCauseWithoutEarlierReads(t *testing.T) {
	for _, secondary := range []bool{false, true} {
		t.Run(map[bool]string{false: "primary", true: "secondary"}[secondary], func(t *testing.T) {
			a, cancelA := context.WithCancelCause(t.Context())
			defer cancelA(nil)
			b, cancelB := context.WithCancelCause(t.Context())
			defer cancelB(nil)
			c := NewCombinedContext(a, b)
			cause := errors.New("custom cancellation cause")
			if secondary {
				cancelB(cause)
			} else {
				cancelA(cause)
			}
			if got := context.Cause(c); got != cause {
				t.Fatalf("first merged read returned cause %v; want %v", got, cause)
			}
		})
	}
}

func TestCombinedContextValues(t *testing.T) {
	type key int
	a := context.WithValue(t.Context(), key(1), "primary")
	b := context.WithValue(context.WithValue(t.Context(), key(1), "secondary"), key(2), "fallback")
	c := NewCombinedContext(a, b)
	if c.Value(key(1)) != "primary" || c.Value(key(2)) != "fallback" || c.Value(key(3)) != nil {
		t.Fatal("incorrect context value precedence")
	}
}

func TestCombinedContextConcurrentReaders(t *testing.T) {
	a, cancelA := context.WithCancel(t.Context())
	defer cancelA()
	b, cancelB := context.WithCancel(t.Context())
	defer cancelB()
	c := NewCombinedContext(a, b)
	done := c.Done()
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			for range 25 {
				if c.Done() != done {
					t.Error("Done changed between readers")
				}
				_ = c.Err()
				_, _ = c.Deadline()
			}
			<-done
			if c.Err() != context.Canceled {
				t.Errorf("cancellation: %v", c.Err())
			}
		})
	}
	cancelB()
	wg.Wait()
}
