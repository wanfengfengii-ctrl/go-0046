// Package clock provides an injectable time abstraction.
//
// Production code uses Real, which delegates to the wall clock. Tests use
// Manual, a fully controllable clock whose Now reports a value set explicitly
// and whose After channels fire deterministically when time is advanced.
//
// The abstraction is deliberately minimal: only Now and After are required.
// Deadline semantics in the executor are implemented by sampling Now against a
// recorded deadline and by respecting caller-provided contexts, so the clock
// itself does not need to mint contexts.
package clock

import (
	"sync"
	"time"
)

// Clock is the injectable time source used across the engine.
type Clock interface {
	// Now returns the current instant.
	Now() time.Time
	// After returns a channel that receives the current time once d has
	// elapsed. A non-positive d fires immediately. Callers must not assume
	// the received value equals time.Now(); under Manual it is the instant
	// the clock was advanced to.
	After(d time.Duration) <-chan time.Time
}

// Real is the wall-clock implementation.
type Real struct{}

// Now reports the wall-clock time.
func (Real) Now() time.Time { return time.Now() }

// After delegates to time.After.
func (Real) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Manual is a controllable clock for deterministic tests. It is safe for
// concurrent use. Advancing the clock fires every previously registered After
// whose deadline is at or before the new instant, in deadline order.
type Manual struct {
	mu      sync.Mutex
	now     time.Time
	waiters []manualWaiter
}

type manualWaiter struct {
	deadline time.Time
	ch       chan time.Time
}

// NewManual returns a Manual clock whose initial time is t.
func NewManual(t time.Time) *Manual {
	return &Manual{now: t}
}

// Now reports the clock's current instant.
func (m *Manual) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

// After registers a timer that fires when the clock advances past d. A
// non-positive d fires immediately with the current instant.
func (m *Manual) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	m.mu.Lock()
	if d <= 0 {
		ch <- m.now
		m.mu.Unlock()
		return ch
	}
	deadline := m.now.Add(d)
	m.waiters = insertWaiter(m.waiters, manualWaiter{deadline: deadline, ch: ch})
	m.mu.Unlock()
	return ch
}

// Advance moves the clock forward to to, which must be after the current time.
// It fires every registered waiter whose deadline is at or before to. Waiters
// are fired outside the lock to avoid blocking Advance on a slow receiver; each
// channel is buffered with capacity one so the send never blocks.
func (m *Manual) Advance(to time.Time) {
	m.mu.Lock()
	if !to.After(m.now) {
		m.mu.Unlock()
		return
	}
	m.now = to
	// Build keep/fired into fresh slices: reusing the waiters backing array for
	// both would alias and corrupt the iteration.
	var keep, fired []manualWaiter
	for _, w := range m.waiters {
		if w.deadline.After(to) {
			keep = append(keep, w)
		} else {
			fired = append(fired, w)
		}
	}
	m.waiters = keep
	m.mu.Unlock()
	for _, w := range fired {
		w.ch <- to
	}
}

// AdvanceBy advances the clock by d.
func (m *Manual) AdvanceBy(d time.Duration) {
	m.Advance(m.Now().Add(d))
}

// insertWaiter keeps waiters sorted by deadline ascending. Sorting is O(n) but
// the expected number of outstanding waiters in tests is small.
func insertWaiter(waiters []manualWaiter, w manualWaiter) []manualWaiter {
	i := 0
	for i < len(waiters) {
		if w.deadline.Before(waiters[i].deadline) {
			break
		}
		i++
	}
	waiters = append(waiters, manualWaiter{})
	copy(waiters[i+1:], waiters[i:])
	waiters[i] = w
	return waiters
}
