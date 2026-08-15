package store

import (
	"context"
	"sync/atomic"

	"github.com/benzhi/auction-pacing-reservation-engine/internal/domain"
)

// FaultMode selects where the next fault is injected.
type FaultMode int

const (
	FaultNone FaultMode = iota
	// FaultBeforeCommit fails the next Apply after the decision function
	// returns but before any state mutation or log write. State is unchanged.
	FaultBeforeCommit
	// FaultAfterCommitLoseResponse commits the next Apply (state + log) and
	// then returns a "lost response" error to the caller. The committed result
	// is recoverable via idempotent retry.
	FaultAfterCommitLoseResponse
	// FaultCheckpoint makes the next Checkpoint call fail; prior checkpoints
	// and the log remain intact.
	FaultCheckpoint
)

// Fault wraps a Store and injects deterministic failures. It is safe for
// concurrent use. Each injected fault fires exactly once.
type Fault struct {
	inner Store
	mode  atomic.Int32 // FaultMode
}

// NewFault wraps inner with a fault-injecting store.
func NewFault(inner Store) *Fault {
	return &Fault{inner: inner}
}

// Arm sets the fault to inject on the next matching operation.
func (f *Fault) Arm(mode FaultMode) {
	f.mode.Store(int32(mode))
}

// take returns and clears the armed mode if it matches one of modes.
func (f *Fault) take(modes ...FaultMode) FaultMode {
	for {
		armed := FaultMode(f.mode.Load())
		matched := false
		for _, mode := range modes {
			if armed == mode {
				matched = true
				break
			}
		}
		if !matched {
			return FaultNone
		}
		if f.mode.CompareAndSwap(int32(armed), int32(FaultNone)) {
			return armed
		}
	}
}

// Apply delegates to the inner store, injecting the armed fault.
func (f *Fault) Apply(ctx context.Context, fn ApplyFn) (int64, *domain.OpResult, error) {
	mode := f.take(FaultBeforeCommit, FaultAfterCommitLoseResponse)
	if mode == FaultBeforeCommit {
		// Run the decision function to observe its result, then fail before
		// any mutation. We snapshot to read state without committing.
		snap, err := f.inner.Snapshot(ctx)
		if err != nil {
			return 0, nil, err
		}
		if _, err := fn(snap); err != nil {
			return 0, nil, err
		}
		return 0, nil, domain.NewError(domain.CodeInternal, "injected: before-commit failure")
	}
	if mode == FaultAfterCommitLoseResponse {
		// Wrap fn so the inner store commits normally; then drop the response.
		var committed int64
		var result *domain.OpResult
		wrap := func(s *domain.State) (*Action, error) {
			a, err := fn(s)
			if err != nil {
				return nil, err
			}
			if a != nil && a.Commit != nil {
				committed = s.LastSeq + 1
				result = a.Commit.Result
			}
			return a, nil
		}
		_, _, err := f.inner.Apply(ctx, wrap)
		if err != nil {
			return 0, nil, err
		}
		_ = committed
		_ = result
		return 0, nil, domain.NewError(domain.CodeInternal, "injected: response lost after commit")
	}
	return f.inner.Apply(ctx, fn)
}

// Snapshot delegates to the inner store.
func (f *Fault) Snapshot(ctx context.Context) (*domain.State, error) {
	return f.inner.Snapshot(ctx)
}

// GetReservation delegates to the inner store.
func (f *Fault) GetReservation(ctx context.Context, id string) (*domain.Reservation, error) {
	return f.inner.GetReservation(ctx, id)
}

// GetIdempotency delegates to the inner store.
func (f *Fault) GetIdempotency(ctx context.Context, key string) (*domain.IdemRecord, bool) {
	return f.inner.GetIdempotency(ctx, key)
}

// Checkpoint delegates to the inner store, injecting FaultCheckpoint.
func (f *Fault) Checkpoint(ctx context.Context) (int64, error) {
	if f.take(FaultCheckpoint) == FaultCheckpoint {
		return 0, domain.NewError(domain.CodeInternal, "injected: checkpoint failure")
	}
	return f.inner.Checkpoint(ctx)
}

// LastSeq delegates to the inner store.
func (f *Fault) LastSeq() int64 { return f.inner.LastSeq() }

// OpLog delegates to the inner store.
func (f *Fault) OpLog(ctx context.Context, after int64, limit int) ([]*domain.Op, error) {
	return f.inner.OpLog(ctx, after, limit)
}

// Close delegates to the inner store.
func (f *Fault) Close() error { return f.inner.Close() }
