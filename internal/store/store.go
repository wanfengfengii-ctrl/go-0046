// Package store defines the transactional persistence boundary for the pacing
// engine and provides three implementations:
//
//   - Memory: an in-memory store suitable for unit tests and ephemeral runs.
//   - File: a durable, pure-Go embedded store using an append-only operation
//     log plus atomic checkpoints, supporting crash recovery.
//   - Fault: a wrapper that injects deterministic failures for testing.
//
// All implementations share the same state machine (domain.State.Apply), so
// live and replayed state are identical by construction. A single Apply call
// executes the caller's decision function under the store lock; the function
// reads the current state and returns either an op to commit or a no-op result
// to return. The store assigns the sequence number, appends to the log (for the
// file store), and applies the op atomically.
package store

import (
	"context"
	"errors"
	"sync"

	"github.com/benzhi/auction-pacing-reservation-engine/internal/config"
	"github.com/benzhi/auction-pacing-reservation-engine/internal/domain"
)

// Action is the outcome of a decision function: either commit an op or return
// a precomputed result without mutating state (used for idempotent replays and
// no-op settlements).
type Action struct {
	Commit *domain.Op
	Result *domain.OpResult
}

// CommitAction returns an action that commits op.
func CommitAction(op *domain.Op) *Action { return &Action{Commit: op} }

// NoOpAction returns an action that yields result without committing.
func NoOpAction(result *domain.OpResult) *Action { return &Action{Result: result} }

// ApplyFn is the decision function executed under the store lock. It receives a
// read-only view of the current state and returns the action to take or an
// error to abort. Because the lock is held, the read-decide-commit sequence is
// atomic with respect to other operations.
type ApplyFn func(s *domain.State) (*Action, error)

// Store is the persistence and state-access boundary.
type Store interface {
	// Apply runs fn under the store lock. If fn returns a commit action, the
	// store assigns a sequence number, checks the context for pre-commit
	// cancellation, appends to the log (durable stores), and applies the op to
	// the materialized state. If fn returns a no-op action, the result is
	// returned without mutation. The returned result is non-nil on success.
	Apply(ctx context.Context, fn ApplyFn) (seq int64, result *domain.OpResult, err error)

	// Snapshot returns a deep copy of the current state for read-only use.
	Snapshot(ctx context.Context) (*domain.State, error)

	// GetReservation returns a copy of a reservation by id, or
	// domain.ErrUnknownReservation (wrapped) if absent.
	GetReservation(ctx context.Context, id string) (*domain.Reservation, error)

	// GetIdempotency returns a copy of an idempotency record by key.
	GetIdempotency(ctx context.Context, key string) (*domain.IdemRecord, bool)

	// Checkpoint writes a durable snapshot (durable stores) and returns the
	// sequence number it covers.
	Checkpoint(ctx context.Context) (int64, error)

	// LastSeq returns the last committed sequence number.
	LastSeq() int64

	// OpLog returns a copy of the operation log entries after the given
	// sequence number, for the read-only ops endpoint.
	OpLog(ctx context.Context, after int64, limit int) ([]*domain.Op, error)

	// Close releases resources.
	Close() error
}

// baseStore holds the shared in-memory state and lock used by all
// implementations. Concrete stores add persistence around it.
type baseStore struct {
	mu    sync.Mutex
	state *domain.State
	log   []*domain.Op // in-memory mirror of committed ops (for ops endpoint)
}

func (b *baseStore) prepareAndApply(op *domain.Op) error {
	op.Seq = b.state.LastSeq + 1
	op.PrevHash = b.state.LastHash
	if op.Result != nil {
		op.Result.Seq = op.Seq
	}
	if err := b.state.Apply(op); err != nil {
		return err
	}
	b.state.LastSeq = op.Seq
	b.state.LastHash = domain.HashOp(op)
	b.log = append(b.log, op)
	return nil
}

func (b *baseStore) snapshotState() *domain.State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state.Clone()
}

func (b *baseStore) getReservation(id string) (*domain.Reservation, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	r := b.state.Reservation(id)
	if r == nil {
		return nil, domain.NewError(domain.CodeUnknownReservation, "reservation "+id)
	}
	cp := *r
	return &cp, nil
}

func (b *baseStore) getIdem(key string) (*domain.IdemRecord, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	r := b.state.IdemRecord(key)
	if r == nil {
		return nil, false
	}
	cp := *r
	if r.Result != nil {
		rc := *r.Result
		cp.Result = &rc
	}
	return &cp, true
}

func (b *baseStore) lastSeq() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state.LastSeq
}

func (b *baseStore) opLog(after int64, limit int) []*domain.Op {
	b.mu.Lock()
	defer b.mu.Unlock()
	if limit <= 0 {
		limit = len(b.log)
	}
	out := make([]*domain.Op, 0, limit)
	for _, op := range b.log {
		if op.Seq <= after {
			continue
		}
		if len(out) >= limit {
			break
		}
		cp := *op
		if op.Result != nil {
			rc := *op.Result
			cp.Result = &rc
		}
		out = append(out, &cp)
	}
	return out
}

// BuildState constructs a fresh materialized state from configuration, creating
// the three-tier ledger tree (campaign, period, channel) for every campaign.
func BuildState(cfg *config.Config) *domain.State {
	s := domain.NewState(cfg.Version)
	for _, cm := range cfg.Campaigns {
		s.AddLedger(domain.LevelCampaign, cm.ID, "", "", cm.LimitMicros)
		for _, p := range cm.Periods {
			s.AddLedger(domain.LevelPeriod, cm.ID, p.ID, "", p.LimitMicros)
			for _, ch := range cm.Channels {
				s.AddLedger(domain.LevelChannel, cm.ID, p.ID, ch.ID, ch.LimitMicros)
			}
		}
	}
	return s
}

// mapCtxError converts a context error into a stable domain error.
func mapCtxError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, context.Canceled):
		return domain.NewError(domain.CodeCanceled, "context canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return domain.NewError(domain.CodeDeadlineExceeded, "context deadline exceeded")
	default:
		return err
	}
}
