package store

import (
	"context"

	"github.com/benzhi/auction-pacing-reservation-engine/internal/domain"
)

// Memory is an in-memory, non-durable Store. It is safe for concurrent use.
type Memory struct {
	baseStore
}

// NewMemory builds an in-memory store from a fresh state derived from cfg.
func NewMemory(state *domain.State) *Memory {
	return &Memory{baseStore: baseStore{state: state}}
}

// Apply runs fn under the store lock and commits the resulting op.
func (m *Memory) Apply(ctx context.Context, fn ApplyFn) (int64, *domain.OpResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	action, err := fn(m.state)
	if err != nil {
		return 0, nil, err
	}
	if action == nil {
		return 0, nil, domain.NewError(domain.CodeInternal, "nil action")
	}
	if action.Commit == nil {
		// No-op / replay: return cached result without mutating state.
		return 0, action.Result, nil
	}
	if err := ctx.Err(); err != nil {
		return 0, nil, mapCtxError(err)
	}
	if err := m.prepareAndApply(action.Commit); err != nil {
		return 0, nil, err
	}
	return action.Commit.Seq, action.Commit.Result, nil
}

// Snapshot returns a deep copy of the current state.
func (m *Memory) Snapshot(ctx context.Context) (*domain.State, error) {
	return m.snapshotState(), nil
}

// GetReservation returns a copy of a reservation.
func (m *Memory) GetReservation(ctx context.Context, id string) (*domain.Reservation, error) {
	return m.getReservation(id)
}

// GetIdempotency returns a copy of an idempotency record.
func (m *Memory) GetIdempotency(ctx context.Context, key string) (*domain.IdemRecord, bool) {
	return m.getIdem(key)
}

// Checkpoint is a no-op for the in-memory store; it returns the last seq.
func (m *Memory) Checkpoint(ctx context.Context) (int64, error) {
	return m.lastSeq(), nil
}

// LastSeq returns the last committed sequence number.
func (m *Memory) LastSeq() int64 { return m.lastSeq() }

// OpLog returns a copy of committed ops after the given sequence.
func (m *Memory) OpLog(ctx context.Context, after int64, limit int) ([]*domain.Op, error) {
	return m.opLog(after, limit), nil
}

// Close is a no-op.
func (m *Memory) Close() error { return nil }
