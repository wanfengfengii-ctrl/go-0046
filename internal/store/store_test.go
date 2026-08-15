package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/benzhi/auction-pacing-reservation-engine/internal/config"
	"github.com/benzhi/auction-pacing-reservation-engine/internal/domain"
)

func testConfig() *config.Config {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return &config.Config{
		Version:             1,
		ShardCount:          4,
		QueueCap:            16,
		ReservationTTL:      config.Duration(30 * time.Second),
		CheckpointThreshold: 5,
		Campaigns: []config.Campaign{{
			ID:          "c1",
			LimitMicros: 1_000_000,
			BurstMicros: 0,
			Periods: []config.Period{{
				ID:          "p1",
				Start:       t0,
				End:         t0.Add(time.Hour),
				LimitMicros: 1_000_000,
			}},
			Channels: []config.Channel{{ID: "ch1", LimitMicros: 1_000_000}},
		}},
	}
}

func reserveOp(seq int64, id string, amount int64, now time.Time) *domain.Op {
	return &domain.Op{
		Type:          domain.OpReserve,
		Seq:           seq,
		ReservationID:  id,
		CampaignID:    "c1",
		PeriodID:      "p1",
		Channel:       "ch1",
		Amount:        amount,
		Now:           now,
		ExpiresAt:     now.Add(time.Minute),
		Digest:        [32]byte{byte(seq)},
		Result: &domain.OpResult{
			OK:            true,
			Code:          domain.CodeOK,
			ReservationID: id,
			Amount:        amount,
			State:         domain.StateReserved,
		},
	}
}

func TestMemoryApplyCommitsAndReplays(t *testing.T) {
	s := NewMemory(BuildState(testConfig()))
	ctx := context.Background()
	now := time.Now()
	op := reserveOp(0, "r1", 100, now)
	op.RequestID = "rq1"
	op.Digest = domain.ReserveInput{RequestID: "rq1", CampaignID: "c1", Channel: "ch1", Amount: 100}.Digest()
	seq, res, err := s.Apply(ctx, func(st *domain.State) (*Action, error) {
		if rec := st.IdemRecord("reserve:rq1"); rec != nil {
			return NoOpAction(rec.Result), nil
		}
		op.PrevHash = st.LastHash
		return CommitAction(op), nil
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if seq != 1 || res.ReservationID != "r1" {
		t.Fatalf("result = %+v", res)
	}
	// Idempotent replay returns the same result without a new commit.
	seq2, res2, err := s.Apply(ctx, func(st *domain.State) (*Action, error) {
		rec := st.IdemRecord("reserve:rq1")
		if rec == nil {
			return nil, errors.New("missing record")
		}
		// Same digest → replay.
		return NoOpAction(rec.Result), nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if seq2 != 0 || res2.ReservationID != "r1" {
		t.Fatalf("replay result = %+v", res2)
	}
	if got := s.LastSeq(); got != 1 {
		t.Fatalf("lastseq = %d, want 1", got)
	}
}

func TestFaultBeforeCommitLeavesStateUnchanged(t *testing.T) {
	mem := NewMemory(BuildState(testConfig()))
	fs := NewFault(mem)
	ctx := context.Background()
	now := time.Now()
	fs.Arm(FaultBeforeCommit)
	op := reserveOp(0, "r1", 100, now)
	_, _, err := fs.Apply(ctx, func(st *domain.State) (*Action, error) {
		return CommitAction(op), nil
	})
	if err == nil {
		t.Fatal("expected injected failure")
	}
	if !domain.IsDomainError(err, domain.CodeInternal) {
		t.Fatalf("expected internal injected error, got %v", err)
	}
	// No reservation should exist.
	if _, err := mem.GetReservation(ctx, "r1"); err == nil {
		t.Fatal("reservation was created despite before-commit fault")
	}
	if got := mem.LastSeq(); got != 0 {
		t.Fatalf("lastseq = %d, want 0", got)
	}
}

func TestFaultAfterCommitLoseResponseRecoverable(t *testing.T) {
	mem := NewMemory(BuildState(testConfig()))
	fs := NewFault(mem)
	ctx := context.Background()
	now := time.Now()
	rid := "r1"
	rqid := "rq1"
	op := reserveOp(0, rid, 100, now)
	op.RequestID = rqid
	op.Digest = domain.ReserveInput{RequestID: rqid, CampaignID: "c1", Channel: "ch1", Amount: 100}.Digest()
	// First attempt: commit succeeds but response is lost.
	fs.Arm(FaultAfterCommitLoseResponse)
	_, _, err := fs.Apply(ctx, func(st *domain.State) (*Action, error) {
		if rec := st.IdemRecord("reserve:" + rqid); rec != nil {
			return NoOpAction(rec.Result), nil
		}
		return CommitAction(op), nil
	})
	if err == nil {
		t.Fatal("expected lost-response error")
	}
	// The op was committed despite the lost response.
	if got := mem.LastSeq(); got != 1 {
		t.Fatalf("lastseq = %d, want 1", got)
	}
	// Retry with the same id: returns the committed result without re-committing.
	seq2, res2, err := fs.Apply(ctx, func(st *domain.State) (*Action, error) {
		rec := st.IdemRecord("reserve:" + rqid)
		if rec == nil {
			return nil, errors.New("missing")
		}
		return NoOpAction(rec.Result), nil
	})
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if seq2 != 0 || res2.ReservationID != rid {
		t.Fatalf("retry result = %+v", res2)
	}
	// No duplicate commit.
	if got := mem.LastSeq(); got != 1 {
		t.Fatalf("lastseq after retry = %d, want 1", got)
	}
}

func TestFaultCheckpointFailureLeavesOldIntact(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	f, err := OpenFile(dir, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	now := time.Now()
	op := reserveOp(0, "r1", 100, now)
	if _, _, err := f.Apply(ctx, func(st *domain.State) (*Action, error) {
		return CommitAction(op), nil
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	// Successful checkpoint.
	if _, err := f.Checkpoint(ctx); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	seqBefore := f.LastSeq()
	// Add another op, then fail the next checkpoint.
	op2 := reserveOp(0, "r2", 100, now)
	if _, _, err := f.Apply(ctx, func(st *domain.State) (*Action, error) {
		return CommitAction(op2), nil
	}); err != nil {
		t.Fatalf("apply2: %v", err)
	}
	// Wrap with fault for checkpoint failure.
	fs := NewFault(f)
	fs.Arm(FaultCheckpoint)
	if _, err := fs.Checkpoint(ctx); err == nil {
		t.Fatal("expected checkpoint failure")
	}
	if err := fs.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Reopen: the old checkpoint (at seq 1) plus the log (seq 2) must recover.
	f2, err := OpenFile(dir, cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer f2.Close()
	if got := f2.LastSeq(); got != seqBefore+1 {
		t.Fatalf("recovered lastseq = %d, want %d", got, seqBefore+1)
	}
	r1, _ := f2.GetReservation(ctx, "r1")
	r2, _ := f2.GetReservation(ctx, "r2")
	if r1 == nil || r2 == nil {
		t.Fatalf("reservations not recovered: r1=%+v r2=%+v", r1, r2)
	}
}

func TestFileRecoveryMatchesLive(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	f, err := OpenFile(dir, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	now := time.Now()
	// Reserve, confirm, refund.
	op1 := reserveOp(1, "r1", 100, now)
	op1.Digest = [32]byte{1}
	applyDirect(t, f, op1)
	op2 := &domain.Op{Type: domain.OpConfirm, Seq: 2, NotifID: "n1", ReservationID: "r1",
		ConfirmAmount: 80, Now: now, Digest: [32]byte{2},
		Result: &domain.OpResult{OK: true, Code: domain.CodeOK, State: domain.StateConfirmed, ConfirmedAmount: 80}}
	applyDirect(t, f, op2)
	op3 := &domain.Op{Type: domain.OpRefund, Seq: 3, NotifID: "rf1", ReservationID: "r1",
		RefundAmount: 30, Now: now, Digest: [32]byte{3},
		Result: &domain.OpResult{OK: true, Code: domain.CodeOK}}
	applyDirect(t, f, op3)
	liveSnap, _ := f.Snapshot(ctx)
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Reopen and compare.
	f2, err := OpenFile(dir, cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer f2.Close()
	recoveredSnap, _ := f2.Snapshot(ctx)
	if liveSnap.LastSeq != recoveredSnap.LastSeq {
		t.Fatalf("lastseq live=%d recovered=%d", liveSnap.LastSeq, recoveredSnap.LastSeq)
	}
	if liveSnap.LastHash != recoveredSnap.LastHash {
		t.Fatalf("lasthash mismatch")
	}
	clLive := liveSnap.CampaignLedger("c1")
	clRec := recoveredSnap.CampaignLedger("c1")
	if clLive != clRec {
		// Compare field by field.
		if clLive.Pending != clRec.Pending || clLive.Confirmed != clRec.Confirmed ||
			clLive.Refunded != clRec.Refunded || clLive.Available() != clRec.Available() {
			t.Fatalf("ledger mismatch: live=%+v recovered=%+v", clLive, clRec)
		}
	}
	rLive := liveSnap.Reservation("r1")
	rRec := recoveredSnap.Reservation("r1")
	if rLive.State != rRec.State || rLive.ConfirmedAmount != rRec.ConfirmedAmount ||
		rLive.RefundedAmount != rRec.RefundedAmount {
		t.Fatalf("reservation mismatch: live=%+v recovered=%+v", rLive, rRec)
	}
}

func TestCorruptLogFailsStartup(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	f, err := OpenFile(dir, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	now := time.Now()
	op := reserveOp(1, "r1", 100, now)
	op.Digest = [32]byte{1}
	applyDirect(t, f, op)
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Corrupt a byte in the middle of the payload (after the 4-byte length
	// header). The frame is 4 + payload + 32 bytes; flip payload byte 5.
	if err := corruptFileByte(filepath.Join(dir, oplogFile), 9); err != nil {
		t.Fatalf("corrupt: %v", err)
	}
	_ = ctx // ctx retained for readability of the lifecycle steps above
	if _, err := OpenFile(dir, cfg); err == nil {
		t.Fatal("expected startup failure on corrupted log")
	}
}

func TestCheckpointThenRestartResumes(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	f, err := OpenFile(dir, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	now := time.Now()
	for i := 0; i < 3; i++ {
		op := reserveOp(int64(i+1), "r"+string(rune('1'+i)), 100, now)
		op.Digest = [32]byte{byte(i + 1)}
		applyDirect(t, f, op)
	}
	if _, err := f.Checkpoint(ctx); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	// Add more ops after the checkpoint.
	for i := 0; i < 2; i++ {
		op := reserveOp(int64(i+4), "r"+string(rune('4'+i)), 100, now)
		op.Digest = [32]byte{byte(i + 4)}
		applyDirect(t, f, op)
	}
	liveSeq := f.LastSeq()
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	f2, err := OpenFile(dir, cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer f2.Close()
	if got := f2.LastSeq(); got != liveSeq {
		t.Fatalf("lastseq = %d, want %d", got, liveSeq)
	}
	for i := 0; i < 5; i++ {
		id := "r" + string(rune('1'+i))
		r, _ := f2.GetReservation(ctx, id)
		if r == nil {
			t.Fatalf("reservation %s not recovered", id)
		}
	}
}

func TestConfigVersionMismatchFails(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	f, err := OpenFile(dir, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	now := time.Now()
	op := reserveOp(1, "r1", 100, now)
	applyDirect(t, f, op)
	if _, err := f.Checkpoint(ctx); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	cfg2 := testConfig()
	cfg2.Version = 2
	if _, err := OpenFile(dir, cfg2); err == nil {
		t.Fatal("expected version-mismatch failure")
	}
}

func applyDirect(t *testing.T, s Store, op *domain.Op) {
	t.Helper()
	_, _, err := s.Apply(context.Background(), func(st *domain.State) (*Action, error) {
		op.PrevHash = st.LastHash
		return CommitAction(op), nil
	})
	if err != nil {
		t.Fatalf("apply op %s: %v", op.ReservationID, err)
	}
}

// commitOp returns a decision function that commits op (using the store's
// PrevHash) without any idempotency/replay logic. Used by the fault tests.
func commitOp(op *domain.Op) ApplyFn {
	return func(st *domain.State) (*Action, error) {
		op.PrevHash = st.LastHash
		return CommitAction(op), nil
	}
}

// TestFaultCheckpointSurvivesInterveningApply covers the headline bug: a
// FaultCheckpoint armed before a normal Apply must not be cleared by that
// Apply, so the next Checkpoint still fails.
func TestFaultCheckpointSurvivesInterveningApply(t *testing.T) {
	mem := NewMemory(BuildState(testConfig()))
	fs := NewFault(mem)
	ctx := context.Background()
	now := time.Now()

	fs.Arm(FaultCheckpoint)
	// A normal Apply must NOT consume the checkpoint fault.
	if _, _, err := fs.Apply(ctx, commitOp(reserveOp(0, "r1", 100, now))); err != nil {
		t.Fatalf("apply should succeed: %v", err)
	}
	if got := mem.LastSeq(); got != 1 {
		t.Fatalf("lastseq = %d, want 1 (apply should have committed)", got)
	}
	// The checkpoint fault must still fire on the next Checkpoint.
	if _, err := fs.Checkpoint(ctx); err == nil {
		t.Fatal("expected checkpoint failure after intervening apply")
	} else if !domain.IsDomainError(err, domain.CodeInternal) {
		t.Fatalf("expected internal injected error, got %v", err)
	}
}

// TestFaultBeforeCommitSurvivesInterveningCheckpoint verifies an Apply-type
// fault is not swallowed by an unrelated Checkpoint call.
func TestFaultBeforeCommitSurvivesInterveningCheckpoint(t *testing.T) {
	mem := NewMemory(BuildState(testConfig()))
	fs := NewFault(mem)
	ctx := context.Background()
	now := time.Now()

	fs.Arm(FaultBeforeCommit)
	// An unrelated Checkpoint must NOT consume the before-commit fault.
	if _, err := fs.Checkpoint(ctx); err != nil {
		t.Fatalf("checkpoint should succeed: %v", err)
	}
	// The before-commit fault must still fire on the next Apply.
	_, _, err := fs.Apply(ctx, commitOp(reserveOp(0, "r1", 100, now)))
	if err == nil {
		t.Fatal("expected before-commit failure after intervening checkpoint")
	}
	if !domain.IsDomainError(err, domain.CodeInternal) {
		t.Fatalf("expected internal injected error, got %v", err)
	}
	// Before-commit semantics: no state mutation occurred.
	if got := mem.LastSeq(); got != 0 {
		t.Fatalf("lastseq = %d, want 0", got)
	}
}

// TestFaultAfterCommitLoseResponseSurvivesInterveningCheckpoint verifies the
// second Apply-type fault is also not swallowed by an unrelated Checkpoint.
func TestFaultAfterCommitLoseResponseSurvivesInterveningCheckpoint(t *testing.T) {
	mem := NewMemory(BuildState(testConfig()))
	fs := NewFault(mem)
	ctx := context.Background()
	now := time.Now()
	rid := "r1"
	rqid := "rq1"
	op := reserveOp(0, rid, 100, now)
	op.RequestID = rqid
	op.Digest = domain.ReserveInput{RequestID: rqid, CampaignID: "c1", Channel: "ch1", Amount: 100}.Digest()

	fs.Arm(FaultAfterCommitLoseResponse)
	// An unrelated Checkpoint must NOT consume the lose-response fault.
	if _, err := fs.Checkpoint(ctx); err != nil {
		t.Fatalf("checkpoint should succeed: %v", err)
	}
	// The lose-response fault must still fire on the next Apply.
	_, _, err := fs.Apply(ctx, func(st *domain.State) (*Action, error) {
		if rec := st.IdemRecord("reserve:" + rqid); rec != nil {
			return NoOpAction(rec.Result), nil
		}
		return CommitAction(op), nil
	})
	if err == nil {
		t.Fatal("expected lost-response error after intervening checkpoint")
	}
	// The op was committed despite the lost response.
	if got := mem.LastSeq(); got != 1 {
		t.Fatalf("lastseq = %d, want 1", got)
	}
}

// TestFaultCheckpointFiresOnce verifies one-shot semantics for the checkpoint
// fault: it fails the first matching Checkpoint and lets the second succeed.
func TestFaultCheckpointFiresOnce(t *testing.T) {
	mem := NewMemory(BuildState(testConfig()))
	fs := NewFault(mem)
	ctx := context.Background()

	fs.Arm(FaultCheckpoint)
	if _, err := fs.Checkpoint(ctx); err == nil {
		t.Fatal("expected first checkpoint failure")
	}
	// Second checkpoint must succeed: the fault fires exactly once.
	if _, err := fs.Checkpoint(ctx); err != nil {
		t.Fatalf("second checkpoint should succeed: %v", err)
	}
}

// TestFaultBeforeCommitFiresOnce verifies one-shot semantics for an Apply-type
// fault: it fails the first Apply and lets the second succeed.
func TestFaultBeforeCommitFiresOnce(t *testing.T) {
	mem := NewMemory(BuildState(testConfig()))
	fs := NewFault(mem)
	ctx := context.Background()
	now := time.Now()

	fs.Arm(FaultBeforeCommit)
	if _, _, err := fs.Apply(ctx, commitOp(reserveOp(0, "r1", 100, now))); err == nil {
		t.Fatal("expected first apply failure")
	}
	// Second apply must succeed: the fault fires exactly once.
	if _, _, err := fs.Apply(ctx, commitOp(reserveOp(0, "r2", 100, now))); err != nil {
		t.Fatalf("second apply should succeed: %v", err)
	}
	if got := mem.LastSeq(); got != 1 {
		t.Fatalf("lastseq = %d, want 1", got)
	}
}

// TestFaultApplyAndCheckpointIndependent arms both fault types at once and
// verifies each fires only on its matching operation, leaving the other armed,
// and that both are spent after their respective operations.
func TestFaultApplyAndCheckpointIndependent(t *testing.T) {
	mem := NewMemory(BuildState(testConfig()))
	fs := NewFault(mem)
	ctx := context.Background()
	now := time.Now()

	// Arm both an Apply-type fault and a Checkpoint-type fault.
	fs.Arm(FaultBeforeCommit)
	fs.Arm(FaultCheckpoint)

	// Apply must fire the before-commit fault and leave the checkpoint armed.
	if _, _, err := fs.Apply(ctx, commitOp(reserveOp(0, "r1", 100, now))); err == nil {
		t.Fatal("expected before-commit failure on apply")
	}
	// Checkpoint must still fire (not consumed by the apply above).
	if _, err := fs.Checkpoint(ctx); err == nil {
		t.Fatal("expected checkpoint failure after apply consumed apply-fault")
	}
	// Both faults are now spent: a second apply and checkpoint must succeed.
	if _, _, err := fs.Apply(ctx, commitOp(reserveOp(0, "r2", 100, now))); err != nil {
		t.Fatalf("second apply should succeed: %v", err)
	}
	if _, err := fs.Checkpoint(ctx); err != nil {
		t.Fatalf("second checkpoint should succeed: %v", err)
	}
	if got := mem.LastSeq(); got != 1 {
		t.Fatalf("lastseq = %d, want 1", got)
	}
}

// TestArmNoneDisarmsPendingFaults verifies Arm(FaultNone) clears pending
// faults of both types so neither fires on the next matching operation.
func TestArmNoneDisarmsPendingFaults(t *testing.T) {
	mem := NewMemory(BuildState(testConfig()))
	fs := NewFault(mem)
	ctx := context.Background()
	now := time.Now()

	fs.Arm(FaultBeforeCommit)
	fs.Arm(FaultCheckpoint)
	fs.Arm(FaultNone) // disarm both pending faults.

	if _, _, err := fs.Apply(ctx, commitOp(reserveOp(0, "r1", 100, now))); err != nil {
		t.Fatalf("apply should succeed after disarm: %v", err)
	}
	if _, err := fs.Checkpoint(ctx); err != nil {
		t.Fatalf("checkpoint should succeed after disarm: %v", err)
	}
}
