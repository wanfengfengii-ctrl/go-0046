package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
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

// corruptCheckpointJSON rewrites the checkpoint file at dir by applying mutate
// to its raw JSON bytes. It fails the test if mutate does not change the bytes,
// so a test cannot silently pass against an uncorrupted file.
func corruptCheckpointJSON(t *testing.T, dir string, mutate func([]byte) []byte) {
	t.Helper()
	path := filepath.Join(dir, checkpointFile)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read checkpoint: %v", err)
	}
	changed := mutate(data)
	if string(changed) == string(data) {
		t.Fatalf("corruption did not alter checkpoint json")
	}
	if err := os.WriteFile(path, changed, 0o644); err != nil {
		t.Fatalf("write checkpoint: %v", err)
	}
}

// TestCheckpointRejectsCorruptIdempotencyResult is the regression test for the
// bug where a parseable change to the cached booking identifier in a
// checkpoint's idempotency record left the recorded summary hash unchanged, so
// startup accepted the corrupted checkpoint and replays returned a broken
// reservation id. Startup must refuse to run on such partially-trusted state.
func TestCheckpointRejectsCorruptIdempotencyResult(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	f, err := OpenFile(dir, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	now := time.Now()
	op := reserveOp(1, "r1", 100, now)
	op.RequestID = "rq1"
	op.Digest = domain.ReserveInput{RequestID: "rq1", CampaignID: "c1", Channel: "ch1", Amount: 100}.Digest()
	if _, _, err := f.Apply(ctx, func(st *domain.State) (*Action, error) {
		op.PrevHash = st.LastHash
		return CommitAction(op), nil
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if _, err := f.Checkpoint(ctx); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Flip the cached booking id in the idempotency record (MarshalIndent
	// emits "reservation_id": "r1"); the change is valid JSON, so only the
	// summary hash can catch it.
	corruptCheckpointJSON(t, dir, func(b []byte) []byte {
		return bytes.Replace(b, []byte(`"reservation_id": "r1"`), []byte(`"reservation_id": "rEVIL"`), 1)
	})

	f2, err := OpenFile(dir, cfg)
	if err == nil {
		// If startup wrongly accepted the checkpoint, the replayed result
		// must not carry the corrupted id.
		rec, ok := f2.GetIdempotency(ctx, "reserve:rq1")
		f2.Close()
		if ok && rec != nil && rec.Result != nil && rec.Result.ReservationID == "rEVIL" {
			t.Fatal("startup accepted checkpoint with corrupted idempotency result")
		}
		t.Fatal("expected startup to reject checkpoint with corrupted idempotency result")
	}
}

// TestCheckpointRejectsCorruptIdempotencyDigest verifies that corrupting the
// request digest stored on an idempotency record (used to detect conflicting
// retries) is also rejected at startup, since it affects replay semantics.
func TestCheckpointRejectsCorruptIdempotencyDigest(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	f, err := OpenFile(dir, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	now := time.Now()
	op := reserveOp(1, "r1", 100, now)
	op.RequestID = "rq1"
	op.Digest = domain.ReserveInput{RequestID: "rq1", CampaignID: "c1", Channel: "ch1", Amount: 100}.Digest()
	if _, _, err := f.Apply(ctx, func(st *domain.State) (*Action, error) {
		op.PrevHash = st.LastHash
		return CommitAction(op), nil
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if _, err := f.Checkpoint(ctx); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// The digest is a 32-byte JSON array; flip the first element. Locate the
	// idempotency record's digest by rewriting the substring that follows the
	// record key.
	corruptCheckpointJSON(t, dir, func(b []byte) []byte {
		// The record carries Result with reservation_id "r1"; the digest array
		// appears earlier in the record. Replace the leading [N, of the digest
		// following the reservation_id of the reserve op payload. To stay
		// robust, flip the first numeric entry of the digest array by turning
		// a leading 0 element into 255.
		idx := bytes.Index(b, []byte(`"digest": [`))
		if idx < 0 {
			idx = bytes.Index(b, []byte(`"digest":[`))
		}
		if idx < 0 {
			t.Fatalf("digest array not found in checkpoint json")
		}
		// Find the first digit after the bracket.
		rest := b[idx:]
		bi := bytes.IndexByte(rest, '[')
		start := idx + bi + 1
		// Skip whitespace.
		for start < len(b) && (b[start] == ' ' || b[start] == '\n' || b[start] == '\t') {
			start++
		}
		if start >= len(b) || b[start] < '0' || b[start] > '9' {
			t.Fatalf("could not locate digest first element")
		}
		// Parse the integer, flip all bits, rewrite.
		end := start
		for end < len(b) && b[end] >= '0' && b[end] <= '9' {
			end++
		}
		var n int
		fmt.Sscanf(string(b[start:end]), "%d", &n)
		out := append([]byte{}, b[:start]...)
		out = append(out, []byte(fmt.Sprintf("%d", n^0xFF))...)
		out = append(out, b[end:]...)
		return out
	})

	if _, err := OpenFile(dir, cfg); err == nil {
		t.Fatal("expected startup to reject checkpoint with corrupted idempotency digest")
	}
}

// TestCheckpointNormalRestartReplaysIdempotency verifies the happy path: after a
// clean checkpoint and restart, an idempotent retry of a committed request
// returns the original result (correct booking id) without a new commit.
func TestCheckpointNormalRestartReplaysIdempotency(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	f, err := OpenFile(dir, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	now := time.Now()
	op := reserveOp(1, "r1", 100, now)
	op.RequestID = "rq1"
	op.Digest = domain.ReserveInput{RequestID: "rq1", CampaignID: "c1", Channel: "ch1", Amount: 100}.Digest()
	if _, _, err := f.Apply(ctx, func(st *domain.State) (*Action, error) {
		if rec := st.IdemRecord("reserve:rq1"); rec != nil {
			return NoOpAction(rec.Result), nil
		}
		op.PrevHash = st.LastHash
		return CommitAction(op), nil
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	liveSeq := f.LastSeq()
	if _, err := f.Checkpoint(ctx); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Restart from the valid checkpoint.
	f2, err := OpenFile(dir, cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer f2.Close()
	if got := f2.LastSeq(); got != liveSeq {
		t.Fatalf("lastseq = %d, want %d", got, liveSeq)
	}
	// Idempotent retry: must return the cached result, not re-commit.
	seq2, res2, err := f2.Apply(ctx, func(st *domain.State) (*Action, error) {
		rec := st.IdemRecord("reserve:rq1")
		if rec == nil {
			return nil, errors.New("idempotency record not recovered")
		}
		return NoOpAction(rec.Result), nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if seq2 != 0 {
		t.Fatalf("retry committed a new op: seq %d", seq2)
	}
	if res2 == nil || res2.ReservationID != "r1" || res2.Amount != 100 {
		t.Fatalf("replay result = %+v, want reservation r1 amount 100", res2)
	}
	if got := f2.LastSeq(); got != liveSeq {
		t.Fatalf("lastseq changed after replay: %d, want %d", got, liveSeq)
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
