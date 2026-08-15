package domain

import (
	"testing"
	"time"
)

func newStateWithLedgers() *State {
	s := NewState(1)
	s.AddLedger(LevelCampaign, "c1", "", "", 1_000_000)
	s.AddLedger(LevelPeriod, "c1", "p1", "", 500_000)
	s.AddLedger(LevelChannel, "c1", "p1", "ch1", 200_000)
	return s
}

func TestLedgerInvariantFormula(t *testing.T) {
	l := &Ledger{Level: LevelCampaign, Limit: 100, Pending: 20, Confirmed: 30, Refunded: 10}
	if got, want := l.Available(), int64(100-20-30+10); got != want {
		t.Fatalf("Available = %d, want %d", got, want)
	}
	if got, want := l.Occupied(), int64(20+30-10); got != want {
		t.Fatalf("Occupied = %d, want %d", got, want)
	}
}

func TestLedgerValidateRejectsNegativesAndOverspend(t *testing.T) {
	cases := []struct {
		name string
		l    Ledger
	}{
		{"negative limit", Ledger{Level: LevelCampaign, Limit: -1}},
		{"negative pending", Ledger{Level: LevelCampaign, Limit: 10, Pending: -1}},
		{"negative confirmed", Ledger{Level: LevelCampaign, Limit: 10, Confirmed: -1}},
		{"negative refunded", Ledger{Level: LevelCampaign, Limit: 10, Refunded: -1}},
		{"refunded over confirmed", Ledger{Level: LevelCampaign, Limit: 10, Confirmed: 5, Refunded: 6}},
		{"negative available", Ledger{Level: LevelCampaign, Limit: 10, Pending: 11}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.l.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestReserveIncrementsPendingAllLevels(t *testing.T) {
	s := newStateWithLedgers()
	now := time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC)
	op := &Op{
		Type: OpReserve, Seq: 1, ReservationID: "r1",
		CampaignID: "c1", PeriodID: "p1", Channel: "ch1",
		Amount: 100, Now: now, ExpiresAt: now.Add(time.Minute),
		Digest: [32]byte{1},
		Result: &OpResult{OK: true, Code: CodeOK, ReservationID: "r1", Amount: 100, State: StateReserved},
	}
	op.PrevHash = s.LastHash
	if err := s.Apply(op); err != nil {
		t.Fatalf("apply reserve: %v", err)
	}
	for _, l := range []*Ledger{
		s.CampaignLedger("c1"),
		s.PeriodLedger("c1", "p1"),
		s.ChannelLedger("c1", "p1", "ch1"),
	} {
		if l.Pending != 100 {
			t.Fatalf("%+v pending = %d, want 100", l, l.Pending)
		}
		if l.Available() != l.Limit-100 {
			t.Fatalf("available = %d, want %d", l.Available(), l.Limit-100)
		}
	}
	if r := s.Reservation("r1"); r == nil || r.State != StateReserved {
		t.Fatalf("reservation not stored: %+v", r)
	}
}

func TestReserveRejectsOverspend(t *testing.T) {
	s := newStateWithLedgers()
	now := time.Now()
	// Channel limit is 200_000; reserve 200_001 must be rejected.
	op := &Op{Type: OpReserve, ReservationID: "r1", CampaignID: "c1", PeriodID: "p1", Channel: "ch1",
		Amount: 200_001, Now: now, ExpiresAt: now.Add(time.Minute), Result: &OpResult{}}
	if err := s.Apply(op); !IsDomainError(err, CodeNoBudget) {
		t.Fatalf("expected no_budget, got %v", err)
	}
	// Campaign limit is 1_000_000; an amount over the channel but under the
	// campaign still fails at the channel level.
	op.Amount = 500_000
	if err := s.Apply(op); !IsDomainError(err, CodeNoBudget) {
		t.Fatalf("expected no_budget on channel, got %v", err)
	}
}

func TestConfirmReleasesDifference(t *testing.T) {
	s := newStateWithLedgers()
	now := time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC)
	reserve := &Op{Type: OpReserve, Seq: 1, ReservationID: "r1", CampaignID: "c1", PeriodID: "p1", Channel: "ch1",
		Amount: 100, Now: now, ExpiresAt: now.Add(time.Minute), Digest: [32]byte{1}, Result: &OpResult{OK: true}}
	reserve.PrevHash = s.LastHash
	_ = s.Apply(reserve)
	cl := s.CampaignLedger("c1")
	beforeAvail := cl.Available()
	// Confirm 60 of 100 → pending drops by 100, confirmed rises by 60.
	confirm := &Op{Type: OpConfirm, Seq: 2, NotifID: "n1", ReservationID: "r1",
		ConfirmAmount: 60, Now: now, Digest: [32]byte{2}, Result: &OpResult{OK: true}}
	confirm.PrevHash = HashOp(reserve)
	if err := s.Apply(confirm); err != nil {
		t.Fatalf("apply confirm: %v", err)
	}
	if got := cl.Pending; got != 0 {
		t.Fatalf("pending = %d, want 0", got)
	}
	if got := cl.Confirmed; got != 60 {
		t.Fatalf("confirmed = %d, want 60", got)
	}
	// Available should rise by the released difference (100-60=40).
	if got, want := cl.Available(), beforeAvail+40; got != want {
		t.Fatalf("available = %d, want %d", got, want)
	}
	r := s.Reservation("r1")
	if r.State != StateConfirmed || r.ConfirmedAmount != 60 {
		t.Fatalf("reservation = %+v", r)
	}
}

func TestConfirmRejectsOverConfirmAndBadState(t *testing.T) {
	s := newStateWithLedgers()
	now := time.Now()
	reserve := &Op{Type: OpReserve, Seq: 1, ReservationID: "r1", CampaignID: "c1", PeriodID: "p1", Channel: "ch1",
		Amount: 100, Now: now, ExpiresAt: now.Add(time.Minute), Digest: [32]byte{1}, Result: &OpResult{OK: true}}
	reserve.PrevHash = s.LastHash
	_ = s.Apply(reserve)
	// Over-confirm.
	over := &Op{Type: OpConfirm, NotifID: "n1", ReservationID: "r1", ConfirmAmount: 101, Now: now, Result: &OpResult{}}
	if err := s.Apply(over); !IsDomainError(err, CodeOverConfirm) {
		t.Fatalf("expected over_confirm, got %v", err)
	}
	// Confirm then re-confirm (state no longer RESERVED).
	ok := &Op{Type: OpConfirm, Seq: 2, NotifID: "n1", ReservationID: "r1", ConfirmAmount: 50, Now: now, Digest: [32]byte{2}, Result: &OpResult{OK: true}}
	ok.PrevHash = HashOp(reserve)
	_ = s.Apply(ok)
	dup := &Op{Type: OpConfirm, NotifID: "n2", ReservationID: "r1", ConfirmAmount: 50, Now: now, Result: &OpResult{}}
	if err := s.Apply(dup); !IsDomainError(err, CodeInvalidState) {
		t.Fatalf("expected invalid_state, got %v", err)
	}
	// Unknown reservation.
	unk := &Op{Type: OpConfirm, NotifID: "n3", ReservationID: "nope", ConfirmAmount: 1, Now: now, Result: &OpResult{}}
	if err := s.Apply(unk); !IsDomainError(err, CodeUnknownReservation) {
		t.Fatalf("expected unknown_reservation, got %v", err)
	}
}

func TestReleaseReturnsPending(t *testing.T) {
	s := newStateWithLedgers()
	now := time.Now()
	reserve := &Op{Type: OpReserve, Seq: 1, ReservationID: "r1", CampaignID: "c1", PeriodID: "p1", Channel: "ch1",
		Amount: 100, Now: now, ExpiresAt: now.Add(time.Minute), Digest: [32]byte{1}, Result: &OpResult{OK: true}}
	reserve.PrevHash = s.LastHash
	_ = s.Apply(reserve)
	cl := s.CampaignLedger("c1")
	before := cl.Available()
	release := &Op{Type: OpRelease, Seq: 2, NotifID: "n1", ReservationID: "r1", Now: now, Digest: [32]byte{2}, Result: &OpResult{OK: true}}
	release.PrevHash = HashOp(reserve)
	if err := s.Apply(release); err != nil {
		t.Fatalf("apply release: %v", err)
	}
	if cl.Pending != 0 || cl.Available() != before+100 {
		t.Fatalf("ledger = %+v", cl)
	}
	if r := s.Reservation("r1"); r.State != StateReleased || !r.IsTerminal() {
		t.Fatalf("reservation = %+v", r)
	}
}

func TestRefundAccumulatesUpToConfirmed(t *testing.T) {
	s := newStateWithLedgers()
	now := time.Now()
	reserve := &Op{Type: OpReserve, Seq: 1, ReservationID: "r1", CampaignID: "c1", PeriodID: "p1", Channel: "ch1",
		Amount: 100, Now: now, ExpiresAt: now.Add(time.Minute), Digest: [32]byte{1}, Result: &OpResult{OK: true}}
	reserve.PrevHash = s.LastHash
	_ = s.Apply(reserve)
	confirm := &Op{Type: OpConfirm, Seq: 2, NotifID: "n1", ReservationID: "r1", ConfirmAmount: 80, Now: now, Digest: [32]byte{2}, Result: &OpResult{OK: true}}
	confirm.PrevHash = HashOp(reserve)
	_ = s.Apply(confirm)
	cl := s.CampaignLedger("c1")
	before := cl.Available()
	// First refund 30.
	r1 := &Op{Type: OpRefund, Seq: 3, NotifID: "rf1", ReservationID: "r1", RefundAmount: 30, Now: now, Digest: [32]byte{3}, Result: &OpResult{OK: true}}
	r1.PrevHash = HashOp(confirm)
	if err := s.Apply(r1); err != nil {
		t.Fatalf("apply refund 1: %v", err)
	}
	if cl.Available() != before+30 {
		t.Fatalf("available after refund1 = %d, want %d", cl.Available(), before+30)
	}
	// Second refund 50 (cumulative 80, the max).
	r2 := &Op{Type: OpRefund, Seq: 4, NotifID: "rf2", ReservationID: "r1", RefundAmount: 50, Now: now, Digest: [32]byte{4}, Result: &OpResult{OK: true}}
	r2.PrevHash = HashOp(r1)
	if err := s.Apply(r2); err != nil {
		t.Fatalf("apply refund 2: %v", err)
	}
	// Over-refund now (cumulative would be 90 > 80).
	r3 := &Op{Type: OpRefund, NotifID: "rf3", ReservationID: "r1", RefundAmount: 10, Now: now, Result: &OpResult{}}
	if err := s.Apply(r3); !IsDomainError(err, CodeOverRefund) {
		t.Fatalf("expected over_refund, got %v", err)
	}
	r := s.Reservation("r1")
	if r.RefundedAmount != 80 || r.RefundedAmount > r.ConfirmedAmount {
		t.Fatalf("reservation refunds = %+v", r)
	}
	if cl.Refunded > cl.Confirmed {
		t.Fatalf("ledger refunded %d > confirmed %d", cl.Refunded, cl.Confirmed)
	}
}

func TestExpireReturnsPending(t *testing.T) {
	s := newStateWithLedgers()
	now := time.Now()
	reserve := &Op{Type: OpReserve, Seq: 1, ReservationID: "r1", CampaignID: "c1", PeriodID: "p1", Channel: "ch1",
		Amount: 100, Now: now, ExpiresAt: now.Add(time.Minute), Digest: [32]byte{1}, Result: &OpResult{OK: true}}
	reserve.PrevHash = s.LastHash
	_ = s.Apply(reserve)
	cl := s.CampaignLedger("c1")
	before := cl.Available()
	expire := &Op{Type: OpExpire, Seq: 2, ReservationID: "r1", Now: now.Add(2 * time.Minute), Result: &OpResult{OK: true}}
	expire.PrevHash = HashOp(reserve)
	if err := s.Apply(expire); err != nil {
		t.Fatalf("apply expire: %v", err)
	}
	if cl.Pending != 0 || cl.Available() != before+100 {
		t.Fatalf("ledger = %+v", cl)
	}
	if r := s.Reservation("r1"); r.State != StateExpired {
		t.Fatalf("reservation = %+v", r)
	}
	// Expiring again is rejected.
	if err := s.Apply(&Op{Type: OpExpire, ReservationID: "r1", Result: &OpResult{}}); !IsDomainError(err, CodeInvalidState) {
		t.Fatalf("expected invalid_state, got %v", err)
	}
}

func TestRefundRequiresConfirmed(t *testing.T) {
	s := newStateWithLedgers()
	now := time.Now()
	reserve := &Op{Type: OpReserve, Seq: 1, ReservationID: "r1", CampaignID: "c1", PeriodID: "p1", Channel: "ch1",
		Amount: 100, Now: now, ExpiresAt: now.Add(time.Minute), Digest: [32]byte{1}, Result: &OpResult{OK: true}}
	reserve.PrevHash = s.LastHash
	_ = s.Apply(reserve)
	if err := s.Apply(&Op{Type: OpRefund, NotifID: "rf1", ReservationID: "r1", RefundAmount: 10, Now: now, Result: &OpResult{}}); !IsDomainError(err, CodeInvalidState) {
		t.Fatalf("expected invalid_state for refund-before-confirm, got %v", err)
	}
}

func TestStateCloneIsDeep(t *testing.T) {
	s := newStateWithLedgers()
	now := time.Now()
	op := &Op{Type: OpReserve, Seq: 1, ReservationID: "r1", CampaignID: "c1", PeriodID: "p1", Channel: "ch1",
		Amount: 100, Now: now, ExpiresAt: now.Add(time.Minute), Digest: [32]byte{1}, Result: &OpResult{OK: true}}
	op.PrevHash = s.LastHash
	_ = s.Apply(op)
	c := s.Clone()
	c.CampaignLedger("c1").Pending = 999
	if s.CampaignLedger("c1").Pending == 999 {
		t.Fatal("clone shared ledger backing storage")
	}
}

func TestExpireCandidates(t *testing.T) {
	s := newStateWithLedgers()
	now := time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC)
	for i, off := range []time.Duration{time.Minute, 2 * time.Minute, 3 * time.Minute} {
		op := &Op{Type: OpReserve, Seq: int64(i + 1), ReservationID: "r" + string(rune('1'+i)),
			CampaignID: "c1", PeriodID: "p1", Channel: "ch1", Amount: 1, Now: now, ExpiresAt: now.Add(off),
			Digest: [32]byte{byte(i + 1)}, Result: &OpResult{OK: true}}
		op.PrevHash = s.LastHash
		s.LastHash = HashOp(op)
		s.LastSeq = op.Seq
		_ = s.Apply(op)
	}
	cands := s.ExpireCandidates(now.Add(90 * time.Second))
	if len(cands) != 1 || cands[0] != "r1" {
		t.Fatalf("candidates = %v, want [r1]", cands)
	}
	cands = s.ExpireCandidates(now.Add(4 * time.Minute))
	if len(cands) != 3 {
		t.Fatalf("candidates = %v, want 3", cands)
	}
}

func TestStateSummaryHashStable(t *testing.T) {
	s1 := newStateWithLedgers()
	s2 := newStateWithLedgers()
	if s1.SummaryHash() != s2.SummaryHash() {
		t.Fatal("identical states have different summary hashes")
	}
	now := time.Now()
	op := &Op{Type: OpReserve, Seq: 1, ReservationID: "r1", CampaignID: "c1", PeriodID: "p1", Channel: "ch1",
		Amount: 1, Now: now, ExpiresAt: now.Add(time.Minute), Digest: [32]byte{1}, Result: &OpResult{OK: true}}
	op.PrevHash = s1.LastHash
	_ = s1.Apply(op)
	if s1.SummaryHash() == s2.SummaryHash() {
		t.Fatal("different states have identical summary hashes")
	}
}
