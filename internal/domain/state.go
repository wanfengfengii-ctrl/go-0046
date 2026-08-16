package domain

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// State is the full materialized budget state: all ledgers, reservations and
// idempotency records, plus the operation-log chain cursor (LastSeq/LastHash)
// and the configuration version the state was built under.
//
// State is NOT safe for concurrent use by itself; the store wraps it with a
// mutex. All mutations flow through Apply, which preserves the budget
// invariants or returns an error without mutating state.
//
// State implements json.Marshaler/Unmarshaler: ledgers are serialized as a
// slice (each Ledger carries its own key fields) because JSON cannot encode a
// map keyed by a struct.
type State struct {
	Ledgers       map[LedgerKey]*Ledger
	Reservations  map[string]*Reservation
	Idempotency   map[string]*IdemRecord
	LastSeq       int64
	LastHash      [32]byte
	ConfigVersion int64
}

// stateJSON is the serializable projection of State.
type stateJSON struct {
	Ledgers       []*Ledger             `json:"ledgers"`
	Reservations  map[string]*Reservation `json:"reservations"`
	Idempotency   map[string]*IdemRecord  `json:"idempotency"`
	LastSeq       int64                 `json:"last_seq"`
	LastHash      [32]byte              `json:"last_hash"`
	ConfigVersion int64                 `json:"config_version"`
}

// MarshalJSON implements json.Marshaler.
func (s *State) MarshalJSON() ([]byte, error) {
	out := stateJSON{
		Reservations:  s.Reservations,
		Idempotency:   s.Idempotency,
		LastSeq:       s.LastSeq,
		LastHash:      s.LastHash,
		ConfigVersion: s.ConfigVersion,
		Ledgers:       make([]*Ledger, 0, len(s.Ledgers)),
	}
	for _, l := range s.Ledgers {
		out.Ledgers = append(out.Ledgers, l)
	}
	return json.Marshal(out)
}

// UnmarshalJSON implements json.Unmarshaler.
func (s *State) UnmarshalJSON(data []byte) error {
	var in stateJSON
	if err := json.Unmarshal(data, &in); err != nil {
		return err
	}
	s.Ledgers = make(map[LedgerKey]*Ledger, len(in.Ledgers))
	for _, l := range in.Ledgers {
		if l == nil {
			continue
		}
		s.Ledgers[LedgerKey{CampaignID: l.CampaignID, PeriodID: l.PeriodID, Channel: l.Channel}] = l
	}
	s.Reservations = in.Reservations
	if s.Reservations == nil {
		s.Reservations = make(map[string]*Reservation)
	}
	s.Idempotency = in.Idempotency
	if s.Idempotency == nil {
		s.Idempotency = make(map[string]*IdemRecord)
	}
	s.LastSeq = in.LastSeq
	s.LastHash = in.LastHash
	s.ConfigVersion = in.ConfigVersion
	return nil
}

// NewState returns an empty state pinned to configVersion.
func NewState(configVersion int64) *State {
	return &State{
		Ledgers:      make(map[LedgerKey]*Ledger),
		Reservations: make(map[string]*Reservation),
		Idempotency:  make(map[string]*IdemRecord),
		ConfigVersion: configVersion,
	}
}

// AddLedger creates a ledger at the given level with a limit. It is used during
// initialization from configuration.
func (s *State) AddLedger(level LedgerLevel, campaignID, periodID, channel string, limit Micros) *Ledger {
	l := &Ledger{
		Level:      level,
		CampaignID: campaignID,
		PeriodID:   periodID,
		Channel:    channel,
		Limit:      limit,
	}
	s.Ledgers[LedgerKey{CampaignID: campaignID, PeriodID: periodID, Channel: channel}] = l
	return l
}

// CampaignLedger returns the campaign-level ledger or nil.
func (s *State) CampaignLedger(campaignID string) *Ledger {
	return s.Ledgers[LedgerKey{CampaignID: campaignID}]
}

// PeriodLedger returns the period-level ledger or nil.
func (s *State) PeriodLedger(campaignID, periodID string) *Ledger {
	return s.Ledgers[LedgerKey{CampaignID: campaignID, PeriodID: periodID}]
}

// ChannelLedger returns the channel-level ledger or nil.
func (s *State) ChannelLedger(campaignID, periodID, channel string) *Ledger {
	return s.Ledgers[LedgerKey{CampaignID: campaignID, PeriodID: periodID, Channel: channel}]
}

// Reservation returns the reservation with the given id or nil.
func (s *State) Reservation(id string) *Reservation {
	return s.Reservations[id]
}

// IdemRecord returns the idempotency record for a key or nil.
func (s *State) IdemRecord(key string) *IdemRecord {
	return s.Idempotency[key]
}

// Apply transitions the state according to op. It is the single source of
// truth for state transitions, used both live and during log replay. It
// validates invariants and returns an error without mutating state if the
// operation would violate them. OpRepay performs no state change.
func (s *State) Apply(op *Op) error {
	switch op.Type {
	case OpReplay:
		return nil
	case OpReserve:
		return s.applyReserve(op)
	case OpConfirm:
		return s.applyConfirm(op)
	case OpRelease:
		return s.applyRelease(op)
	case OpRefund:
		return s.applyRefund(op)
	case OpExpire:
		return s.applyExpire(op)
	}
	return NewError(CodeInternal, "unknown op type")
}

func (s *State) applyReserve(op *Op) error {
	cl := s.CampaignLedger(op.CampaignID)
	pl := s.PeriodLedger(op.CampaignID, op.PeriodID)
	chl := s.ChannelLedger(op.CampaignID, op.PeriodID, op.Channel)
	if cl == nil || pl == nil || chl == nil {
		return NewError(CodeInternal, "missing ledger for reserve")
	}
	if op.Amount <= 0 {
		return NewError(CodeInvalidRequest, "reserve amount must be positive")
	}
	// Hard availability invariant: no level may go negative.
	if cl.Available() < op.Amount {
		return NewError(CodeNoBudget, "campaign budget exceeded")
	}
	if pl.Available() < op.Amount {
		return NewError(CodeNoBudget, "period budget exceeded")
	}
	if chl.Available() < op.Amount {
		return NewError(CodeNoBudget, "channel budget exceeded")
	}
	cl.Pending += op.Amount
	pl.Pending += op.Amount
	chl.Pending += op.Amount
	if err := cl.Validate(); err != nil {
		return err
	}
	if err := pl.Validate(); err != nil {
		return err
	}
	if err := chl.Validate(); err != nil {
		return err
	}
	s.Reservations[op.ReservationID] = &Reservation{
		ID:         op.ReservationID,
		CampaignID: op.CampaignID,
		PeriodID:   op.PeriodID,
		Channel:    op.Channel,
		Amount:     op.Amount,
		State:      StateReserved,
		CreatedAt:  op.Now,
		ExpiresAt:  op.ExpiresAt,
		RequestID:  op.RequestID,
	}
	key := "reserve:" + op.RequestID
	s.Idempotency[key] = &IdemRecord{
		Key:           key,
		Kind:          IdemReserve,
		Digest:        op.Digest,
		Result:        op.Result,
		Seq:           op.Seq,
		ReservationID: op.ReservationID,
		CreatedAt:     op.Now,
	}
	return nil
}

func (s *State) applyConfirm(op *Op) error {
	res := s.Reservations[op.ReservationID]
	if res == nil {
		return NewError(CodeUnknownReservation, "confirm: unknown reservation")
	}
	if res.State != StateReserved {
		return NewError(CodeInvalidState, "confirm: reservation not RESERVED")
	}
	if op.ConfirmAmount <= 0 {
		return NewError(CodeOverConfirm, "confirm amount must be positive")
	}
	if op.ConfirmAmount > res.Amount {
		return NewError(CodeOverConfirm, "confirm exceeds reserved")
	}
	cl := s.CampaignLedger(res.CampaignID)
	pl := s.PeriodLedger(res.CampaignID, res.PeriodID)
	chl := s.ChannelLedger(res.CampaignID, res.PeriodID, res.Channel)
	if cl == nil || pl == nil || chl == nil {
		return NewError(CodeInternal, "missing ledger for confirm")
	}
	// Release the full pending hold and recognize the confirmed spend. The
	// difference (reserved - confirmed) returns to availability via the
	// invariant formula.
	cl.Pending -= res.Amount
	pl.Pending -= res.Amount
	chl.Pending -= res.Amount
	cl.Confirmed += op.ConfirmAmount
	pl.Confirmed += op.ConfirmAmount
	chl.Confirmed += op.ConfirmAmount
	if err := cl.Validate(); err != nil {
		return err
	}
	if err := pl.Validate(); err != nil {
		return err
	}
	if err := chl.Validate(); err != nil {
		return err
	}
	res.ConfirmedAmount = op.ConfirmAmount
	res.State = StateConfirmed
	s.recordSettle(op, res)
	return nil
}

func (s *State) applyRelease(op *Op) error {
	res := s.Reservations[op.ReservationID]
	if res == nil {
		return NewError(CodeUnknownReservation, "release: unknown reservation")
	}
	if res.State != StateReserved {
		return NewError(CodeInvalidState, "release: reservation not RESERVED")
	}
	cl := s.CampaignLedger(res.CampaignID)
	pl := s.PeriodLedger(res.CampaignID, res.PeriodID)
	chl := s.ChannelLedger(res.CampaignID, res.PeriodID, res.Channel)
	if cl == nil || pl == nil || chl == nil {
		return NewError(CodeInternal, "missing ledger for release")
	}
	cl.Pending -= res.Amount
	pl.Pending -= res.Amount
	chl.Pending -= res.Amount
	if err := cl.Validate(); err != nil {
		return err
	}
	if err := pl.Validate(); err != nil {
		return err
	}
	if err := chl.Validate(); err != nil {
		return err
	}
	res.State = StateReleased
	s.recordSettle(op, res)
	return nil
}

func (s *State) applyRefund(op *Op) error {
	res := s.Reservations[op.ReservationID]
	if res == nil {
		return NewError(CodeUnknownReservation, "refund: unknown reservation")
	}
	if res.State != StateConfirmed {
		return NewError(CodeInvalidState, "refund: reservation not CONFIRMED")
	}
	if op.RefundAmount <= 0 {
		return NewError(CodeOverRefund, "refund amount must be positive")
	}
	if res.RefundedAmount+op.RefundAmount > res.ConfirmedAmount {
		return NewError(CodeOverRefund, "refund exceeds confirmed")
	}
	cl := s.CampaignLedger(res.CampaignID)
	pl := s.PeriodLedger(res.CampaignID, res.PeriodID)
	chl := s.ChannelLedger(res.CampaignID, res.PeriodID, res.Channel)
	if cl == nil || pl == nil || chl == nil {
		return NewError(CodeInternal, "missing ledger for refund")
	}
	cl.Refunded += op.RefundAmount
	pl.Refunded += op.RefundAmount
	chl.Refunded += op.RefundAmount
	if err := cl.Validate(); err != nil {
		return err
	}
	if err := pl.Validate(); err != nil {
		return err
	}
	if err := chl.Validate(); err != nil {
		return err
	}
	res.RefundedAmount += op.RefundAmount
	s.recordSettle(op, res)
	return nil
}

func (s *State) applyExpire(op *Op) error {
	res := s.Reservations[op.ReservationID]
	if res == nil {
		return NewError(CodeUnknownReservation, "expire: unknown reservation")
	}
	if res.State != StateReserved {
		return NewError(CodeInvalidState, "expire: reservation not RESERVED")
	}
	cl := s.CampaignLedger(res.CampaignID)
	pl := s.PeriodLedger(res.CampaignID, res.PeriodID)
	chl := s.ChannelLedger(res.CampaignID, res.PeriodID, res.Channel)
	if cl == nil || pl == nil || chl == nil {
		return NewError(CodeInternal, "missing ledger for expire")
	}
	cl.Pending -= res.Amount
	pl.Pending -= res.Amount
	chl.Pending -= res.Amount
	if err := cl.Validate(); err != nil {
		return err
	}
	if err := pl.Validate(); err != nil {
		return err
	}
	if err := chl.Validate(); err != nil {
		return err
	}
	res.State = StateExpired
	return nil
}

// recordSettle writes the idempotency record for a settlement notification.
func (s *State) recordSettle(op *Op, res *Reservation) {
	key := "settle:" + op.NotifID
	s.Idempotency[key] = &IdemRecord{
		Key:           key,
		Kind:          IdemSettle,
		Digest:        op.Digest,
		Result:        op.Result,
		Seq:           op.Seq,
		ReservationID: res.ID,
		CreatedAt:     op.Now,
	}
}

// Validate checks every ledger invariant. It is used for checkpoint integrity
// and defensive assertions.
func (s *State) Validate() error {
	for k, l := range s.Ledgers {
		if err := l.Validate(); err != nil {
			return fmt.Errorf("ledger %v: %w", k, err)
		}
	}
	return nil
}

// SummaryHash returns a canonical hash of the state, used to detect checkpoint
// corruption. It hashes the chain cursor, config version, and sorted ledger,
// reservation, and idempotency snapshots.
//
// Idempotency records are covered in full: the cached Result (including the
// booking identifier returned on replay), the request Digest used to detect
// conflicting retries, and the structural fields. A corrupted idempotent
// return value therefore changes the summary hash and is rejected at startup,
// rather than silently replaying a broken result. Only the metadata-only
// CreatedAt field is excluded, matching the reservation snapshot which omits
// timestamps.
func (s *State) SummaryHash() [32]byte {
	h := sha256.New()
	var lb [8]byte
	binary.LittleEndian.PutUint64(lb[:], uint64(s.LastSeq))
	h.Write(lb[:])
	h.Write(s.LastHash[:])
	binary.LittleEndian.PutUint64(lb[:], uint64(s.ConfigVersion))
	h.Write(lb[:])
	keys := make([]LedgerKey, 0, len(s.Ledgers))
	for k := range s.Ledgers {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return ledgerKeyLess(keys[i], keys[j])
	})
	for _, k := range keys {
		l := s.Ledgers[k]
		writeStr(h, k.CampaignID)
		writeStr(h, k.PeriodID)
		writeStr(h, k.Channel)
		writeI64(h, l.Limit)
		writeI64(h, l.Pending)
		writeI64(h, l.Confirmed)
		writeI64(h, l.Refunded)
	}
	ids := make([]string, 0, len(s.Reservations))
	for id := range s.Reservations {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		r := s.Reservations[id]
		writeStr(h, id)
		writeStr(h, r.CampaignID)
		writeStr(h, r.PeriodID)
		writeStr(h, r.Channel)
		writeI64(h, r.Amount)
		writeI64(h, r.ConfirmedAmount)
		writeI64(h, r.RefundedAmount)
		writeI64(h, int64(r.State))
	}
	idemKeys := make([]string, 0, len(s.Idempotency))
	for k := range s.Idempotency {
		idemKeys = append(idemKeys, k)
	}
	sort.Strings(idemKeys)
	for _, k := range idemKeys {
		r := s.Idempotency[k]
		// Hash both the map key (used for replay lookup) and the record's own
		// Key field (a redundant copy that callers may read); a mismatch
		// between the two, or corruption of either, must change the hash.
		writeStr(h, k)
		writeStr(h, r.Key)
		writeI64(h, int64(r.Kind))
		h.Write(r.Digest[:])
		writeI64(h, r.Seq)
		writeStr(h, r.ReservationID)
		writeOpResultHash(h, r.Result)
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// writeOpResultHash folds an idempotent op result into the summary hash. The
// flag distinguishes a present result from an absent one so that corrupting a
// nil result into a non-nil one (or vice versa) also changes the hash. Every
// field is a stable JSON type, so the value survives checkpoint round-trips.
func writeOpResultHash(h interface{ Write([]byte) (int, error) }, r *OpResult) {
	if r == nil {
		writeI64(h, 0)
		return
	}
	writeI64(h, 1)
	var ok int64
	if r.OK {
		ok = 1
	}
	writeI64(h, ok)
	writeStr(h, r.Code)
	writeStr(h, r.ReservationID)
	writeI64(h, int64(r.State))
	writeI64(h, r.Amount)
	writeI64(h, r.ConfirmedAmount)
	writeI64(h, r.RefundedAmount)
	writeI64(h, r.Seq)
}

func ledgerKeyLess(a, b LedgerKey) bool {
	if a.CampaignID != b.CampaignID {
		return a.CampaignID < b.CampaignID
	}
	if a.PeriodID != b.PeriodID {
		return a.PeriodID < b.PeriodID
	}
	return a.Channel < b.Channel
}

// Clone returns a deep copy of the state. Callers use it to obtain a stable
// read-only snapshot for ops endpoints without holding the store lock.
func (s *State) Clone() *State {
	out := &State{
		Ledgers:       make(map[LedgerKey]*Ledger, len(s.Ledgers)),
		Reservations:  make(map[string]*Reservation, len(s.Reservations)),
		Idempotency:   make(map[string]*IdemRecord, len(s.Idempotency)),
		LastSeq:       s.LastSeq,
		LastHash:      s.LastHash,
		ConfigVersion: s.ConfigVersion,
	}
	for k, l := range s.Ledgers {
		cp := *l
		out.Ledgers[k] = &cp
	}
	for id, r := range s.Reservations {
		cp := *r
		out.Reservations[id] = &cp
	}
	for k, r := range s.Idempotency {
		cp := *r
		if r.Result != nil {
			rc := *r.Result
			cp.Result = &rc
		}
		out.Idempotency[k] = &cp
	}
	return out
}

// SnapshotLedgers returns a flat, sorted list of ledgers for display.
func (s *State) SnapshotLedgers() []*Ledger {
	out := make([]*Ledger, 0, len(s.Ledgers))
	keys := make([]LedgerKey, 0, len(s.Ledgers))
	for k := range s.Ledgers {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return ledgerKeyLess(keys[i], keys[j]) })
	for _, k := range keys {
		cp := *s.Ledgers[k]
		out = append(out, &cp)
	}
	return out
}

// ExpireCandidates returns the ids of RESERVED reservations whose ExpiresAt is
// at or before now, sorted for deterministic expiry processing.
func (s *State) ExpireCandidates(now time.Time) []string {
	var out []string
	for id, r := range s.Reservations {
		if r.State == StateReserved && !r.ExpiresAt.After(now) {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// HashOp computes the chain hash of an op: sha256 of the op's JSON payload,
// which includes Seq and PrevHash but excludes any computed self-hash. The
// store writes this hash alongside the payload and uses it as the next op's
// PrevHash.
func HashOp(op *Op) [32]byte {
	data, err := EncodeOp(op)
	if err != nil {
		// EncodeOp only fails on unsupported types; all Op fields are
		// JSON-safe, so this is unreachable in practice.
		return [32]byte{}
	}
	return sha256.Sum256(data)
}
