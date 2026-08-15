// Package domain implements the budget-conservation core of the auction
// pacing reservation engine.
//
// The model maintains a three-tier ledger for every campaign: a campaign-level
// ledger, a per-period ledger, and a per-period-channel ledger. Every
// reservation touches all three levels atomically. The fundamental invariant
// maintained after every operation is:
//
//	available = limit - pending - confirmed + refunded
//
// with refunded <= confirmed and every counter non-negative. Available may
// never be negative, which makes overspending provably impossible: any
// operation that would drive a ledger negative is rejected before it mutates
// state.
//
// State.Apply is the single source of truth for state transitions. It is used
// both by the live executor (to commit operations) and by the recovery path
// (to replay the operation log), so live and replayed state are identical by
// construction.
package domain

import (
	"errors"
	"fmt"
	"time"
)

// Micros is the monetary unit: integer micro-units (1e-6 of a currency unit).
// Amounts are always strictly positive for reserves, confirms and refunds.
type Micros = int64

// ReservationState is the lifecycle state of a reservation.
type ReservationState int

const (
	// StateReserved means the reservation holds pending budget awaiting
	// settlement. It is the only state from which confirm, release and expire
	// may proceed.
	StateReserved ReservationState = iota + 1
	// StateConfirmed means the reservation has been at least partially
	// confirmed; the difference between reserved and confirmed amounts is
	// released back to availability. Refunds may accumulate on a confirmed
	// reservation.
	StateConfirmed
	// StateReleased is a terminal state reached by an explicit release of an
	// unconfirmed reservation.
	StateReleased
	// StateExpired is a terminal state reached by the expiry scanner.
	StateExpired
)

// String returns a human-readable state name.
func (s ReservationState) String() string {
	switch s {
	case StateReserved:
		return "RESERVED"
	case StateConfirmed:
		return "CONFIRMED"
	case StateReleased:
		return "RELEASED"
	case StateExpired:
		return "EXPIRED"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int(s))
	}
}

// LedgerLevel identifies which tier of the budget tree a ledger belongs to.
type LedgerLevel int

const (
	LevelCampaign LedgerLevel = iota + 1
	LevelPeriod
	LevelChannel
)

// LedgerKey identifies a ledger within the tree. The campaign ledger uses
// empty PeriodID and Channel; the period ledger uses empty Channel.
type LedgerKey struct {
	CampaignID string
	PeriodID   string
	Channel    string
}

// Ledger is a single budget account.
type Ledger struct {
	Level     LedgerLevel `json:"level"`
	CampaignID string     `json:"campaign_id"`
	PeriodID   string     `json:"period_id"`
	Channel    string     `json:"channel"`
	Limit      Micros     `json:"limit"`
	Pending    Micros     `json:"pending"`
	Confirmed  Micros     `json:"confirmed"`
	Refunded   Micros     `json:"refunded"`
}

// Available returns the spendable balance derived from the invariant formula.
func (l *Ledger) Available() Micros {
	return l.Limit - l.Pending - l.Confirmed + l.Refunded
}

// Occupied returns the net occupied amount: pending spend plus confirmed spend
// minus refunds. Pacing compares this against the pacing water level.
func (l *Ledger) Occupied() Micros {
	return l.Pending + l.Confirmed - l.Refunded
}

// Validate enforces the budget invariants. It returns an error if any counter
// is negative, if refunded exceeds confirmed, or if availability is negative.
func (l *Ledger) Validate() error {
	if l.Limit < 0 {
		return fmt.Errorf("ledger %s: limit negative", l.key())
	}
	if l.Pending < 0 {
		return fmt.Errorf("ledger %s: pending negative", l.key())
	}
	if l.Confirmed < 0 {
		return fmt.Errorf("ledger %s: confirmed negative", l.key())
	}
	if l.Refunded < 0 {
		return fmt.Errorf("ledger %s: refunded negative", l.key())
	}
	if l.Refunded > l.Confirmed {
		return fmt.Errorf("ledger %s: refunded %d > confirmed %d", l.key(), l.Refunded, l.Confirmed)
	}
	if l.Available() < 0 {
		return fmt.Errorf("ledger %s: available %d negative", l.key(), l.Available())
	}
	return nil
}

func (l *Ledger) key() string {
	switch l.Level {
	case LevelCampaign:
		return "campaign:" + l.CampaignID
	case LevelPeriod:
		return "period:" + l.CampaignID + "/" + l.PeriodID
	case LevelChannel:
		return "channel:" + l.CampaignID + "/" + l.PeriodID + "/" + l.Channel
	}
	return "unknown"
}

// Reservation is a single budget hold created by a reserve request.
type Reservation struct {
	ID              string            `json:"id"`
	CampaignID      string            `json:"campaign_id"`
	PeriodID        string            `json:"period_id"`
	Channel         string            `json:"channel"`
	Amount          Micros            `json:"amount"`
	ConfirmedAmount Micros            `json:"confirmed_amount"`
	RefundedAmount  Micros            `json:"refunded_amount"`
	State           ReservationState  `json:"state"`
	CreatedAt       time.Time         `json:"created_at"`
	ExpiresAt       time.Time         `json:"expires_at"`
	RequestID       string            `json:"request_id"`
}

// IsTerminal reports whether the reservation is in a terminal state.
func (r *Reservation) IsTerminal() bool {
	return r.State != StateReserved && r.State != StateConfirmed
}

// IdemKind distinguishes reserve idempotency keys from settlement keys.
type IdemKind int

const (
	IdemReserve IdemKind = iota + 1
	IdemSettle
)

// IdemRecord stores the canonical payload digest and first result for an
// idempotent operation.
type IdemRecord struct {
	Key           string   `json:"key"`
	Kind          IdemKind `json:"kind"`
	Digest        [32]byte `json:"digest"`
	Result        *OpResult `json:"result"`
	Seq           int64    `json:"seq"`
	ReservationID string   `json:"reservation_id"`
	CreatedAt     time.Time `json:"created_at"`
}

// OpType identifies the kind of operation in the log.
type OpType int

const (
	OpReserve OpType = iota + 1
	OpConfirm
	OpRelease
	OpRefund
	OpExpire
	// OpReplay carries no state change; it returns a cached result for an
	// already-committed idempotent operation. It is never written to the log.
	OpReplay
)

// String returns a human-readable op type name.
func (t OpType) String() string {
	switch t {
	case OpReserve:
		return "RESERVE"
	case OpConfirm:
		return "CONFIRM"
	case OpRelease:
		return "RELEASE"
	case OpRefund:
		return "REFUND"
	case OpExpire:
		return "EXPIRE"
	case OpReplay:
		return "REPLAY"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int(t))
	}
}

// Op is the unit of the operation log. All fields are JSON-serializable so an
// Op can be persisted as a log frame and replayed on recovery.
type Op struct {
	Type          OpType            `json:"type"`
	Seq           int64             `json:"seq"`
	RequestID     string            `json:"request_id,omitempty"`
	NotifID       string            `json:"notif_id,omitempty"`
	ReservationID string            `json:"reservation_id,omitempty"`
	CampaignID    string            `json:"campaign_id,omitempty"`
	PeriodID      string            `json:"period_id,omitempty"`
	Channel       string            `json:"channel,omitempty"`
	Amount        Micros            `json:"amount,omitempty"`
	ConfirmAmount Micros           `json:"confirm_amount,omitempty"`
	RefundAmount  Micros           `json:"refund_amount,omitempty"`
	State         ReservationState  `json:"state,omitempty"`
	Now           time.Time         `json:"now"`
	ExpiresAt     time.Time         `json:"expires_at,omitempty"`
	Digest        [32]byte          `json:"digest,omitempty"`
	PrevHash      [32]byte          `json:"prev_hash"`
	Result        *OpResult         `json:"result,omitempty"`
}

// OpResult is the outcome of an operation, stored for idempotent replay and
// returned to callers.
type OpResult struct {
	OK              bool               `json:"ok"`
	Code            string             `json:"code"`
	ReservationID   string             `json:"reservation_id,omitempty"`
	State           ReservationState  `json:"state,omitempty"`
	Amount          Micros            `json:"amount,omitempty"`
	ConfirmedAmount Micros           `json:"confirmed_amount,omitempty"`
	RefundedAmount  Micros           `json:"refunded_amount,omitempty"`
	Seq             int64             `json:"seq,omitempty"`
}

// Stable, machine-readable error codes.
const (
	CodeOK                  = "ok"
	CodeNoBudget            = "no_budget"
	CodePacingLimited       = "pacing_limited"
	CodeQueueFull           = "queue_full"
	CodeDeadlineExceeded    = "deadline_exceeded"
	CodeCanceled            = "canceled"
	CodeIdempotencyConflict = "idempotency_conflict"
	CodeUnknownReservation  = "unknown_reservation"
	CodeInvalidState        = "invalid_state"
	CodeOverConfirm         = "over_confirm"
	CodeOverRefund          = "over_refund"
	CodeUnknownCampaign     = "unknown_campaign"
	CodeUnknownChannel      = "unknown_channel"
	CodeUnknownPeriod       = "unknown_period"
	CodeInvalidRequest      = "invalid_request"
	CodeInternal            = "internal_error"
)

// Error is a domain error carrying a stable machine-readable code.
type Error struct {
	Code string
	Msg  string
}

// Error implements the error interface.
func (e *Error) Error() string { return e.Code + ": " + e.Msg }

// NewError constructs a domain error.
func NewError(code, msg string) *Error { return &Error{Code: code, Msg: msg} }

// CodeOf extracts the stable code from an error, defaulting to CodeInternal.
func CodeOf(err error) string {
	if err == nil {
		return CodeOK
	}
	var de *Error
	if errors.As(err, &de) {
		return de.Code
	}
	return CodeInternal
}

// IsDomainError reports whether err is a domain error with the given code.
func IsDomainError(err error, code string) bool {
	var de *Error
	return errors.As(err, &de) && de.Code == code
}
