package domain

import (
	"crypto/sha256"
	"encoding/binary"
	"time"
)

// ReserveInput is the domain-level input for a reserve request, decoupled from
// the wire protocol. Its Digest is stored for idempotency replay detection.
type ReserveInput struct {
	RequestID  string
	AuctionID  string
	ImpID      string
	CampaignID string
	Channel    string
	Amount     int64
}

// Digest returns a stable hash of the reserve intent.
func (i ReserveInput) Digest() [32]byte {
	h := sha256.New()
	writeStr(h, i.RequestID)
	writeStr(h, i.AuctionID)
	writeStr(h, i.ImpID)
	writeStr(h, i.CampaignID)
	writeStr(h, i.Channel)
	writeI64(h, i.Amount)
	return finishHash(h)
}

// ConfirmInput is the domain-level input for a confirm notification.
type ConfirmInput struct {
	NotifID       string
	ReservationID string
	ConfirmAmount int64
}

// Digest returns a stable hash of the confirm intent.
func (i ConfirmInput) Digest() [32]byte {
	h := sha256.New()
	writeStr(h, i.NotifID)
	writeStr(h, i.ReservationID)
	writeI64(h, i.ConfirmAmount)
	return finishHash(h)
}

// ReleaseInput is the domain-level input for a release notification.
type ReleaseInput struct {
	NotifID       string
	ReservationID string
}

// Digest returns a stable hash of the release intent.
func (i ReleaseInput) Digest() [32]byte {
	h := sha256.New()
	writeStr(h, i.NotifID)
	writeStr(h, i.ReservationID)
	return finishHash(h)
}

// RefundInput is the domain-level input for a refund notification.
type RefundInput struct {
	NotifID       string
	ReservationID string
	RefundAmount  int64
}

// Digest returns a stable hash of the refund intent.
func (i RefundInput) Digest() [32]byte {
	h := sha256.New()
	writeStr(h, i.NotifID)
	writeStr(h, i.ReservationID)
	writeI64(h, i.RefundAmount)
	return finishHash(h)
}

func writeStr(h interface{ Write([]byte) (int, error) }, s string) {
	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(s)))
	h.Write(lenBuf[:])
	h.Write([]byte(s))
}

func writeI64(h interface{ Write([]byte) (int, error) }, n int64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(n))
	h.Write(buf[:])
}

func finishHash(h interface{ Sum([]byte) []byte }) [32]byte {
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// ReserveInputAt captures a reserve input together with the decision timestamp
// used for pacing. It is a convenience type for the executor.
type ReserveInputAt struct {
	ReserveInput
	Now time.Time
	Tmax time.Duration
}
