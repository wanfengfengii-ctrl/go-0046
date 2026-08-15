package domain

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// EncodeOp serializes an op to canonical JSON. Because Op is a struct (not a
// map), json.Encoder emits fields in declaration order, making the output
// deterministic and the chain hash stable.
func EncodeOp(op *Op) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	if err := enc.Encode(op); err != nil {
		return nil, fmt.Errorf("encode op: %w", err)
	}
	// json.Encoder.Encode appends a trailing newline; trim it so the hash is
	// stable regardless of encoder settings.
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// DecodeOp parses an op from JSON.
func DecodeOp(data []byte) (*Op, error) {
	var op Op
	if err := json.Unmarshal(data, &op); err != nil {
		return nil, fmt.Errorf("decode op: %w", err)
	}
	return &op, nil
}

// EncodeState serializes a full state snapshot for a checkpoint. State contains
// only struct fields and maps of structs; struct field order is deterministic,
// and map key order does not affect the chain (the checkpoint carries its own
// SummaryHash for integrity).
func EncodeState(s *State) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	if err := enc.Encode(s); err != nil {
		return nil, fmt.Errorf("encode state: %w", err)
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// DecodeState parses a state snapshot from JSON.
func DecodeState(data []byte) (*State, error) {
	var s State
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("decode state: %w", err)
	}
	if s.Ledgers == nil {
		s.Ledgers = make(map[LedgerKey]*Ledger)
	}
	if s.Reservations == nil {
		s.Reservations = make(map[string]*Reservation)
	}
	if s.Idempotency == nil {
		s.Idempotency = make(map[string]*IdemRecord)
	}
	return &s, nil
}
