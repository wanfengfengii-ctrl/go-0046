// Package idsource provides injectable unique-identifier generation.
//
// Production code uses Random, backed by crypto/rand. Tests use Seeded, which
// derives identifiers deterministically from a math/rand source so that
// assertions do not depend on goroutine scheduling or wall-clock entropy.
//
// Every identifier lies within the engine's restricted ASCII set
// ([0-9a-zA-Z._-]) and is bounded in length, so generated values always pass
// the protocol identifier validation.
package idsource

import (
	crand "crypto/rand"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"sync"
)

// MaxIDLen is the upper bound on generated identifier length and matches the
// protocol's identifier length cap.
const MaxIDLen = 128

// IDSource mints unique identifiers.
type IDSource interface {
	// NewID returns a fresh identifier. Identifiers are unique within a
	// process for the lifetime of the source; uniqueness across restarts is
	// provided by Random's entropy and by Seeded's monotonic counter when the
	// seed differs.
	NewID() string
}

// Random is the production identifier source.
type Random struct{}

// NewID returns a 32-hex-character identifier prefixed with "rsv_".
func (Random) NewID() string {
	var b [16]byte
	if _, err := crand.Read(b[:]); err != nil {
		// crypto/rand should never fail on a healthy host; a failure here is a
		// fatal environment problem. Fall back to a best-effort value rather
		// than panicking on the hot path, but surface it via an unusual prefix.
		return "rsv_fallback_unavailable"
	}
	return "rsv_" + hex.EncodeToString(b[:])
}

// Seeded is a deterministic identifier source for tests. Given the same seed,
// the sequence of identifiers is fully determined regardless of goroutine
// interleaving because NewID holds a mutex.
type Seeded struct {
	mu      sync.Mutex
	r       *rand.Rand
	counter uint64
	seed    int64
}

// NewSeeded returns a Seeded source initialized from seed.
func NewSeeded(seed int64) *Seeded {
	return &Seeded{r: rand.New(rand.NewPCG(uint64(seed), uint64(seed))), seed: seed}
}

// NewID returns a deterministic identifier of the form "rsv_<hex>_<counter>".
func (s *Seeded) NewID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counter++
	hi := s.r.Uint64()
	return fmt.Sprintf("rsv_%016x_%05d", hi, s.counter)
}

// Seed returns the source's seed, for diagnostics.
func (s *Seeded) Seed() int64 { return s.seed }
