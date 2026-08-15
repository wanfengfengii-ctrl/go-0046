package idsource

import (
	"strings"
	"testing"
)

func TestRandomUniqueAndValid(t *testing.T) {
	r := Random{}
	seen := make(map[string]struct{}, 1000)
	for i := 0; i < 1000; i++ {
		id := r.NewID()
		if len(id) > MaxIDLen {
			t.Fatalf("id too long: %d", len(id))
		}
		if !strings.HasPrefix(id, "rsv_") {
			t.Fatalf("id missing prefix: %q", id)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = struct{}{}
	}
}

func TestSeededDeterministic(t *testing.T) {
	a := NewSeeded(42)
	b := NewSeeded(42)
	for i := 0; i < 100; i++ {
		xa, xb := a.NewID(), b.NewID()
		if xa != xb {
			t.Fatalf("seeded source not deterministic at %d: %q vs %q", i, xa, xb)
		}
	}
}

func TestSeededDifferentSeedsDiverge(t *testing.T) {
	a := NewSeeded(1)
	b := NewSeeded(2)
	if a.NewID() == b.NewID() {
		t.Fatal("different seeds produced identical first id")
	}
}

func TestSeededCounterMonotonic(t *testing.T) {
	s := NewSeeded(7)
	prev := uint64(0)
	for i := 0; i < 50; i++ {
		id := s.NewID()
		// Extract the trailing counter to assert strict monotonicity.
		idx := strings.LastIndex(id, "_")
		if idx < 0 {
			t.Fatalf("malformed id %q", id)
		}
		var n uint64
		for _, c := range id[idx+1:] {
			if c < '0' || c > '9' {
				t.Fatalf("non-digit in counter %q", id)
			}
			n = n*10 + uint64(c-'0')
		}
		if n <= prev {
			t.Fatalf("counter not monotonic: %d then %d", prev, n)
		}
		prev = n
	}
}
