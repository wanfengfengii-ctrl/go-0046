package domain

import (
	"math/big"
	"testing"
)

func TestPacingAllowedBoundaries(t *testing.T) {
	cases := []struct {
		limit, burst, elapsed, duration, want int64
	}{
		// Linear from 0 burst to 100 limit over 100 ns.
		{limit: 100, burst: 0, elapsed: 0, duration: 100, want: 0},
		{limit: 100, burst: 0, elapsed: 100, duration: 100, want: 100},
		{limit: 100, burst: 0, elapsed: 50, duration: 100, want: 50},
		// Non-divisible: floor.
		{limit: 100, burst: 0, elapsed: 1, duration: 3, want: 33},
		{limit: 100, burst: 0, elapsed: 2, duration: 3, want: 66},
		// Burst front-loads the curve.
		{limit: 100, burst: 30, elapsed: 0, duration: 100, want: 30},
		{limit: 100, burst: 30, elapsed: 100, duration: 100, want: 100},
		{limit: 100, burst: 30, elapsed: 50, duration: 100, want: 65},
		// Burst >= limit clamps to limit.
		{limit: 100, burst: 150, elapsed: 0, duration: 100, want: 100},
		// Degenerate duration.
		{limit: 100, burst: 10, elapsed: 5, duration: 0, want: 100},
		// Beyond duration clamps to limit.
		{limit: 100, burst: 10, elapsed: 999, duration: 100, want: 100},
		// Negative elapsed → burst.
		{limit: 100, burst: 10, elapsed: -5, duration: 100, want: 10},
		// Zero limit.
		{limit: 0, burst: 5, elapsed: 10, duration: 100, want: 0},
	}
	for _, tc := range cases {
		got := PacingAllowed(tc.limit, tc.burst, tc.elapsed, tc.duration)
		if got != tc.want {
			t.Errorf("PacingAllowed(%d,%d,%d,%d) = %d, want %d",
				tc.limit, tc.burst, tc.elapsed, tc.duration, got, tc.want)
		}
	}
}

func TestPacingAllowedMatchesBigIntFormula(t *testing.T) {
	// Verify the implementation matches burst + (limit-burst)*elapsed/duration
	// floored with arbitrary precision, across a range that would overflow int64
	// if naively multiplied.
	const limit int64 = 9_000_000_000_000_000 // 9e15 micros (~9 billion)
	const burst int64 = 1_000_000_000
	const duration int64 = 3_600_000_000_000 // 1h in ns
	for _, elapsed := range []int64{0, 1, 1_000, 1_000_000, 1_800_000_000_000, duration - 1, duration} {
		got := PacingAllowed(limit, burst, elapsed, duration)
		want := bigPacing(limit, burst, elapsed, duration)
		if got != want {
			t.Fatalf("elapsed=%d: got %d, want %d (big)", elapsed, got, want)
		}
	}
}

func bigPacing(limit, burst, elapsed, duration int64) int64 {
	if burst > limit {
		burst = limit
	}
	span := new(big.Int).SetInt64(limit - burst)
	res := new(big.Int).Mul(span, new(big.Int).SetInt64(elapsed))
	res.Quo(res, new(big.Int).SetInt64(duration))
	res.Add(res, new(big.Int).SetInt64(burst))
	if res.Cmp(new(big.Int).SetInt64(limit)) > 0 {
		return limit
	}
	return res.Int64()
}

func TestPacingPermits(t *testing.T) {
	if !PacingPermits(0, 10, 10) {
		t.Fatal("0+10 <= 10 should permit")
	}
	if PacingPermits(5, 10, 10) {
		t.Fatal("5+10 <= 10 should not permit")
	}
	if !PacingPermits(5, 10, 15) {
		t.Fatal("5+10 <= 15 should permit")
	}
	if !PacingPermits(0, 0, 0) {
		t.Fatal("zero amount should permit")
	}
}
