package domain

import (
	"math"
	"math/big"
)

// PacingAllowed computes the cumulative occupancy a period permits at a given
// elapsed time, using pure integer arithmetic.
//
// The curve is linear with a configurable burst:
//
//	allowed = burst + (limit - burst) * elapsed / duration   (floored)
//
// clamped to [0, limit]. At elapsed <= 0 the allowed occupancy is the burst;
// at elapsed >= duration it is the full limit. The multiplication is performed
// with arbitrary precision to avoid int64 overflow for large budgets or long
// durations, then floored back to int64. Because the computation uses only
// integers, it is free of floating-point error and is stable across runs and
// platforms.
//
// duration is expressed in the same units as elapsed (nanoseconds in practice).
func PacingAllowed(limit, burst, elapsed, duration int64) int64 {
	if limit <= 0 {
		return 0
	}
	if burst < 0 {
		burst = 0
	}
	if burst > limit {
		burst = limit
	}
	if duration <= 0 {
		return limit
	}
	if elapsed <= 0 {
		return burst
	}
	if elapsed >= duration {
		return limit
	}
	span := limit - burst
	added := mulDiv(span, elapsed, duration)
	allowed := burst + added
	if allowed > limit {
		allowed = limit
	}
	if allowed < 0 {
		allowed = 0
	}
	return allowed
}

// mulDiv computes (a*b)/c with arbitrary precision and floors the result to
// int64. Inputs are assumed non-negative with c > 0; results that do not fit
// in int64 saturate to math.MaxInt64.
func mulDiv(a, b, c int64) int64 {
	if a == 0 || b == 0 {
		return 0
	}
	res := new(big.Int).Mul(big.NewInt(a), big.NewInt(b))
	res.Quo(res, big.NewInt(c))
	if res.IsInt64() {
		return res.Int64()
	}
	return math.MaxInt64
}

// PeriodOccupied returns the net occupancy of a period ledger: pending spend
// plus confirmed spend minus refunds. Pacing compares this against
// PacingAllowed.
func PeriodOccupied(pending, confirmed, refunded int64) int64 {
	return pending + confirmed - refunded
}

// PacingPermits reports whether reserving amount at the given occupancy and
// pacing water level is allowed. It returns false (without mutating anything)
// when occupancy+amount would exceed the allowed water level.
func PacingPermits(occupied, amount, allowed int64) bool {
	if amount <= 0 {
		return true
	}
	if occupied < 0 {
		occupied = 0
	}
	return occupied+amount <= allowed
}
