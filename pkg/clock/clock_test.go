package clock

import (
	"testing"
	"time"
)

func TestRealSmoke(t *testing.T) {
	r := Real{}
	if r.Now().IsZero() {
		t.Fatal("real clock returned zero time")
	}
	select {
	case <-r.After(time.Millisecond):
	case <-time.After(time.Second):
		t.Fatal("real.After did not fire within 1s")
	}
}

func TestManualNowAndAdvance(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	m := NewManual(start)
	if !m.Now().Equal(start) {
		t.Fatalf("now = %v, want %v", m.Now(), start)
	}
	mid := start.Add(30 * time.Minute)
	m.Advance(mid)
	if !m.Now().Equal(mid) {
		t.Fatalf("now = %v, want %v", m.Now(), mid)
	}
	// Advancing backward is a no-op.
	m.Advance(start)
	if !m.Now().Equal(mid) {
		t.Fatalf("backward advance moved clock to %v", m.Now())
	}
}

func TestManualAfterNonPositiveFiresImmediately(t *testing.T) {
	m := NewManual(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	select {
	case <-m.After(0):
	default:
		t.Fatal("After(0) should fire immediately")
	}
	select {
	case <-m.After(-time.Second):
	default:
		t.Fatal("After(-d) should fire immediately")
	}
}

func TestManualAfterFiresOnAdvance(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	m := NewManual(start)
	ch := m.After(time.Minute)
	select {
	case <-ch:
		t.Fatal("channel fired before advance")
	default:
	}
	m.Advance(start.Add(30 * time.Second))
	select {
	case <-ch:
		t.Fatal("channel fired before deadline")
	default:
	}
	m.Advance(start.Add(time.Minute))
	select {
	case got := <-ch:
		if !got.Equal(start.Add(time.Minute)) {
			t.Fatalf("received %v, want %v", got, start.Add(time.Minute))
		}
	case <-time.After(time.Second):
		t.Fatal("channel did not fire after advancing past deadline")
	}
}

func TestManualMultipleWaitersFiredInOrder(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	m := NewManual(start)
	// Register out of deadline order.
	long := m.After(2 * time.Minute)
	short := m.After(time.Minute)
	m.Advance(start.Add(90 * time.Second))
	select {
	case <-short:
	default:
		t.Fatal("short waiter did not fire")
	}
	select {
	case <-long:
		t.Fatal("long waiter fired too early")
	default:
	}
	m.Advance(start.Add(3 * time.Minute))
	select {
	case <-long:
	default:
		t.Fatal("long waiter did not fire")
	}
}
