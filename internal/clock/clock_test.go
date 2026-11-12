package clock

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func recv(c <-chan time.Time) (time.Time, bool) {
	select {
	case v := <-c:
		return v, true
	default:
		return time.Time{}, false
	}
}

func TestFakeTimerFiresAtDeadline(t *testing.T) {
	f := NewFake(t0)
	a := f.After(3 * time.Second)
	f.Advance(2999 * time.Millisecond)
	if _, ok := recv(a); ok {
		t.Fatal("fired early")
	}
	f.Advance(time.Millisecond)
	got, ok := recv(a)
	if !ok || !got.Equal(t0.Add(3*time.Second)) {
		t.Fatalf("got %v %v, want fire at +3s", got, ok)
	}
}

func TestFakeTickerAndStop(t *testing.T) {
	f := NewFake(t0)
	k := f.NewTicker(2 * time.Second)
	f.Advance(2 * time.Second)
	if v, ok := recv(k.C()); !ok || !v.Equal(t0.Add(2*time.Second)) {
		t.Fatalf("tick 1: %v %v", v, ok)
	}
	f.Advance(2 * time.Second)
	if v, ok := recv(k.C()); !ok || !v.Equal(t0.Add(4*time.Second)) {
		t.Fatalf("tick 2: %v %v", v, ok)
	}
	k.Stop()
	f.Advance(10 * time.Second)
	if _, ok := recv(k.C()); ok {
		t.Fatal("ticked after Stop")
	}
	if f.Pending() != 0 {
		t.Fatalf("pending=%d after stop", f.Pending())
	}
}

func TestFakeResetMovesDeadline(t *testing.T) {
	f := NewFake(t0)
	tm := f.NewTimer(5 * time.Second)
	f.Advance(4 * time.Second)
	tm.Reset(5 * time.Second) // now due at +9s
	f.Advance(4 * time.Second)
	if _, ok := recv(tm.C()); ok {
		t.Fatal("fired at old deadline")
	}
	f.Advance(time.Second)
	if v, ok := recv(tm.C()); !ok || !v.Equal(t0.Add(9*time.Second)) {
		t.Fatalf("got %v %v", v, ok)
	}
}
