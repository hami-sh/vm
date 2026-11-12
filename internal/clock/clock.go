// Package clock is the only source of time. No component calls time.Now,
// time.After or time.Sleep directly: the demo clock can be sped up and paused,
// and tests drive a fake one.
package clock

import (
	"sort"
	"sync"
	"time"
)

// Clock reads sim time and creates timers on it.
type Clock interface {
	Now() time.Time
	// After fires once d of clock time has passed. On a paused clock it
	// does not fire until play.
	After(d time.Duration) <-chan time.Time
	NewTimer(d time.Duration) Timer
	NewTicker(d time.Duration) Ticker
	// Sleep blocks for d of clock time. Use only in worker goroutines.
	Sleep(d time.Duration)
}

// Timer is a one-shot timer on a Clock.
type Timer interface {
	C() <-chan time.Time
	// Stop prevents the timer firing; it reports whether it was pending.
	Stop() bool
	Reset(d time.Duration)
}

// Ticker fires every period of clock time.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// Controllable is a clock the dashboard can pause and speed up. The scaled
// demo clock (W3) and Fake implement it.
type Controllable interface {
	Clock
	Speed() float64
	SetSpeed(x float64)
	Paused() bool
	Pause()
	Play()
}

// UnixMs is the format every persisted timestamp uses.
func UnixMs(c Clock) int64 { return c.Now().UnixMilli() }

// ------------------------------------------------------------------- Real

// Real is wall-clock time.
type Real struct{}

func (Real) Now() time.Time                         { return time.Now() }
func (Real) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (Real) Sleep(d time.Duration)                  { time.Sleep(d) }
func (Real) NewTimer(d time.Duration) Timer         { return realTimer{time.NewTimer(d)} }
func (Real) NewTicker(d time.Duration) Ticker       { return realTicker{time.NewTicker(d)} }

type realTimer struct{ t *time.Timer }

func (r realTimer) C() <-chan time.Time   { return r.t.C }
func (r realTimer) Stop() bool            { return r.t.Stop() }
func (r realTimer) Reset(d time.Duration) { r.t.Reset(d) }

type realTicker struct{ t *time.Ticker }

func (r realTicker) C() <-chan time.Time { return r.t.C }
func (r realTicker) Stop()               { r.t.Stop() }

// ------------------------------------------------------------------- Fake

// Fake only moves when Advance is called. Timers fire in deadline order, each
// seeing Now() equal to its own deadline. Channels are buffered (1), so a
// timer whose receiver is slow never blocks Advance; like time.Ticker, a
// ticker drops ticks its receiver missed.
type Fake struct {
	mu     sync.Mutex
	now    time.Time
	seq    uint64
	timers []*fakeTimer
}

// NewFake starts at start (use a fixed instant for reproducible tests).
func NewFake(start time.Time) *Fake { return &Fake{now: start} }

func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Advance moves time forward by d, firing every timer due on the way.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	end := f.now.Add(d)
	for {
		t := f.nextDueLocked(end)
		if t == nil {
			break
		}
		f.now = t.at
		select {
		case t.ch <- t.at:
		default:
		}
		if t.period > 0 {
			t.at = t.at.Add(t.period)
			f.seq++
			t.seq = f.seq
		} else {
			t.active = false
			f.removeLocked(t)
		}
	}
	f.now = end
	f.mu.Unlock()
}

// Pending reports how many timers are waiting; useful to wait until an actor
// has armed its timer before advancing.
func (f *Fake) Pending() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.timers)
}

func (f *Fake) nextDueLocked(end time.Time) *fakeTimer {
	if len(f.timers) == 0 {
		return nil
	}
	sort.Slice(f.timers, func(i, j int) bool {
		a, b := f.timers[i], f.timers[j]
		if !a.at.Equal(b.at) {
			return a.at.Before(b.at)
		}
		return a.seq < b.seq
	})
	if t := f.timers[0]; !t.at.After(end) {
		return t
	}
	return nil
}

func (f *Fake) removeLocked(t *fakeTimer) {
	for i, x := range f.timers {
		if x == t {
			f.timers = append(f.timers[:i], f.timers[i+1:]...)
			return
		}
	}
}

func (f *Fake) add(d, period time.Duration) *fakeTimer {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	t := &fakeTimer{f: f, ch: make(chan time.Time, 1), at: f.now.Add(d), period: period, seq: f.seq, active: true}
	f.timers = append(f.timers, t)
	return t
}

func (f *Fake) After(d time.Duration) <-chan time.Time { return f.add(d, 0).ch }
func (f *Fake) NewTimer(d time.Duration) Timer         { return f.add(d, 0) }
func (f *Fake) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		panic("clock: non-positive ticker period")
	}
	return fakeTicker{f.add(d, d)}
}

// Sleep blocks until another goroutine advances the clock past d.
func (f *Fake) Sleep(d time.Duration) { <-f.After(d) }

type fakeTimer struct {
	f      *Fake
	ch     chan time.Time
	at     time.Time
	period time.Duration
	seq    uint64
	active bool
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }

func (t *fakeTimer) Stop() bool {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	was := t.active
	t.active = false
	t.f.removeLocked(t)
	return was
}

func (t *fakeTimer) Reset(d time.Duration) {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	t.f.removeLocked(t)
	t.f.seq++
	t.at, t.seq, t.active = t.f.now.Add(d), t.f.seq, true
	t.f.timers = append(t.f.timers, t)
}

type fakeTicker struct{ t *fakeTimer }

func (k fakeTicker) C() <-chan time.Time { return k.t.ch }
func (k fakeTicker) Stop()               { k.t.Stop() }
