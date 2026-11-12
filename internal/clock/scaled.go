package clock

import (
	"math"
	"sync"
	"time"

	"github.com/hami-sh/vm/internal/config"
)

// Scaled keeps deadlines in simulation time, rescheduling wall timers whenever
// the mapping changes. There is no polling goroutine to leak on shutdown.
type Scaled struct {
	mu         sync.Mutex
	base, wall time.Time
	speed, max float64
	paused     bool
	timers     map[*scaledTimer]struct{}
}

func NewScaled(start time.Time, speed float64) *Scaled {
	s := &Scaled{base: start, wall: time.Now(), speed: 1, max: config.Default().MaxSpeed, timers: make(map[*scaledTimer]struct{})}
	s.SetSpeed(speed)
	return s
}
func (s *Scaled) now() time.Time {
	if s.paused {
		return s.base
	}
	return s.base.Add(time.Duration(float64(time.Since(s.wall)) * s.speed))
}
func (s *Scaled) Now() time.Time { s.mu.Lock(); defer s.mu.Unlock(); return s.now() }
func (s *Scaled) Speed() float64 { s.mu.Lock(); defer s.mu.Unlock(); return s.speed }
func (s *Scaled) Paused() bool   { s.mu.Lock(); defer s.mu.Unlock(); return s.paused }
func (s *Scaled) change(fn func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.base, s.wall = s.now(), time.Now()
	fn()
	for t := range s.timers {
		t.arm()
	}
}
func (s *Scaled) SetSpeed(x float64) {
	if math.IsNaN(x) || math.IsInf(x, 0) || x <= 0 {
		return
	}
	s.change(func() { s.speed = min(x, s.max) })
}
func (s *Scaled) Pause() { s.change(func() { s.paused = true }) }
func (s *Scaled) Play()  { s.change(func() { s.paused = false }) }

// Step advances only a paused clock. Due timers fire with their sim deadlines.
func (s *Scaled) Step(d time.Duration) {
	if d < 0 {
		return
	}
	s.change(func() {
		if s.paused {
			s.base = s.base.Add(d)
		}
	})
}
func (s *Scaled) After(d time.Duration) <-chan time.Time { return s.NewTimer(d).C() }
func (s *Scaled) Sleep(d time.Duration)                  { <-s.After(d) }
func (s *Scaled) NewTimer(d time.Duration) Timer         { return s.add(d, 0) }
func (s *Scaled) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		panic("clock: non-positive ticker period")
	}
	return scaledTicker{s.add(d, d)}
}
func (s *Scaled) add(d, p time.Duration) *scaledTimer {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := &scaledTimer{s: s, ch: make(chan time.Time, 1), at: s.now().Add(d), period: p, active: true}
	s.timers[t] = struct{}{}
	t.arm()
	return t
}

type scaledTimer struct {
	s       *Scaled
	ch      chan time.Time
	at      time.Time
	period  time.Duration
	wall    *time.Timer
	version uint64
	active  bool
}

func (t *scaledTimer) arm() {
	t.version++
	if t.wall != nil {
		t.wall.Stop()
	}
	if !t.active {
		return
	}
	now := t.s.now()
	if !t.at.After(now) {
		select {
		case t.ch <- t.at:
		default:
		}
		if t.period == 0 {
			t.active = false
			delete(t.s.timers, t)
			return
		}
		t.at = t.at.Add((now.Sub(t.at)/t.period + 1) * t.period)
	}
	if t.s.paused {
		return
	}
	v := t.version
	d := time.Duration(float64(t.at.Sub(now)) / t.s.speed)
	t.wall = time.AfterFunc(max(d, time.Nanosecond), func() {
		t.s.mu.Lock()
		defer t.s.mu.Unlock()
		if v == t.version {
			t.arm()
		}
	})
}
func (t *scaledTimer) C() <-chan time.Time { return t.ch }
func (t *scaledTimer) Stop() bool {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	was := t.active
	t.active = false
	t.version++
	if t.wall != nil {
		t.wall.Stop()
	}
	delete(t.s.timers, t)
	return was
}
func (t *scaledTimer) Reset(d time.Duration) {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	t.at = t.s.now().Add(d)
	t.active = true
	t.s.timers[t] = struct{}{}
	t.arm()
}

type scaledTicker struct{ t *scaledTimer }

func (k scaledTicker) C() <-chan time.Time { return k.t.C() }
func (k scaledTicker) Stop()               { k.t.Stop() }

var _ Controllable = (*Scaled)(nil)
