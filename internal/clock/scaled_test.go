package clock

import (
	"testing"
	"time"

	"github.com/hami-sh/vm/internal/config"
)

func TestScaledPauseSpeedAndStepPreserveDeadline(t *testing.T) {
	s := NewScaled(t0, 1)
	s.Pause()
	start := s.Now()
	timer := s.NewTimer(9 * time.Second)
	defer timer.Stop()
	s.Step(3 * time.Second)
	s.SetSpeed(17)
	if got := s.Now(); !got.Equal(start.Add(3 * time.Second)) {
		t.Fatalf("speed moved paused time: %v", got)
	}
	if _, ok := recv(timer.C()); ok {
		t.Fatal("early deadline")
	}
	s.Step(5 * time.Second)
	if _, ok := recv(timer.C()); ok {
		t.Fatal("deadline lost elapsed time")
	}
	s.Step(time.Second)
	if got, ok := recv(timer.C()); !ok || !got.Equal(start.Add(9*time.Second)) {
		t.Fatalf("deadline %v %v", got, ok)
	}
}

func TestScaledPlayReschedulesPendingTimer(t *testing.T) {
	s := NewScaled(t0, 1)
	s.Pause()
	start := s.Now()
	timer := s.NewTimer(2 * time.Second)
	defer timer.Stop()
	s.Step(1500 * time.Millisecond)
	s.SetSpeed(config.Default().MaxSpeed)
	s.Play()
	// The real clock is used only in the clock package, including this test's watchdog.
	select {
	case got := <-timer.C():
		if !got.Equal(start.Add(2 * time.Second)) {
			t.Fatalf("wrong sim deadline: %v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timer not rescheduled on play")
	}
	s.Pause()
}

func TestScaledSpeedChangeReschedulesActiveDeadline(t *testing.T) {
	s := NewScaled(t0, 1)
	timer := s.NewTimer(10 * time.Second)
	defer timer.Stop()
	start := s.Now()
	s.SetSpeed(config.Default().MaxSpeed)
	select {
	case got := <-timer.C():
		if delta := got.Sub(start); delta < 9*time.Second || delta > 10*time.Second {
			t.Fatalf("deadline shifted: %s", delta)
		}
	case <-time.After(time.Second):
		t.Fatal("speed change kept old wall deadline")
	}
	s.Pause()
}
