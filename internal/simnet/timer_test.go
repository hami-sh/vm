package simnet

import (
	"testing"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
)

type timerGate struct {
	*clock.Fake
	armed  chan struct{}
	resume chan struct{}
}

func (c *timerGate) NewTimer(d time.Duration) clock.Timer {
	close(c.armed)
	<-c.resume
	return c.Fake.NewTimer(d)
}

func TestDeliveryWhenClockAdvancesWhileArmingTimer(t *testing.T) {
	c := &timerGate{Fake: clock.NewFake(time.Unix(100, 0)), armed: make(chan struct{}), resume: make(chan struct{})}
	n := New(c, config.Default(), 22)
	defer n.Close()
	s, err := n.Attach(api.Addr{Kind: api.KindScheduler, ID: "scheduler"})
	if err != nil {
		t.Fatal(err)
	}
	h, err := n.Attach(api.Addr{Kind: api.KindAgent, ID: "h1", Host: "h1"})
	if err != nil {
		t.Fatal(err)
	}
	n.SetLatency(time.Second)
	if err := s.Send(h.Addr(), api.PortControl, "payload"); err != nil {
		t.Fatal(err)
	}
	<-c.armed
	// Remove Send's wake, if still queued, so it cannot mask the timer race.
	select {
	case <-n.wake:
	default:
	}
	c.Advance(time.Second)
	close(c.resume)
	deadline := clock.Real{}.NewTimer(time.Second)
	defer deadline.Stop()
	select {
	case e := <-h.Inbox():
		if e.Msg != "payload" {
			t.Fatalf("wrong payload: %+v", e)
		}
	case <-deadline.C():
		t.Fatal("due delivery stranded while arming timer")
	}
}
