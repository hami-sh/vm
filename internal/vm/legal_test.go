package vm

import (
	"testing"

	"github.com/hami-sh/vm/internal/api"
)

func TestLegalMoves(t *testing.T) {
	cases := []struct {
		from, to api.VMState
		want     bool
	}{
		{None, api.VMPending, true},
		{api.VMPending, api.VMBooting, true},
		{api.VMBooting, api.VMReady, true},
		{api.VMReady, api.VMUnhealthy, true},
		{api.VMUnhealthy, api.VMBooting, true},
		{api.VMLost, api.VMPending, true},
		{api.VMTerminating, api.VMTerminated, true},
		// Only a registering guest makes a VM Ready; no shortcut from Pending.
		{api.VMPending, api.VMReady, false},
		// A lost VM is re-placed at gen+1, never adopted back to Ready.
		{api.VMLost, api.VMReady, false},
		// A recycle goes through Unhealthy, keeping the reason in the log.
		{api.VMReady, api.VMBooting, false},
		{api.VMTerminated, api.VMPending, false},
		{api.VMFailed, api.VMPending, false},
		{None, api.VMBooting, false},
	}
	for _, c := range cases {
		if got := Legal(c.from, c.to); got != c.want {
			t.Errorf("Legal(%q, %q) = %v, want %v", c.from, c.to, got, c.want)
		}
	}
}

func TestEveryStateCanReachTerminated(t *testing.T) {
	all := []api.VMState{api.VMPending, api.VMBooting, api.VMReady, api.VMUnhealthy,
		api.VMLost, api.VMTerminating, api.VMFailed}
	for _, s := range all {
		seen := map[api.VMState]bool{s: true}
		frontier := []api.VMState{s}
		for len(frontier) > 0 {
			cur := frontier[0]
			frontier = frontier[1:]
			for _, n := range legal[cur] {
				if !seen[n] {
					seen[n] = true
					frontier = append(frontier, n)
				}
			}
		}
		if !seen[api.VMTerminated] {
			t.Errorf("%s cannot reach Terminated: DELETE would be stuck", s)
		}
	}
}
