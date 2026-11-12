package fleet

import (
	"context"
	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
)

// This is deliberately not a second scheduler. Until W1 lands, writes fail
// explicitly and hosts can still be assembled, observed and fault-injected.
type standIn struct{}

func newStandIn(ctx context.Context, cfg config.Config, c clock.Clock, n api.Transport, path string) (Scheduler, func(), error) {
	ep, err := n.Attach(schedulerAddr)
	if err != nil {
		return nil, nil, err
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ep.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-ep.Inbox():
				if !ok {
					return
				}
			}
		}
	}()
	return standIn{}, func() { <-done }, nil
}
func (standIn) CreateVM(context.Context, string, api.CreateVMRequest) (api.CreateVMResponse, error) {
	return api.CreateVMResponse{}, &api.Error{Status: 503, Msg: "scheduler not integrated"}
}
func (standIn) GetVM(context.Context, string) (api.VM, error) {
	return api.VM{}, &api.Error{Status: 404, Msg: "scheduler not integrated"}
}
func (standIn) ListVMs(context.Context) ([]api.VM, error) { return []api.VM{}, nil }
func (standIn) DeleteVM(context.Context, string) (api.VM, error) {
	return api.VM{}, &api.Error{Status: 503, Msg: "scheduler not integrated"}
}
func (standIn) ListHosts(context.Context) ([]api.Host, error)   { return []api.Host{}, nil }
func (standIn) Queue(context.Context) ([]api.QueueEntry, error) { return []api.QueueEntry{}, nil }
func (standIn) SetPriority(context.Context, string, api.Priority) error {
	return &api.Error{Status: 503, Msg: "scheduler not integrated"}
}
func (standIn) Snapshot(context.Context) (api.Snapshot, error) {
	return api.Snapshot{Scheduler: api.SchedulerDown}, nil
}
func (standIn) SetDown(context.Context, bool) error                           { return nil }
func (standIn) SetHostAdmin(context.Context, string, api.AdminState) error    { return nil }
func (standIn) EventsSince(context.Context, int64) ([]api.Event, bool, error) { return nil, false, nil }
