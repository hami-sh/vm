package scenario

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/config"
)

var errScriptedUnavailable = errors.New("scripted action could not reach scheduler")

type scriptDeadlineKey struct{}

func scriptContext(ctx context.Context, speed float64) context.Context {
	if ctx.Value(scriptDeadlineKey{}) != nil {
		return ctx
	}
	cfg := config.Default()
	return context.WithValue(ctx, scriptDeadlineKey{}, WallClock.Now().Add(time.Duration(float64(cfg.AmbientHealAfter+cfg.BootDeadline)/speed)))
}

// Selection and the subsequent calls share one action budget. Re-read the
// published view while unavailable; never choose targets from a frozen outage.
func selectTargets(ctx context.Context, f System, ready func(api.Snapshot) bool, action func(context.Context, api.Snapshot) error) error {
	c, ok := f.(*scriptedClient)
	if !ok {
		c = &scriptedClient{System: f, speed: 1}
	}
	ctx = scriptContext(ctx, c.speed)
	s, err := retryCall(ctx, c, config.Default().SnapshotEvery, func() (api.Snapshot, error) {
		s := f.Latest()
		if s.Scheduler != api.SchedulerUp || !ready(s) {
			return s, &api.Error{Status: 503, Msg: "target selection awaits an up scheduler and eligible candidates"}
		}
		return s, nil
	})
	if err != nil {
		return err
	}
	return action(ctx, s)
}

// Scripted calls retry individual rejected requests, not whole multi-call
// actions. Successful mutations must not be replayed when a later call fails.
type scriptedClient struct {
	System
	speed    float64
	mu       sync.Mutex // duplicate-create issues concurrent scripted calls
	failures []api.CheckResult
}

func (c *scriptedClient) unavailable(err error) error {
	c.mu.Lock()
	c.failures = append(c.failures, api.CheckResult{Name: errScriptedUnavailable.Error(), Detail: err.Error()})
	c.mu.Unlock()
	return fmt.Errorf("%w: %v", errScriptedUnavailable, err)
}

func scriptCall[T any](ctx context.Context, c *scriptedClient, call func() (T, error)) (T, error) {
	return retryCall(ctx, c, config.Default().HostHeartbeat, call)
}

func retryCall[T any](ctx context.Context, c *scriptedClient, interval time.Duration, call func() (T, error)) (T, error) {
	deadline := scriptContext(ctx, c.speed).Value(scriptDeadlineKey{}).(time.Time)
	for {
		value, err := call()
		var rejected *api.Error
		if !errors.As(err, &rejected) || (rejected.Status != 503 && rejected.Status != 429) {
			return value, err
		}
		remaining := deadline.Sub(WallClock.Now())
		if remaining <= 0 {
			return value, c.unavailable(err)
		}
		wait := interval
		if rejected.RetryAfterS > 0 {
			wait = time.Duration(rejected.RetryAfterS) * time.Second
		}
		// API durations and the retry budget are on the simulation clock.
		wait = time.Duration(float64(wait) / c.speed)
		timer := WallClock.NewTimer(min(wait, remaining))
		select {
		case <-ctx.Done():
			timer.Stop()
			return value, ctx.Err()
		case <-timer.C():
		}
		if wait > remaining || WallClock.Now().After(deadline) {
			return value, c.unavailable(err)
		}
	}
}

func (c *scriptedClient) CreateVM(ctx context.Context, key string, req api.CreateVMRequest) (api.CreateVMResponse, error) {
	return scriptCall(ctx, c, func() (api.CreateVMResponse, error) { return c.System.CreateVM(ctx, key, req) })
}
func (c *scriptedClient) DeleteVM(ctx context.Context, id string) (api.VM, error) {
	return scriptCall(ctx, c, func() (api.VM, error) { return c.System.DeleteVM(ctx, id) })
}
func (c *scriptedClient) GetVM(ctx context.Context, id string) (api.VM, error) {
	return scriptCall(ctx, c, func() (api.VM, error) { return c.System.GetVM(ctx, id) })
}
func (c *scriptedClient) Do(ctx context.Context, action string, req api.ChaosRequest) (api.ChaosResult, error) {
	return scriptCall(ctx, c, func() (api.ChaosResult, error) { return c.System.Do(ctx, action, req) })
}
func (c *scriptedClient) Sim(ctx context.Context, action string, req api.SimRequest) (api.SimStatus, error) {
	return scriptCall(ctx, c, func() (api.SimStatus, error) { return c.System.Sim(ctx, action, req) })
}
func (c *scriptedClient) Topology(ctx context.Context, action string, req api.TopologyRequest) (api.ChaosResult, error) {
	return scriptCall(ctx, c, func() (api.ChaosResult, error) { return c.System.Topology(ctx, action, req) })
}
