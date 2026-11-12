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
	cfg := config.Default()
	deadline := WallClock.Now().Add(time.Duration(float64(cfg.AmbientHealAfter+cfg.BootDeadline) / c.speed))
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
		wait := cfg.HostHeartbeat
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
