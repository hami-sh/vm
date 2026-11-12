package api

import (
	"context"
	"fmt"
)

// These interfaces are how the outside (HTTP handlers, the scenario runner,
// tests) reaches the running system. Implementations send messages to actors;
// callers never touch component state.
//
//	ControlPlane   implemented by internal/scheduler (W1)
//	ChaosEngine    implemented by internal/chaos (W3)
//	SnapshotSource implemented by internal/fleet (W3), which assembles the
//	               scheduler, agents, simnet, drivers, clock, chaos engine and
//	               invariant checker into one running fleet.
//
// W4 serves HTTP against these with fakes until integration; W5's runner
// drives a fleet.Fleet through them.

// ControlPlane is the public VM API.
type ControlPlane interface {
	// CreateVM is idempotent on key. Errors are *Error: 422 (same key,
	// different body), 429 (queue full, RetryAfterS set), 503 (scheduler down).
	CreateVM(ctx context.Context, key string, req CreateVMRequest) (CreateVMResponse, error)
	GetVM(ctx context.Context, id string) (VM, error)
	ListVMs(ctx context.Context) ([]VM, error)
	// DeleteVM is idempotent: deleting a terminal VM returns it unchanged.
	DeleteVM(ctx context.Context, id string) (VM, error)
	ListHosts(ctx context.Context) ([]Host, error)
	Queue(ctx context.Context) ([]QueueEntry, error)
	// SetPriority moves a queued request between classes 1 and 3.
	SetPriority(ctx context.Context, vmID string, p Priority) error
}

// ChaosEngine runs chaos actions, presets, sim controls and topology changes.
type ChaosEngine interface {
	Catalog() ChaosCatalog
	Do(ctx context.Context, action string, req ChaosRequest) (ChaosResult, error)
	// Sim handles pause, play, step, speed and chaos settings (intensity,
	// filters, load rate, seed).
	Sim(ctx context.Context, action string, req SimRequest) (SimStatus, error)
	// Topology handles add-rack, add-host and drain.
	Topology(ctx context.Context, action string, req TopologyRequest) (ChaosResult, error)
}

// SnapshotSource publishes the state the dashboard renders and runs record.
type SnapshotSource interface {
	Latest() Snapshot
	// Subscribe streams every published snapshot (every config.SnapshotEvery
	// of sim time) and every event. Slow subscribers miss values rather than
	// block the fleet. cancel releases the subscription.
	Subscribe() (snapshots <-chan Snapshot, events <-chan Event, cancel func())
}

// Error is an API error with its HTTP status.
type Error struct {
	Status      int
	Msg         string
	RetryAfterS int
}

func (e *Error) Error() string { return fmt.Sprintf("%d: %s", e.Status, e.Msg) }
