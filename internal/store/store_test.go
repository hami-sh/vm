package store

import (
	"context"
	"errors"
	"maps"
	"path/filepath"
	"testing"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/config"
)

func TestAtomicGenerationCASAndQueue(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	empty, _, err := s.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	before := Data{Epoch: empty.Epoch, VMs: map[string]Record{"v": {VM: api.VM{ID: "v", Pool: config.Pools[0].Name, State: api.VMPending, Desired: api.DesiredRunning, Priority: 2}, Key: "key", Hash: "hash"}}, Hosts: map[string]HostRecord{"h": {ID: "h", Rack: "r", Slots: 2, Admin: api.AdminActive}}, Queue: map[string]api.QueueEntry{"v": {VMID: "v", Priority: 2, Reason: api.QueueNew}}}
	if err = s.Commit(ctx, empty, before, nil, config.Default().EventRetention); err != nil {
		t.Fatal(err)
	}
	after := Data{Epoch: before.Epoch, VMs: maps.Clone(before.VMs), Hosts: before.Hosts, Queue: map[string]api.QueueEntry{}}
	r := after.VMs["v"]
	r.VM.Generation = 1
	r.VM.State = api.VMBooting
	r.VM.HostID = "h"
	after.VMs["v"] = r
	event := api.Event{Kind: api.EventTransition, VMID: "v", From: api.VMPending, To: api.VMBooting, Generation: 1}
	// The second writer has the same old generation. It must not dequeue or
	// append another placement event when the CAS loses.
	if err = s.Commit(ctx, before, after, []api.Event{event}, config.Default().EventRetention); err != nil {
		t.Fatal(err)
	}
	if err = s.Commit(ctx, before, after, []api.Event{event}, config.Default().EventRetention); !errors.Is(err, ErrCAS) {
		t.Fatalf("stale writer accepted: %v", err)
	}
	loaded, _, err := s.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.VMs["v"].VM.Generation != 1 || loaded.VMs["v"].VM.State != api.VMBooting || len(loaded.Queue) != 0 {
		t.Fatal("placement was not atomic")
	}
	events, err := s.Events(ctx)
	if err != nil || len(events) != 1 {
		t.Fatalf("failed CAS appended event: %+v %v", events, err)
	}

	// A bad event must roll back the VM, queue, and all previous writes in
	// this transaction, not merely reject that event row.
	deleted := Data{Epoch: loaded.Epoch, VMs: maps.Clone(loaded.VMs), Hosts: loaded.Hosts, Queue: loaded.Queue}
	r = deleted.VMs["v"]
	r.VM.Desired = api.DesiredDeleted
	r.VM.State = api.VMTerminating
	deleted.VMs["v"] = r
	if err = s.Commit(ctx, loaded, deleted, []api.Event{{Kind: "invalid"}}, config.Default().EventRetention); err == nil {
		t.Fatal("invalid audit row accepted")
	}
	restored, _, err := s.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if restored.VMs["v"].VM.Desired != api.DesiredRunning || restored.VMs["v"].VM.State != api.VMBooting {
		t.Fatal("failed transaction leaked intent")
	}
}

func TestEpochAndFreshnessAcrossReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	_, epoch, err := s.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if epoch != 1 || !s.Fresh() {
		t.Fatal("fresh epoch not 1")
	}
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_, next, err := s.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if next != epoch+1 || s.Fresh() {
		t.Fatalf("restart epoch=%d fresh=%v", next, s.Fresh())
	}
	// Further starts increment persisted metadata without consulting time.
	_, next, err = s.Start(ctx)
	if err != nil || next != epoch+2 {
		t.Fatalf("epoch rollback: %d %v", next, err)
	}
}
