// Package config holds every timing and limit, in sim time. Real-world values
// are in comments and in the README. Change a value only with a note in
// COORDINATION.md: loosening a threshold is a decision, not a fix.
package config

import "time"

type Config struct {
	// Fleet
	Racks        int
	HostsPerRack int
	SlotsPerHost int // Apple's licence: 2 macOS VMs per host

	// Leases (real: 10-30 s TTLs)
	GuestHeartbeat time.Duration
	GuestLeaseTTL  time.Duration
	HostHeartbeat  time.Duration
	HostLeaseTTL   time.Duration // healthy -> suspect
	HostLostGrace  time.Duration // suspect -> lost

	// Boots (real: 30-60 s)
	BootMin, BootMax time.Duration
	BootDeadline     time.Duration
	MaxAttempts      int
	BackoffBase      time.Duration // exponential with full jitter
	BackoffMax       time.Duration
	ReservationTTL   time.Duration
	// DrainGrace is how long VMs on a draining host keep running before they
	// are replaced elsewhere (class 0, reason drained). Real: hours.
	DrainGrace time.Duration

	// Queue
	QueueCap  int
	QueueScan int           // dequeue looks at the first N eligible entries
	AgingStep time.Duration // effective class = max(1, class - wait/AgingStep)

	// Evidence
	SnapshotEvery      time.Duration // sim time between published snapshots
	SSEEvery           time.Duration // wall time between SSE snapshots
	InvariantEvery     time.Duration
	PendingPlaceWithin time.Duration // invariant 8
	EventRetention     int           // events rows kept in SQLite
	RecentEventLimit   int           // events carried per snapshot

	// Security
	QuarantineAttempts int
	QuarantineWindow   time.Duration
	GuestMsgPerSec     int // per-VM rate limit at the agent
	GuestMsgMaxBytes   int

	// Public URL hardening
	MaxVMs         int
	ChaosPerIP     time.Duration
	IdleResetAfter time.Duration // wall time with no chaos before auto-reset

	// Demo
	DefaultSpeed float64
	MaxSpeed     float64

	// Chaos rates are attempts per simulation second at intensity 1.
	AmbientEvery    time.Duration
	ChaosBaseRates  map[string]float64
	AttackerEvery   time.Duration
	PresetHealAfter time.Duration
	PresetStepEvery time.Duration
	LoadPriorityMix [3]float64 // interactive, standard, batch weights
}

// Default is the configuration the demo and the scenarios use.
func Default() Config {
	return Config{
		Racks: 3, HostsPerRack: 4, SlotsPerHost: 2,

		GuestHeartbeat: 2 * time.Second,
		GuestLeaseTTL:  6 * time.Second,
		HostHeartbeat:  2 * time.Second,
		HostLeaseTTL:   6 * time.Second,
		HostLostGrace:  6 * time.Second,

		BootMin: 3 * time.Second, BootMax: 6 * time.Second,
		BootDeadline:   15 * time.Second,
		MaxAttempts:    3,
		BackoffBase:    time.Second,
		BackoffMax:     8 * time.Second,
		ReservationTTL: 10 * time.Second,
		DrainGrace:     20 * time.Second,

		QueueCap: 32, QueueScan: 8, AgingStep: 60 * time.Second,

		SnapshotEvery:      100 * time.Millisecond,
		SSEEvery:           500 * time.Millisecond,
		InvariantEvery:     100 * time.Millisecond,
		PendingPlaceWithin: 2 * time.Second,
		EventRetention:     10000,
		RecentEventLimit:   50,

		QuarantineAttempts: 5,
		QuarantineWindow:   60 * time.Second,
		GuestMsgPerSec:     10,
		GuestMsgMaxBytes:   4096,

		MaxVMs: 60, ChaosPerIP: 500 * time.Millisecond, IdleResetAfter: 15 * time.Minute,

		DefaultSpeed: 2, MaxSpeed: 50,
		AmbientEvery:    100 * time.Millisecond,
		AttackerEvery:   2 * time.Second,
		PresetHealAfter: 30 * time.Second,
		PresetStepEvery: 15 * time.Second,
		LoadPriorityMix: [3]float64{.2, .5, .3},
		ChaosBaseRates: map[string]float64{
			"kill-guestd": .05, "crash-vm": .02, "hang-next-boot": .02, "slow-boot": .02, "compromise": .01,
			"kill-host": .01, "pause-heartbeats": .01, "restart-host": .02, "clock-skew": .01, "fill-disk": .01, "cordon": .01,
			"partition-rack": .005, "one-way-partition": .01, "latency-spike": .02, "packet-loss": .02, "duplicate-messages": .02, "reorder-messages": .02,
			"kill-scheduler": .005, "wipe-store": .001, "slow-store": .01, "flood": .01, "sustained-load": .01, "burst-after-rack-loss": .005,
			"add-rack": .001, "remove-rack": .001,
		},
	}
}

// Pools ships two pools so per-pool rack spreading is visible.
var Pools = []struct {
	Name, Image    string
	CPUs, MemoryMB int
}{
	{"ios-xcode16-m", "macos-15-xcode16", 4, 8192},
	{"ios-xcode15-s", "macos-14-xcode15", 2, 4096},
}
