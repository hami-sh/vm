package chaos

import "github.com/hami-sh/vm/internal/api"

// catalogue names are stable URL segments shared by scenarios and the UI.
var catalogue = []api.ChaosAction{
	{Name: "kill-guestd", Label: "Kill guestd", Layer: "guest", Target: api.TargetVM, Ambient: true},
	{Name: "crash-vm", Label: "Crash VM", Layer: "guest", Target: api.TargetVM, Ambient: true},
	{Name: "hang-next-boot", Label: "Hang next boot", Layer: "guest", Target: api.TargetVM, Ambient: true},
	{Name: "slow-boot", Label: "Slow boot", Layer: "guest", Target: api.TargetHost, Ambient: true, Params: []string{"factor"}},
	{Name: "compromise", Label: "Compromise guest", Layer: "guest", Target: api.TargetVM, Ambient: true},
	{Name: "kill-host", Label: "Kill host", Layer: "host", Target: api.TargetHost, Ambient: true},
	{Name: "pause-heartbeats", Label: "Pause heartbeats", Layer: "host", Target: api.TargetHost, Ambient: true},
	{Name: "restart-host", Label: "Restart host", Layer: "host", Target: api.TargetHost, Ambient: true},
	{Name: "clock-skew", Label: "Clock skew", Layer: "host", Target: api.TargetHost, Ambient: true, Params: []string{"ms"}},
	{Name: "fill-disk", Label: "Fill disk", Layer: "host", Target: api.TargetHost, Ambient: true},
	{Name: "cordon", Label: "Cordon / drain", Layer: "host", Target: api.TargetHost, Ambient: true},
	{Name: "partition-rack", Label: "Partition rack", Layer: "network", Target: api.TargetRack, Ambient: true},
	{Name: "one-way-partition", Label: "One-way partition", Layer: "network", Target: api.TargetHost, Ambient: true},
	{Name: "latency-spike", Label: "Latency spike", Layer: "network", Target: api.TargetGlobal, Ambient: true, Params: []string{"ms"}},
	{Name: "packet-loss", Label: "Packet loss", Layer: "network", Target: api.TargetGlobal, Ambient: true, Params: []string{"percent"}},
	{Name: "duplicate-messages", Label: "Duplicate messages", Layer: "network", Target: api.TargetGlobal, Ambient: true},
	{Name: "reorder-messages", Label: "Reorder messages", Layer: "network", Target: api.TargetGlobal, Ambient: true},
	{Name: "kill-scheduler", Label: "Kill scheduler", Layer: "control", Target: api.TargetScheduler, Ambient: true},
	{Name: "wipe-store", Label: "Wipe store", Layer: "control", Target: api.TargetScheduler, Ambient: true},
	{Name: "slow-store", Label: "Slow store", Layer: "control", Target: api.TargetScheduler, Ambient: true, Params: []string{"ms"}},
	{Name: "flood", Label: "Flood N", Layer: "load", Target: api.TargetGlobal, Ambient: true, Params: []string{"count"}},
	{Name: "sustained-load", Label: "Sustained load", Layer: "load", Target: api.TargetGlobal, Ambient: true, Params: []string{"rate", "interactive", "standard", "batch"}},
	{Name: "burst-after-rack-loss", Label: "Burst after rack loss", Layer: "load", Target: api.TargetRack, Ambient: true, Params: []string{"count"}},
	{Name: "add-rack", Label: "Add rack or host", Layer: "topology", Target: api.TargetGlobal, Ambient: true, Params: []string{"hosts"}},
	{Name: "remove-rack", Label: "Drain and remove rack", Layer: "topology", Target: api.TargetRack, Ambient: true},
	{Name: "noise", Label: "Ambient chaos", Layer: "ambient", Target: api.TargetGlobal, Params: []string{"intensity"}},
}
var recoveryActions = []api.ChaosAction{
	{Name: "heal-rack", Label: "Heal rack", Layer: "network", Target: api.TargetRack},
	{Name: "power-off-rack", Label: "Power off rack", Layer: "host", Target: api.TargetRack},
	{Name: "resume-heartbeats", Label: "Resume heartbeats", Layer: "host", Target: api.TargetHost},
	{Name: "restart-scheduler", Label: "Restart scheduler", Layer: "control", Target: api.TargetScheduler},
	{Name: "uncordon", Label: "Uncordon", Layer: "host", Target: api.TargetHost},
	{Name: "drain", Label: "Drain", Layer: "topology", Target: api.TargetHost},
	{Name: "add-host", Label: "Add host", Layer: "topology", Target: api.TargetRack},
	{Name: "impostor-register", Label: "Impostor register", Layer: "guest", Target: api.TargetVM},
	{Name: "delete", Label: "Delete VM", Layer: "guest", Target: api.TargetVM},
	{Name: "reset", Label: "Reset fleet", Layer: "control", Target: api.TargetGlobal},
}
var presets = []api.ChaosAction{
	{Name: "split-brain", Label: "Split brain", Target: api.TargetGlobal},
	{Name: "gray-failure", Label: "Gray failure", Target: api.TargetGlobal},
	{Name: "thundering-herd", Label: "Thundering herd", Target: api.TargetGlobal},
	{Name: "rolling-maintenance", Label: "Rolling maintenance", Target: api.TargetGlobal},
	{Name: "hostile-tenant", Label: "Hostile tenant", Target: api.TargetGlobal},
	{Name: "bad-day", Label: "Bad day", Target: api.TargetGlobal},
}
