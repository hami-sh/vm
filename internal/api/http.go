package api

// Public HTTP API shapes. Routes:
//
//	POST   /v1/vms                      CreateVMRequest + Idempotency-Key -> 202 CreateVMResponse | 422 | 429 | 503
//	GET    /v1/vms, /v1/vms/{id}        []VM, VM
//	DELETE /v1/vms/{id}                 202 VM (idempotent)
//	GET    /v1/hosts                    []Host
//	GET    /v1/queue                    []QueueEntry in dequeue order
//	POST   /v1/queue/{vm_id}/priority   SetPriorityRequest (classes 1-3 only)
//	GET    /v1/events                   SSE: "snapshot" (Snapshot) and "event" (Event)
//	GET    /v1/chaos/actions            ChaosCatalog
//	POST   /v1/chaos/{action}           ChaosRequest -> ChaosResult
//	POST   /v1/sim/{pause|play|step|speed}      SimRequest
//	POST   /v1/topology/{add-rack|add-host|drain} TopologyRequest
//	GET    /v1/runs, /v1/runs/{id}, /v1/runs/{id}/snapshots
//	GET    /metrics                     Prometheus text
//	GET    /healthz                     200 "ok"

// CreateVMRequest is the body of POST /v1/vms. Priority is "interactive",
// "standard" (default) or "batch".
type CreateVMRequest struct {
	Pool     string `json:"pool"`
	Image    string `json:"image,omitempty"`
	Priority string `json:"priority,omitempty"`
}

// CreateVMResponse is returned with 202.
type CreateVMResponse struct {
	ID         string  `json:"id"`
	State      VMState `json:"state"`
	Generation int64   `json:"gen"`
}

// ErrorResponse is the body of every 4xx/5xx. RetryAfterS mirrors the
// Retry-After header on 429 and 503.
type ErrorResponse struct {
	Error       string `json:"error"`
	RetryAfterS int    `json:"retry_after_s,omitempty"`
}

// SetPriorityRequest moves a queued request between classes 1 and 3.
type SetPriorityRequest struct {
	Priority string `json:"priority"`
}

// TargetKind is what a chaos action applies to.
type TargetKind string

const (
	TargetVM        TargetKind = "vm"
	TargetHost      TargetKind = "host"
	TargetRack      TargetKind = "rack"
	TargetScheduler TargetKind = "scheduler"
	TargetGlobal    TargetKind = "global"
	TargetLink      TargetKind = "link"
)

// ChaosAction describes one action in the menus built from GET /v1/chaos/actions.
type ChaosAction struct {
	Name   string     `json:"name"` // URL segment, e.g. "kill-host"
	Label  string     `json:"label"`
	Layer  string     `json:"layer"` // guest | host | network | control | load | topology
	Target TargetKind `json:"target"`
	// Ambient is true when the action can be picked by the intensity slider.
	Ambient bool `json:"ambient"`
	// Params names optional numeric parameters, e.g. "factor", "percent", "count", "ms".
	Params []string `json:"params,omitempty"`
}

// ChaosCatalog is the body of GET /v1/chaos/actions.
type ChaosCatalog struct {
	Actions []ChaosAction `json:"actions"`
	Presets []ChaosAction `json:"presets"`
}

// ChaosRequest is the body of POST /v1/chaos/{action}.
type ChaosRequest struct {
	Target string             `json:"target,omitempty"` // VM, host or rack ID; empty for scheduler/global
	Params map[string]float64 `json:"params,omitempty"`
}

// ChaosResult reports what an action did.
type ChaosResult struct {
	Action string `json:"action"`
	Target string `json:"target,omitempty"`
	Detail string `json:"detail"`
}

// SimRequest is the body of POST /v1/sim/{action}. Speed is used by "speed";
// StepMs by "step"; Intensity, Filters and LoadRate by "chaos" settings.
type SimRequest struct {
	Speed     float64  `json:"speed,omitempty"`
	StepMs    int64    `json:"step_ms,omitempty"`
	Intensity *float64 `json:"intensity,omitempty"`
	Filters   []string `json:"filters,omitempty"`
	LoadRate  *float64 `json:"load_rate,omitempty"`
	Seed      int64    `json:"seed,omitempty"`
}

// TopologyRequest is the body of POST /v1/topology/{action}.
type TopologyRequest struct {
	Rack  string `json:"rack,omitempty"`
	Host  string `json:"host,omitempty"`
	Hosts int    `json:"hosts,omitempty"` // add-rack: hosts in the new rack (default 4)
}

// RunSummary is one row of the Runs page (GET /v1/runs).
type RunSummary struct {
	ID           string  `json:"id"`
	Scenario     string  `json:"scenario"`
	Seed         int64   `json:"seed"`
	Speed        float64 `json:"speed"`
	Passed       bool    `json:"passed"`
	P99RecoverMs int64   `json:"p99_recover_ms"`
	Violations   int     `json:"violations"`
	StartedAt    string  `json:"started_at"` // wall clock RFC 3339
	DurationMs   int64   `json:"duration_ms"`
}

// CheckResult is one check in report.json.
type CheckResult struct {
	Name        string `json:"name"`
	Passed      bool   `json:"passed"`
	Detail      string `json:"detail,omitempty"`
	FirstBadSeq int64  `json:"first_bad_seq,omitempty"` // first event that broke it
}

// RunReport is runs/<id>/report.json.
type RunReport struct {
	RunSummary
	Checks  []CheckResult      `json:"checks"`
	Metrics map[string]float64 `json:"metrics"`
	// Markers are chaos actions and violations for the scrubber.
	Markers []Event `json:"markers"`
}
