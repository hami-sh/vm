package recording

import (
	"encoding/json"
	"errors"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"

	"github.com/hami-sh/vm/internal/api"
)

// DeriveMetrics derives latency only from flushed recording files, including
// old reports whose Ready value came from the scheduler's lifetime histogram.
// Callers can evaluate scenario metric expectations before publishing a report.
func DeriveMetrics(dir string, report *api.RunReport) error {
	created := map[string]int64{}
	events := map[int64]api.Event{}
	excluded := map[int64]bool{}
	var final api.Snapshot
	var start int64
	first := true
	read := func(name string, decode func(*json.Decoder) error) error {
		f, err := os.Open(filepath.Join(dir, name))
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		defer f.Close()
		d := json.NewDecoder(f)
		for {
			if err := decode(d); err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
		}
	}
	add := func(e api.Event) {
		if e.At >= start && !excluded[e.Seq] && e.Kind == api.EventTransition {
			events[e.Seq] = e
		}
	}
	if err := read(SnapshotsFile, func(d *json.Decoder) error {
		var s api.Snapshot
		if err := d.Decode(&s); err != nil {
			return err
		}
		if first {
			start, first = s.At, false
			for _, e := range s.Recent {
				excluded[e.Seq] = true
			}
		}
		for _, v := range s.VMs {
			created[v.ID] = v.CreatedAt
		}
		final = s
		for _, e := range s.Recent {
			add(e)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := read(EventsFile, func(d *json.Decoder) error {
		var e api.Event
		if err := d.Decode(&e); err != nil {
			return err
		}
		add(e)
		return nil
	}); err != nil {
		return err
	}
	ordered := make([]api.Event, 0, len(events))
	for _, e := range events {
		ordered = append(ordered, e)
	}
	slices.SortFunc(ordered, func(a, b api.Event) int {
		if a.At < b.At || (a.At == b.At && a.Seq < b.Seq) {
			return -1
		}
		if a.At == b.At && a.Seq == b.Seq {
			return 0
		}
		return 1
	})
	lost := map[string]int64{}
	ready := map[string]bool{}
	var recoveries, boots []int64
	for _, e := range ordered {
		if e.To == api.VMLost || e.To == api.VMUnhealthy {
			if _, ok := lost[e.VMID]; !ok {
				lost[e.VMID] = e.At
			}
		}
		if e.To != api.VMReady {
			continue
		}
		if at, ok := lost[e.VMID]; ok {
			recoveries = append(recoveries, e.At-at)
			delete(lost, e.VMID)
		}
		if at, ok := created[e.VMID]; ok && !ready[e.VMID] && e.At >= at {
			boots = append(boots, e.At-at)
			ready[e.VMID] = true
		}
	}
	report.Metrics = maps.Clone(report.Metrics)
	if report.Metrics == nil {
		report.Metrics = map[string]float64{}
	}
	// Quarantine is intentional termination, not an unfulfilled recovery.
	for _, v := range final.VMs {
		if v.State == api.VMFailed && v.LastReason == api.ReasonQuarantined {
			delete(lost, v.ID)
		}
	}
	report.Metrics["recovery_failures"] = float64(len(lost))
	for name, samples := range map[string][]int64{"recover": recoveries, "ready": boots} {
		value := int64(0)
		if len(samples) > 0 {
			slices.Sort(samples)
			value = samples[(len(samples)*99-1)/100]
		}
		report.Metrics["p99_"+name+"_ms"] = float64(value)
		if name == "recover" {
			if len(samples) == 0 {
				delete(report.Metrics, "p99_recover_ms")
			}
			report.P99RecoverMs = value
			report.Metrics["recovery_samples"] = float64(len(samples))
		} else {
			report.Metrics["ready_samples"] = float64(len(samples))
		}
	}
	return nil
}
