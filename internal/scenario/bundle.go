package scenario

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/config"
)

// Bundle publishes one passing recording per scenario without changing the
// full batch. Sparse frames retain the endpoints and every frame within one
// host heartbeat of a chaos/violation marker. The release budget is 100 MB.
func Bundle(input, output string) (int, int64, error) {
	input, err := filepath.Abs(input)
	if err != nil {
		return 0, 0, err
	}
	output, err = filepath.Abs(output)
	if err != nil {
		return 0, 0, err
	}
	if input == output || strings.HasPrefix(input, strings.TrimSuffix(output, string(os.PathSeparator))+string(os.PathSeparator)) {
		return 0, 0, fmt.Errorf("bundle output must not replace its input batch")
	}
	entries, err := os.ReadDir(input)
	if err != nil {
		return 0, 0, err
	}
	type run struct {
		path   string
		report api.RunReport
	}
	selected := map[string]run{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		path := filepath.Join(input, entry.Name())
		b, err := os.ReadFile(filepath.Join(path, "report.json"))
		if err != nil {
			return 0, 0, err
		}
		var report api.RunReport
		if err := json.Unmarshal(b, &report); err != nil {
			return 0, 0, err
		}
		if !report.Passed || report.Violations != 0 {
			return 0, 0, fmt.Errorf("cannot bundle non-green batch: %s", path)
		}
		old, ok := selected[report.Scenario]
		if !ok || report.Seed < old.report.Seed {
			selected[report.Scenario] = run{path, report}
		}
	}
	for _, s := range Catalog() {
		if _, ok := selected[s.Name]; !ok {
			return 0, 0, fmt.Errorf("missing catalog scenario %s in %s", s.Name, input)
		}
	}
	if len(selected) != len(Catalog()) {
		return 0, 0, fmt.Errorf("bundle must contain exactly the catalog scenarios")
	}
	if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
		return 0, 0, err
	}
	staged, err := os.MkdirTemp(filepath.Dir(output), ".bundle-")
	if err != nil {
		return 0, 0, err
	}
	defer os.RemoveAll(staged)
	cfg := config.Default()
	for _, r := range selected {
		dir := filepath.Join(staged, filepath.Base(r.path))
		if err := os.Mkdir(dir, 0o755); err != nil {
			return 0, 0, err
		}
		for _, name := range []string{"report.json", "metrics.json", "events.jsonl", "snapshots.jsonl"} {
			err := func() error {
				in, err := os.Open(filepath.Join(r.path, name))
				if err != nil {
					return err
				}
				defer in.Close()
				out, err := os.Create(filepath.Join(dir, name))
				if err != nil {
					return err
				}
				defer out.Close()
				if name != "snapshots.jsonl" {
					_, err = io.Copy(out, in)
					return err
				}
				decoder, encoder := json.NewDecoder(in), json.NewEncoder(out)
				var last json.RawMessage
				var carried []json.RawMessage
				var lastKept int64
				written, lastWritten := false, false
				emit := func(frame json.RawMessage) error {
					var fields map[string]json.RawMessage
					if err := json.Unmarshal(frame, &fields); err != nil {
						return err
					}
					var err error
					fields["recent"], err = json.Marshal(carried)
					if err != nil {
						return err
					}
					carried = nil
					return encoder.Encode(fields)
				}
				for {
					var frame json.RawMessage
					err := decoder.Decode(&frame)
					if err == io.EOF {
						break
					}
					if err != nil {
						return err
					}
					var stamp struct {
						At     int64             `json:"at"`
						Recent []json.RawMessage `json:"recent"`
					}
					if err := json.Unmarshal(frame, &stamp); err != nil {
						return err
					}
					carried = append(carried, stamp.Recent...)
					keep := !written || stamp.At-lastKept >= cfg.HostHeartbeat.Milliseconds()
					for _, marker := range r.report.Markers {
						delta := stamp.At - marker.At
						if delta >= -cfg.HostHeartbeat.Milliseconds() && delta <= cfg.HostHeartbeat.Milliseconds() {
							keep = true
							break
						}
					}
					if keep {
						if err := emit(frame); err != nil {
							return err
						}
						lastKept, written = stamp.At, true
					}
					last = frame
					lastWritten = keep
				}
				if !written {
					return fmt.Errorf("empty snapshots in %s", r.path)
				}
				if !lastWritten {
					return emit(last)
				}
				return nil
			}()
			if err != nil {
				return 0, 0, err
			}
		}
	}
	var size int64
	err = filepath.WalkDir(staged, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		info, err := entry.Info()
		if err == nil {
			size += info.Size()
		}
		return err
	})
	if err != nil {
		return 0, 0, err
	}
	if size > 100_000_000 {
		return 0, size, fmt.Errorf("recording bundle exceeds 100 MB: %d bytes", size)
	}
	// Preserve the previous evidence until the new directory is in place.
	backup := staged + "-previous"
	if err := os.Rename(output, backup); err != nil && !os.IsNotExist(err) {
		return 0, size, err
	}
	if err := os.Rename(staged, output); err != nil {
		if restore := os.Rename(backup, output); restore != nil && !os.IsNotExist(restore) {
			return 0, size, fmt.Errorf("publish: %v; restore previous bundle: %w", err, restore)
		}
		return 0, size, err
	}
	if err := os.RemoveAll(backup); err != nil {
		return len(selected), size, err
	}
	return len(selected), size, nil
}
