package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
)

// Exercise the production assembly in a subprocess of this same test binary,
// including race instrumentation when the parent is run with -race.
func TestMetricsAfterSchedulerReplacement(t *testing.T) {
	if os.Getenv("VM_METRICS_TEST_CHILD") == "1" {
		flag.CommandLine = flag.NewFlagSet("controlplane", flag.ExitOnError)
		os.Args = []string{os.Args[0], "--addr=" + os.Getenv("VM_METRICS_TEST_ADDR"),
			"--store=" + os.Getenv("VM_METRICS_TEST_STORE"), "--speed=5"}
		main()
		return
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMetricsAfterSchedulerReplacement$")
	cmd.Env = append(os.Environ(), "VM_METRICS_TEST_CHILD=1", "VM_METRICS_TEST_ADDR="+addr,
		"VM_METRICS_TEST_STORE="+filepath.Join(t.TempDir(), "fleet.db"))
	var logs bytes.Buffer
	cmd.Stdout, cmd.Stderr = &logs, &logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cmd.Process.Signal(os.Interrupt)
		if err := cmd.Wait(); err != nil {
			t.Errorf("controlplane exit: %v\n%s", err, logs.String())
		}
	}()
	client := &http.Client{Timeout: 2 * time.Second}
	request := func(method, path, body string) (int, string) {
		t.Helper()
		r, err := http.NewRequestWithContext(ctx, method, "http://"+addr+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set("Idempotency-Key", path+body)
		res, err := client.Do(r)
		if err != nil {
			return 0, err.Error()
		}
		defer res.Body.Close()
		b, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		return res.StatusCode, string(b)
	}
	await := func(description string, ready func() bool) {
		t.Helper()
		tick := (clock.Real{}).NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for !ready() {
			select {
			case <-tick.C():
			case <-ctx.Done():
				t.Fatalf("timed out waiting for %s", description)
			}
		}
	}
	up := func() bool {
		code, body := request("GET", "/v1/snapshot", "")
		var snap api.Snapshot
		return code == 200 && json.Unmarshal([]byte(body), &snap) == nil && snap.Scheduler == api.SchedulerUp
	}
	// Pace mutations with the same configured public wall-time limit.
	mutate := func(path, body string) string {
		t.Helper()
		pace := (clock.Real{}).NewTimer(config.Default().ChaosPerIP)
		defer pace.Stop()
		select {
		case <-pace.C():
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		code, response := request("POST", path, body)
		if code < 200 || code >= 300 {
			t.Fatalf("%s: %d %s", path, code, response)
		}
		return response
	}
	histograms := func() string {
		t.Helper()
		code, body := request("GET", "/metrics", "")
		if code != 200 {
			t.Fatalf("metrics: %d %s", code, body)
		}
		for _, name := range []string{"vm_time_to_ready_seconds", "queue_wait_seconds"} {
			if !strings.Contains(body, "# TYPE "+name+" histogram\n") || !strings.Contains(body, name+"_bucket{") {
				t.Fatalf("missing histogram %s: %s", name, body)
			}
		}
		return body
	}
	await("initial scheduler", up)
	histograms()
	mutate("/v1/vms", `{"pool":"ios-xcode16-m","priority":"standard"}`)
	await("initial Ready sample", func() bool {
		_, body := request("GET", "/metrics", "")
		return strings.Contains(body, "vm_time_to_ready_seconds_count{priority=\"standard\"} 1\n")
	})
	histograms()
	for _, action := range []string{"restart-scheduler", "wipe-store"} {
		if action == "restart-scheduler" {
			mutate("/v1/chaos/kill-scheduler", `{}`)
			if code, _ := request("GET", "/metrics", ""); code != 503 {
				t.Fatalf("closed scheduler metrics status = %d, want 503", code)
			}
		}
		mutate("/v1/chaos/"+action, `{}`)
		await("replacement scheduler", up)
		// Each old scheduler has a Ready sample; each new actor has empty
		// latency histograms. A closed or cached handler cannot pass.
		if body := histograms(); !strings.Contains(body, "vm_time_to_ready_seconds_count{priority=\"standard\"} 0\n") {
			t.Fatalf("%s retained old histogram: %s", action, body)
		}
		t.Logf("%s: both histograms served by replacement, Ready sample count reset to 0", action)
		if action == "restart-scheduler" {
			mutate("/v1/vms", `{"pool":"ios-xcode15-s","priority":"standard"}`)
			await("replacement Ready sample", func() bool {
				_, body := request("GET", "/metrics", "")
				return strings.Contains(body, "vm_time_to_ready_seconds_count{priority=\"standard\"} 1\n")
			})
		}
	}
}
