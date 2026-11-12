package driver

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/config"
)

type tartVM struct {
	p        api.BootParams
	launch   api.StartVM
	dir      string
	done     chan struct{}
	running  bool
	stopping bool
}

// Tart uses throwaway clones and a per-boot read-only shared directory. The
// image must contain guestd and a host-provisioned vsock/Unix bridge. Network
// isolation (pf/userspace networking) is a deployment prerequisite, not
// something Tart's default shared NAT provides.
type Tart struct {
	mu    sync.Mutex
	cfg   config.Config
	vms   map[string]*tartVM
	exits chan api.DriverExit
}

func NewTart(cfg config.Config) *Tart {
	return &Tart{cfg: cfg, vms: make(map[string]*tartVM), exits: make(chan api.DriverExit, cfg.GuestMsgMaxBytes)}
}
func tartName(id string, gen int64) string {
	return fmt.Sprintf("vm-%x-%d", sha256.Sum256([]byte(id)), gen)
}
func tartCommand(ctx context.Context, args ...string) error {
	out, err := exec.CommandContext(ctx, "tart", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("tart %s: %w: %s", args[0], err, out)
	}
	return nil
}
func (t *Tart) Start(ctx context.Context, s api.VMSpec, p api.BootParams) (err error) {
	return t.StartCommand(ctx, api.StartVM{VMID: p.VMID, Generation: p.Generation, Token: p.Token, Spec: s}, p)
}
func (t *Tart) StartCommand(ctx context.Context, cmd api.StartVM, p api.BootParams) (err error) {
	s := cmd.Spec
	t.mu.Lock()
	if old := t.vms[p.VMID]; old != nil {
		t.mu.Unlock()
		if old.p.Generation == p.Generation {
			return nil
		}
		return fmt.Errorf("VM already exists")
	}
	if len(t.vms) >= min(2, t.cfg.SlotsPerHost) {
		t.mu.Unlock()
		return fmt.Errorf("full")
	}
	v := &tartVM{p: p, launch: cmd, done: make(chan struct{})}
	t.vms[p.VMID] = v
	t.mu.Unlock()
	name := tartName(p.VMID, p.Generation)
	defer func() {
		if err != nil {
			close(v.done)
		}
	}()
	if err = tartCommand(ctx, "clone", s.Image, name); err != nil {
		return err
	}
	args := []string{"set", name, "--cpu", strconv.Itoa(s.CPUs), "--memory", strconv.Itoa(s.MemoryMB)}
	if s.DiskMB > 0 {
		args = append(args, "--disk-size", strconv.Itoa((s.DiskMB+1023)/1024))
	}
	if err = tartCommand(ctx, args...); err != nil {
		return err
	}
	dir, e := os.MkdirTemp("", "guest-boot-")
	if e != nil {
		return e
	}
	t.mu.Lock()
	v.dir = dir
	t.mu.Unlock()
	b, e := json.Marshal(p)
	if e != nil {
		return e
	}
	if err = os.WriteFile(filepath.Join(dir, "boot.json"), b, 0600); err != nil {
		return err
	}
	process := exec.CommandContext(ctx, "tart", "run", "--no-graphics", "--dir=boot:"+dir+":ro", name)
	if err = process.Start(); err != nil {
		return err
	}
	t.mu.Lock()
	v.running = true
	t.mu.Unlock()
	go func() {
		err := process.Wait()
		t.mu.Lock()
		v.running = false
		stopping := v.stopping
		t.mu.Unlock()
		if !stopping {
			reason := "exited"
			if err != nil {
				reason = err.Error()
			}
			select {
			case t.exits <- api.DriverExit{VMID: p.VMID, Generation: p.Generation, Reason: reason}:
			default:
			}
		}
		close(v.done)
	}()
	return nil
}
func (t *Tart) Stop(ctx context.Context, id string, gen int64) error {
	t.mu.Lock()
	v := t.vms[id]
	if v == nil || v.p.Generation > gen {
		t.mu.Unlock()
		return nil
	}
	v.stopping = true
	running := v.running
	t.mu.Unlock()
	name := tartName(id, v.p.Generation)
	if running {
		if err := tartCommand(ctx, "stop", name); err != nil {
			return err
		}
	}
	select {
	case <-v.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := tartCommand(ctx, "delete", name); err != nil {
		return err
	}
	if err := os.RemoveAll(v.dir); err != nil {
		return err
	}
	t.mu.Lock()
	if t.vms[id] == v {
		delete(t.vms, id)
	}
	t.mu.Unlock()
	return nil
}
func (t *Tart) List(ctx context.Context) ([]api.DriverVM, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []api.DriverVM
	for _, v := range t.vms {
		out = append(out, api.DriverVM{VMID: v.p.VMID, Generation: v.p.Generation, Running: v.running})
	}
	return out, ctx.Err()
}
func (t *Tart) Exits() <-chan api.DriverExit { return t.exits }
func (t *Tart) LaunchInfo(id string, gen int64) (api.StartVM, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	v := t.vms[id]
	if v == nil || v.p.Generation != gen {
		return api.StartVM{}, false
	}
	return v.launch, true
}

var _ api.Driver = (*Tart)(nil)
