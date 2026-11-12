// Package store is the scheduler's write-ahead record in SQLite: intent and
// identity only, one transaction per transition, read back only on startup.
// W1 owns the implementation (modernc.org/sqlite, pure Go, CGO_ENABLED=0).
package store

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"reflect"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/config"
	_ "modernc.org/sqlite"
)

// Schema is applied on open. events.kind includes 'alert' (wipe-store
// higher-generation path) and 'reconcile', added in Phase 0.
//
//go:embed schema.sql
var Schema string

type Record struct {
	VM           api.VM
	Key, Hash    string
	Deadline     int64
	ReachedReady bool
}

type HostRecord struct {
	ID, Rack     string
	Slots        int
	Admin        api.AdminState
	RegisteredAt int64
}

// Data contains only durable intent, never leases, tokens or reservations.
type Data struct {
	Epoch int64
	// RecoverUntil is -1 before the first scheduler initializes the deadline,
	// positive during wipe recovery, and zero after recovery has expired.
	RecoverUntil int64
	VMs          map[string]Record
	Hosts        map[string]HostRecord
	Queue        map[string]api.QueueEntry
}

type Store struct {
	db    *sql.DB
	fresh bool
}

var ErrCAS = errors.New("store: generation compare-and-swap failed")

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	var n int
	err = db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='meta'").Scan(&n)
	if err == nil && n == 0 {
		_, err = db.Exec(Schema)
	}
	if err == nil {
		_, err = db.Exec("PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;")
	}
	if err == nil {
		err = db.QueryRow("SELECT NOT EXISTS(SELECT 1 FROM meta WHERE key='scheduler_epoch') OR EXISTS(SELECT 1 FROM meta WHERE key='recover_until' AND CAST(value AS INTEGER)!=0)").Scan(&s.fresh)
	}
	if err == nil {
		for _, p := range config.Pools {
			_, err = db.Exec("INSERT INTO pools(name,image,cpus,memory_mb) VALUES(?,?,?,?) ON CONFLICT(name) DO NOTHING", p.Name, p.Image, p.CPUs, p.MemoryMB)
			if err != nil {
				break
			}
		}
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Fresh() bool { return s.fresh }

// Start atomically increments scheduler_epoch and loads the write-ahead record.
// Only one live scheduler may own a Store.
func (s *Store) Start(ctx context.Context) (Data, int64, error) {
	d := Data{VMs: map[string]Record{}, Hosts: map[string]HostRecord{}, Queue: map[string]api.QueueEntry{}}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return d, 0, err
	}
	defer tx.Rollback()
	var exists int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM meta WHERE key='scheduler_epoch'").Scan(&exists); err != nil {
		return d, 0, err
	}
	s.fresh = exists == 0
	if s.fresh {
		if _, err = tx.ExecContext(ctx, "INSERT INTO meta(key,value) VALUES('recover_until','-1')"); err != nil {
			return d, 0, err
		}
	}
	if err = tx.QueryRowContext(ctx, "SELECT coalesce((SELECT CAST(value AS INTEGER) FROM meta WHERE key='recover_until'),0)").Scan(&d.RecoverUntil); err != nil {
		return d, 0, err
	}
	s.fresh = d.RecoverUntil != 0
	var epoch int64
	err = tx.QueryRowContext(ctx, `INSERT INTO meta(key,value) VALUES('scheduler_epoch','1') ON CONFLICT(key) DO UPDATE SET value=CAST(value AS INTEGER)+1 RETURNING CAST(value AS INTEGER)`).Scan(&epoch)
	if err != nil {
		return d, 0, err
	}
	d.Epoch = epoch
	rows, err := tx.QueryContext(ctx, "SELECT id,rack,slots,admin_state,registered_at FROM hosts")
	if err != nil {
		return d, 0, err
	}
	for rows.Next() {
		var h HostRecord
		if err = rows.Scan(&h.ID, &h.Rack, &h.Slots, &h.Admin, &h.RegisteredAt); err != nil {
			break
		}
		d.Hosts[h.ID] = h
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return d, 0, err
	}
	rows, err = tx.QueryContext(ctx, `SELECT id,pool,coalesce(idempotency_key,''),request_hash,desired,state,generation,coalesce(host_id,''),priority,attempts,coalesce(boot_deadline_at,0),coalesce(last_reason,''),created_at,updated_at,EXISTS(SELECT 1 FROM meta WHERE key='first_ready/'||vms.id) OR state='Ready' FROM vms`)
	if err != nil {
		return d, 0, err
	}
	for rows.Next() {
		var r Record
		v := &r.VM
		if err = rows.Scan(&v.ID, &v.Pool, &r.Key, &r.Hash, &v.Desired, &v.State, &v.Generation, &v.HostID, &v.Priority, &v.Attempts, &r.Deadline, &v.LastReason, &v.CreatedAt, &v.UpdatedAt, &r.ReachedReady); err != nil {
			break
		}
		if h, ok := d.Hosts[v.HostID]; ok {
			v.Rack = h.Rack
		}
		d.VMs[v.ID] = r
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return d, 0, err
	}
	rows, err = tx.QueryContext(ctx, "SELECT vm_id,priority,enqueued_at,not_before,reason FROM placement_queue")
	if err != nil {
		return d, 0, err
	}
	for rows.Next() {
		var q api.QueueEntry
		if err = rows.Scan(&q.VMID, &q.Priority, &q.EnqueuedAt, &q.NotBefore, &q.Reason); err != nil {
			break
		}
		d.Queue[q.VMID] = q
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return d, 0, err
	}
	return d, epoch, tx.Commit()
}

// Commit writes changed intent and its audit trail in one transaction. Every
// update compares the previous generation; no command is sent on CAS failure.
func (s *Store) Commit(ctx context.Context, before, after Data, events []api.Event, retention int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if after.Epoch < before.Epoch {
		return ErrCAS
	}
	res, err := tx.ExecContext(ctx, "UPDATE meta SET value=? WHERE key='scheduler_epoch' AND CAST(value AS INTEGER)=?", after.Epoch, before.Epoch)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrCAS
	}
	if after.RecoverUntil != before.RecoverUntil {
		if _, err = tx.ExecContext(ctx, "INSERT INTO meta(key,value) VALUES('recover_until',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", after.RecoverUntil); err != nil {
			return err
		}
	}
	for id, h := range after.Hosts {
		if reflect.DeepEqual(before.Hosts[id], h) {
			continue
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO hosts(id,rack,slots,admin_state,registered_at) VALUES(?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET rack=excluded.rack,slots=excluded.slots,admin_state=excluded.admin_state`, h.ID, h.Rack, h.Slots, h.Admin, h.RegisteredAt)
		if err != nil {
			return err
		}
	}
	// Release losing recovered-key claims before inserting their winners;
	// map iteration order must not decide whether a UNIQUE constraint fires.
	for id, old := range before.VMs {
		if next, ok := after.VMs[id]; ok && old.Key != "" && old.Key != next.Key {
			if _, err = tx.ExecContext(ctx, "UPDATE vms SET idempotency_key=NULL WHERE id=? AND generation=?", id, old.VM.Generation); err != nil {
				return err
			}
		}
	}
	for id, r := range after.VMs {
		old, exists := before.VMs[id]
		if exists && reflect.DeepEqual(old, r) {
			continue
		}
		v := r.VM
		if r.ReachedReady {
			if _, err = tx.ExecContext(ctx, "INSERT INTO meta(key,value) VALUES(?,'1') ON CONFLICT(key) DO NOTHING", "first_ready/"+id); err != nil {
				return err
			}
		}
		if exists {
			if v.Generation < old.VM.Generation {
				return ErrCAS
			}
			res, e := tx.ExecContext(ctx, `UPDATE vms SET desired=?,state=?,generation=?,host_id=nullif(?,''),priority=?,attempts=?,boot_deadline_at=nullif(?,0),last_reason=?,updated_at=?,idempotency_key=nullif(?,''),request_hash=? WHERE id=? AND generation=?`, v.Desired, v.State, v.Generation, v.HostID, v.Priority, v.Attempts, r.Deadline, v.LastReason, v.UpdatedAt, r.Key, r.Hash, id, old.VM.Generation)
			if e != nil {
				return e
			}
			n, e := res.RowsAffected()
			if e != nil {
				return e
			}
			if n != 1 {
				return ErrCAS
			}
		} else {
			_, err = tx.ExecContext(ctx, `INSERT INTO vms(id,pool,idempotency_key,request_hash,desired,state,generation,host_id,priority,attempts,boot_deadline_at,last_reason,created_at,updated_at) VALUES(?,?,nullif(?,''),?,?,?,?,nullif(?,''),?,?,nullif(?,0),?,?,?)`, id, v.Pool, r.Key, r.Hash, v.Desired, v.State, v.Generation, v.HostID, v.Priority, v.Attempts, r.Deadline, v.LastReason, v.CreatedAt, v.UpdatedAt)
			if err != nil {
				return err
			}
		}
	}
	if !reflect.DeepEqual(before.Queue, after.Queue) {
		if _, err = tx.ExecContext(ctx, "DELETE FROM placement_queue"); err != nil {
			return err
		}
		for _, q := range after.Queue {
			_, err = tx.ExecContext(ctx, "INSERT INTO placement_queue(vm_id,priority,enqueued_at,not_before,reason) VALUES(?,?,?,?,?)", q.VMID, q.Priority, q.EnqueuedAt, q.NotBefore, q.Reason)
			if err != nil {
				return err
			}
		}
	}
	for _, e := range events {
		_, err = tx.ExecContext(ctx, `INSERT INTO events(at,kind,vm_id,host_id,rack,generation,from_state,to_state,detail) VALUES(?,?,?,?,?,?,?,?,?)`, e.At, e.Kind, e.VMID, e.HostID, e.Rack, e.Generation, e.From, e.To, e.Detail)
		if err != nil {
			return fmt.Errorf("event: %w", err)
		}
	}
	_, err = tx.ExecContext(ctx, "DELETE FROM events WHERE seq <= (SELECT max(seq)-? FROM events)", retention)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) Events(ctx context.Context) ([]api.Event, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT seq,at,kind,coalesce(vm_id,''),coalesce(host_id,''),coalesce(rack,''),coalesce(generation,0),coalesce(from_state,''),coalesce(to_state,''),detail FROM events ORDER BY seq")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []api.Event{}
	for rows.Next() {
		var e api.Event
		if err = rows.Scan(&e.Seq, &e.At, &e.Kind, &e.VMID, &e.HostID, &e.Rack, &e.Generation, &e.From, &e.To, &e.Detail); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
