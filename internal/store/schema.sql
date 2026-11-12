PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;

-- schema_version, scheduler_epoch: on every scheduler start, max(stored + 1, sim-clock unix ms),
-- so it still increases after the store is wiped (agents ignore older epochs)
CREATE TABLE meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE pools (
  name      TEXT PRIMARY KEY,              -- 'ios-xcode16-m'
  image     TEXT NOT NULL,
  cpus      INTEGER NOT NULL,
  memory_mb INTEGER NOT NULL
);

-- Operator intent about hosts. Leases/heartbeats live in memory only.
CREATE TABLE hosts (
  id            TEXT PRIMARY KEY,          -- 'r1-h1'
  rack          TEXT NOT NULL,
  slots         INTEGER NOT NULL DEFAULT 2,
  admin_state   TEXT NOT NULL DEFAULT 'active'
                CHECK (admin_state IN ('active','cordoned','draining','removed')),
  registered_at INTEGER NOT NULL           -- sim-clock unix ms
);
CREATE INDEX hosts_rack ON hosts(rack);

-- One row per VM: desired state + the scheduler's belief + identity.
CREATE TABLE vms (
  id               TEXT PRIMARY KEY,
  pool             TEXT NOT NULL REFERENCES pools(name),
  idempotency_key  TEXT UNIQUE,
  request_hash     TEXT NOT NULL,          -- same key + different body => 422
  desired          TEXT NOT NULL DEFAULT 'running'
                   CHECK (desired IN ('running','deleted')),
  state            TEXT NOT NULL DEFAULT 'Pending'
                   CHECK (state IN ('Pending','Booting','Ready','Unhealthy',
                                    'Lost','Terminating','Terminated','Failed')),
  generation       INTEGER NOT NULL DEFAULT 0,  -- fencing token, only ever increases
  host_id          TEXT REFERENCES hosts(id),
  priority         INTEGER NOT NULL DEFAULT 2   -- requested class: 1 interactive, 2 standard, 3 batch
                   CHECK (priority BETWEEN 1 AND 3),
  attempts         INTEGER NOT NULL DEFAULT 0,
  boot_deadline_at INTEGER,                -- set only while Booting
  last_reason      TEXT,
  created_at       INTEGER NOT NULL,
  updated_at       INTEGER NOT NULL
);
CREATE INDEX vms_host  ON vms(host_id) WHERE host_id IS NOT NULL;

-- Admission queue: one row per VM waiting for a slot. Deleted in the same
-- transaction that places the VM. Bounded to 32 rows by the scheduler.
CREATE TABLE placement_queue (
  vm_id       TEXT PRIMARY KEY REFERENCES vms(id) ON DELETE CASCADE,
  priority    INTEGER NOT NULL CHECK (priority BETWEEN 0 AND 3),  -- 0 = replacement (reserved); 1-3 as requested, admin can move
  enqueued_at INTEGER NOT NULL,      -- original arrival; kept on retry so it keeps its place
  not_before  INTEGER NOT NULL,      -- retry backoff: not eligible until this time
  reason      TEXT NOT NULL DEFAULT 'new' CHECK (reason IN ('new','retry','replace'))
);
CREATE INDEX queue_order ON placement_queue(priority, enqueued_at);
-- Class 0 is only for VMs lost while Ready through no fault of their own.
CREATE TRIGGER queue_replace_class BEFORE INSERT ON placement_queue
WHEN (NEW.priority = 0) <> (NEW.reason = 'replace')
BEGIN SELECT RAISE(ABORT, 'priority 0 is reserved for reason=replace'); END;
CREATE TRIGGER queue_replace_class_upd BEFORE UPDATE OF priority, reason ON placement_queue
WHEN (NEW.priority = 0) <> (NEW.reason = 'replace')
BEGIN SELECT RAISE(ABORT, 'priority 0 is reserved for reason=replace'); END;

-- Append-only log: UI detail panel, event log, audit. Pruned to last 10k.
CREATE TABLE events (
  seq        INTEGER PRIMARY KEY AUTOINCREMENT,
  at         INTEGER NOT NULL,
  kind       TEXT NOT NULL
             CHECK (kind IN ('transition','fencing','admission','chaos','topology','security','invariant','alert','reconcile')),
  vm_id      TEXT,
  host_id    TEXT,
  rack       TEXT,
  generation INTEGER,
  from_state TEXT,
  to_state   TEXT,
  detail     TEXT NOT NULL DEFAULT ''
);
CREATE INDEX events_vm ON events(vm_id, seq);
