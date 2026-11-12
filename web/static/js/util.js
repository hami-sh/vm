// Small helpers shared by every module: escaping, mounting, formatting and
// the vocabulary of the API (states, priorities, event kinds).

const ESC = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' };
export const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ESC[c]);

export const $ = (sel, root = document) => root.querySelector(sel);

// VM-targeted actions are history, not ongoing host faults. Missing catalogs
// use the historical names in both live views and older playback.
export function hostFaults(host, catalog) {
  const vmActions = catalog?.actions?.length
    ? catalog.actions.filter((action) => action.target === 'vm').map((action) => action.name)
    : ['kill-guestd', 'crash-vm', 'hang-next-boot', 'compromise', 'impostor-register', 'delete'];
  // Placement eligibility comes from admin_state, not historical commands.
  return (host.faults || []).filter((name) => !vmActions.includes(name) && name !== 'cordon' && name !== 'uncordon');
}

// mount replaces el's content with html. Every panel is rebuilt from the
// snapshot on each render; skipping an identical string keeps hover, scroll
// and CSS animations steady when nothing changed.
export function mount(el, html) {
  if (!el || el.__html === html) return;
  el.innerHTML = html;
  el.__html = html;
}

export const STATES = ['Pending', 'Booting', 'Ready', 'Unhealthy', 'Lost', 'Terminating', 'Terminated', 'Failed'];
const STATE_MEANINGS = {
  Pending: 'Queued, waiting for a free slot', Booting: 'Slot reserved, guest starting',
  Ready: 'guestd proved the VM is usable', Unhealthy: 'Guest stopped answering its host',
  Lost: 'Host lease expired; being replaced', Terminating: 'Stop sent, slot still held',
  Terminated: 'Deleted, slot released', Failed: 'Gave up after repeated attempts',
};
export const stateMeaning = (name) => STATE_MEANINGS[name] || name;
export const stateVar = (s) => `var(--${String(s || 'pending').toLowerCase()})`;

// Priority classes by their wire number. 0 is internal: replacements for
// VMs the fleet lost go ahead of new work.
export const PRIORITIES = [
  { key: 'replacement', label: 'replace', short: 'R', color: '#ff8fa3' },
  { key: 'interactive', label: 'interactive', short: 'I', color: '#6aa8ff' },
  { key: 'standard', label: 'standard', short: 'S', color: '#8fd3c1' },
  { key: 'batch', label: 'batch', short: 'B', color: '#9aa5b6' },
];
export const prio = (n) => PRIORITIES[n] || PRIORITIES[2];

export const EVENT_KINDS = {
  transition: '#8894a7',
  workload: '#8fd3c1',
  chaos: '#f5a524',
  fencing: '#ff5d6c',
  admission: '#6aa8ff',
  topology: '#8fd3c1',
  security: '#d6457a',
  invariant: '#ff5d6c',
  alert: '#ff5d6c',
  reconcile: '#b392f0',
};

export const eventDetail = (event) => event.kind === 'workload'
  ? ({ completed: 'workload completed', idle: 'idle reclaimed' }[event.detail] || event.detail)
  : event.detail;

export const INVARIANTS = {
  one_authoritative_copy: 'One authoritative copy',
  stale_copy_stopped: 'Stale copies stopped',
  slot_capacity: 'Slot capacity',
  generation_monotonic: 'Generations only rise',
  legal_transitions: 'Legal transitions',
  queue_bounded: 'Queue bounded',
  no_successful_attack: 'No successful attack',
  pending_placed_promptly: 'Pending placed promptly',
  idempotency_unique: 'One VM per key',
};

export const INVARIANT_HINTS = {
  one_authoritative_copy: 'At most one copy of a VM is authoritative: current generation and Ready in the scheduler.',
  stale_copy_stopped: 'A copy with an old generation is stopped within one heartbeat of its host reconnecting.',
  slot_capacity: 'No host runs more VMs than it has slots, counting slots still releasing.',
  generation_monotonic: "A VM's generation never decreases, including across scheduler restarts.",
  legal_transitions: 'Every transition in the event log is in the legal-move table.',
  queue_bounded: 'The queue never exceeds its cap, and a replacement is never shed.',
  no_successful_attack: 'Zero successful attacks from a malicious guest.',
  pending_placed_promptly: 'A Pending VM is placed within 2 s of capacity being free.',
  idempotency_unique: 'A retried create never yields a second running VM.',
};

const pad = (n) => String(n).padStart(2, '0');

// clock formats a sim-clock unix ms as HH:MM:SS (UTC).
export function clock(ms) {
  if (!ms) return '--:--:--';
  const d = new Date(ms);
  return `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
}

// dur formats a duration in ms: "850 ms", "4.8 s", "1 m 23 s", "2 h 05 m".
export function dur(ms) {
  if (ms == null || Number.isNaN(ms)) return '–';
  const neg = ms < 0 ? '-' : '';
  ms = Math.abs(ms);
  if (ms < 1000) return `${neg}${Math.round(ms)} ms`;
  if (ms < 60_000) return `${neg}${(ms / 1000).toFixed(ms < 10_000 ? 1 : 0)} s`;
  const s = Math.round(ms / 1000);
  if (s < 3600) return `${neg}${Math.floor(s / 60)} m ${pad(s % 60)} s`;
  return `${neg}${Math.floor(s / 3600)} h ${pad(Math.floor((s % 3600) / 60))} m`;
}

export const secs = (ms) => (ms == null ? '–' : `${(ms / 1000).toFixed(1)} s`);
export const plural = (n, one, many = one + 's') => `${n} ${n === 1 ? one : many}`;
export const title = (s) => String(s || '').replace(/[-_]/g, ' ').replace(/^\w/, (c) => c.toUpperCase());

// vmName shortens a VM id for display. The scheduler's ids are
// vm-<epoch>-<counter>-<uuid>; the counter and the start of the uuid are
// enough to tell VMs apart on screen. The full id stays in titles and in the
// detail panel.
export function vmName(id) {
  const s = String(id ?? '');
  const m = /^vm-\d+-0*(\d+)-([0-9a-f]{4})/.exec(s);
  if (m) return `vm-${m[1]}-${m[2]}`;
  return s.length > 16 ? `${s.slice(0, 7)}…${s.slice(-4)}` : s;
}

// shorten replaces every long VM id inside free text (event details).
export const shorten = (text) => String(text ?? '').replace(/vm-\d{6,}-\d+-[0-9a-f-]{36}/g, vmName);

// badge renders a small coloured label.
export const badge = (text, color, cls = '') =>
  `<span class="badge ${cls}" style="--c:${color}">${esc(text)}</span>`;
