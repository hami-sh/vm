// Small helpers shared by every module: escaping, mounting, formatting and
// the vocabulary of the API (states, priorities, event kinds).

const ESC = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' };
export const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ESC[c]);

export const $ = (sel, root = document) => root.querySelector(sel);

// mount replaces el's content with html. Every panel is rebuilt from the
// snapshot on each render; skipping an identical string keeps hover, scroll
// and CSS animations steady when nothing changed.
export function mount(el, html) {
  if (!el || el.__html === html) return;
  el.innerHTML = html;
  el.__html = html;
}

export const STATES = ['Pending', 'Booting', 'Ready', 'Unhealthy', 'Lost', 'Terminating', 'Terminated', 'Failed'];
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
  chaos: '#f5a524',
  fencing: '#ff5d6c',
  admission: '#6aa8ff',
  topology: '#8fd3c1',
  security: '#d6457a',
  invariant: '#ff5d6c',
  alert: '#ff5d6c',
  reconcile: '#b392f0',
};

export const INVARIANTS = {
  one_authoritative_copy: 'One authoritative copy',
  stale_copy_stopped: 'Stale copies stopped',
  slot_capacity: 'Slot capacity',
  generation_monotonic: 'Generations only rise',
  legal_transitions: 'Legal transitions',
  queue_bounded: 'Queue bounded',
  no_successful_attack: 'No successful attack',
  pending_placed_promptly: 'Pending placed promptly',
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

// badge renders a small coloured label.
export const badge = (text, color, cls = '') =>
  `<span class="badge ${cls}" style="--c:${color}">${esc(text)}</span>`;
