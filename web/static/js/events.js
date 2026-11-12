// The event log: newest first, filtered by kind and by a rack, host or VM id.
import { $, esc, mount, clock, badge, EVENT_KINDS, stateVar, vmName, shorten } from './util.js';
import { update } from './store.js';

const MAX_ROWS = 200;
// Kinds that mean a safety mechanism fired, or failed.
const STRONG = new Set(['fencing', 'invariant', 'alert', 'security']);

const ref = (kind, id, st) =>
  `<button class="ref${st.selection?.kind === kind && st.selection.id === id ? ' on' : ''}" data-act="ref" data-kind="${kind}" data-id="${esc(id)}" title="Inspect ${esc(kind)} ${esc(id)} and its recent history">${esc(kind === 'vm' ? vmName(id) : id)}</button>`;

function matches(e, text) {
  if (!text) return true;
  return [e.vm_id, e.vm_id && vmName(e.vm_id), e.host_id, e.rack, e.detail, e.kind].some((x) => x && x.toLowerCase().includes(text));
}

function row(e, st, n) {
  const refs = [];
  if (e.vm_id) refs.push(ref('vm', e.vm_id, st));
  if (e.host_id) refs.push(ref('host', e.host_id, st));
  if (e.rack && !e.host_id) refs.push(ref('rack', e.rack, st));
  const move =
    e.from || e.to
      ? `<b style="color:${stateVar(e.from)}">${esc(e.from || '—')}</b> <span class="arrow">→</span> <b style="color:${stateVar(e.to)}">${esc(e.to || '—')}</b> `
      : '';
  const gen = e.gen ? `<span class="faint mono">g${e.gen}</span> ` : '';
  const cls = ['log-row'];
  if (STRONG.has(e.kind)) cls.push('strong');
  if (e.kind === 'chaos') cls.push('chaos');
  return `<div class="${cls.join(' ')}">
    <span class="log-time">${clock(e.at)}</span>
    <span>${badge(e.kind, EVENT_KINDS[e.kind] || 'var(--dim)')}</span>
    <span class="log-text">${refs.join(' ')} ${gen}${move}<span class="${e.from || e.to ? 'dim' : ''}">${esc(shorten(e.detail))}</span>${n > 1 ? `<span class="log-n" title="${n} identical events in a row">×${n}</span>` : ''}</span>
  </div>`;
}

export function render(st, v) {
  const kinds = st.log.kinds;
  mount(
    $('#log-kinds'),
    Object.entries(EVENT_KINDS)
      .map(([kind, color]) => `<button class="chip${kinds.has(kind) ? ' on' : ''}" style="--c:${color}" data-act="log-kind" data-kind="${kind}" title="Toggle ${kind} events in the log. With no kinds selected, all events are shown."><i></i>${kind}</button>`)
      .join(''),
  );
  const text = st.log.text.trim().toLowerCase();
  // Newest first. A run of identical events (a flood of 429s) folds into one
  // row with a count, so it cannot push everything else out of view.
  const groups = [];
  for (let i = v.events.length - 1; i >= 0 && groups.length <= MAX_ROWS; i--) {
    const e = v.events[i];
    if (!(kinds.size === 0 || kinds.has(e.kind)) || !matches(e, text)) continue;
    const key = [e.kind, e.vm_id, e.host_id, e.rack, e.from, e.to, e.detail].join('|');
    const last = groups.at(-1);
    if (last && last.key === key) last.n++;
    else groups.push({ key, e, n: 1 });
  }
  const rows = groups.slice(0, MAX_ROWS).map((g) => row(g.e, st, g.n));
  const none = v.events.length ? 'No events match the filter.' : 'No events yet.';
  mount($('#log'), rows.join('') || `<div class="empty">${none}</div>`);
}

// The filter box is static markup, so it keeps focus across renders.
$('#log-filter').addEventListener('input', (e) => update((st) => (st.log.text = e.target.value)));

export const actions = {
  'log-kind'(el) {
    const kind = el.dataset.kind;
    update((st) => (st.log.kinds.has(kind) ? st.log.kinds.delete(kind) : st.log.kinds.add(kind)));
  },
  // A reference in the log or queue selects its target and, for a VM, rings
  // its chip on the grid. It does not open the chaos menu.
  ref(el) {
    const { kind, id } = el.dataset;
    update((st) => {
      st.selection = { kind, id };
      st.highlight = kind === 'vm' ? id : null;
    });
  },
};
