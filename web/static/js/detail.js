// The detail panel for whatever is selected: a VM (with its last 20
// transitions), a host, a rack or the scheduler. Hidden until selected.
import { $, esc, mount, clock, dur, badge, prio, stateVar, EVENT_KINDS, vmName, shorten } from './util.js';
import { update } from './store.js';
const LEASE = { healthy: 'var(--ok)', suspect: 'var(--warn)', lost: 'var(--bad)' };
const SCHED = { up: 'var(--ok)', reconciling: 'var(--warn)', down: 'var(--bad)' };

const kv = (pairs) =>
  `<div class="kv">${pairs
    .filter(Boolean)
    .map(([k, value, wide]) => `<div class="${wide ? 'wide' : ''}"><span class="k">${esc(k)}</span><span class="v">${value}</span></div>`)
    .join('')}</div>`;

const head = (name, tag) =>
  `<header class="panel-head"><span class="detail-title">${esc(name)}</span>${tag}<span class="spacer"></span>
    <button class="btn sm" data-act="deselect" aria-label="Close inspector">Close ×</button></header>`;

function eventRows(list, empty) {
  if (!list.length) return `<div class="empty">${empty}</div>`;
  return list
    .map((e) => {
      const move = e.to ? `<span class="st" style="--c:${stateVar(e.to)}">${esc(e.from || '—')} → ${esc(e.to)}</span> ` : `${badge(e.kind, EVENT_KINDS[e.kind] || 'var(--dim)')} `;
      return `<div class="trans"><span class="t">${clock(e.at)}</span><span>${move}${e.gen ? `<span class="faint mono">g${e.gen}</span> ` : ''}<span class="dim">${esc(shorten(e.detail))}</span></span></div>`;
    })
    .join('');
}

// last returns the newest n events that pass keep, newest first.
function last(events, n, keep) {
  const out = [];
  for (let i = events.length - 1; i >= 0 && out.length < n; i--) if (keep(events[i])) out.push(events[i]);
  return out;
}

function vmDetail(id, s, v) {
  const vm = s.vms.find((x) => x.id === id);
  const history = last(v.events, 20, (e) => e.vm_id === id && e.kind === 'transition');
  const title = `<div class="sub-head">Last ${history.length || ''} transitions</div>`;
  if (!vm) {
    return `${head(vmName(id), badge('gone', 'var(--faint)'))}<div class="detail-body">
      <div class="detail-id">${esc(id)}</div><div class="empty">This VM is no longer in the fleet.</div>${title}${eventRows(history, 'None in the log window.')}</div>`;
  }
  const p = prio(vm.priority);
  const queued = (s.queue || []).findIndex((q) => q.vm_id === id);
  return `${head(vmName(id), badge(vm.state, stateVar(vm.state)) + (vm.compromised ? badge('compromised', 'var(--bad)', 'solid') : ''))}
    <div class="detail-body">
    <div class="detail-id">${esc(id)}</div>
    ${kv([
      ['Generation', vm.gen],
      ['Attempts', vm.attempts],
      ['Host', vm.host ? esc(vm.host) : '<span class="faint">none</span>'],
      ['Rack', vm.rack ? esc(vm.rack) : '<span class="faint">none</span>'],
      ['Pool', esc(vm.pool)],
      ['Priority', `<span style="color:${p.color}">${p.label}</span>`],
      ['Desired', esc(vm.desired)],
      queued >= 0 ? ['Queue position', `${queued + 1} of ${s.queue.length}`] : ['Age', dur(s.at - vm.created_at)],
      vm.ready_at ? ['Ready at', clock(vm.ready_at)] : ['In this state', dur(s.at - vm.updated_at)],
      vm.ready_at ? ['Create to ready', dur(vm.ready_at - vm.created_at)] : ['Created', clock(vm.created_at)],
      vm.last_reason ? ['Last reason', esc(vm.last_reason.replace(/_/g, ' ')), true] : null,
    ])}
    ${title}${eventRows(history, 'None in the log window.')}</div>`;
}

function hostDetail(id, s, v, st) {
  const h = s.hosts.find((x) => x.id === id);
  if (!h) return `${head(id, badge('gone', 'var(--faint)'))}<div class="detail-body"><div class="empty">This host is no longer in the fleet.</div></div>`;
  const used = h.slots.filter((sl) => sl.vm_id).length;
  const stale = (h.stale || []).map((x) => `${esc(vmName(x.vm_id))} g${x.gen}`).join(', ');
  const events = last(v.events, 20, (e) => e.host_id === id);
  return `${head(id, badge(`lease ${h.lease}`, LEASE[h.lease] || 'var(--faint)'))}
    <div class="detail-body">
    ${kv([
      ['Rack', esc(h.rack)],
      ['Admin state', esc(h.admin_state)],
      [h.lease === 'healthy' ? 'Last heartbeat' : 'Silent for', dur(h.lease_age_ms)],
      ['Slots', `${used} of ${h.slots.length} used`],
      ['Faults', (h.faults || []).length ? `<span class="bad">${esc(h.faults.join(', '))}</span>` : '<span class="faint">none</span>', true],
      stale ? ['Stale copies (fenced)', `<span class="bad">${stale}</span>`, true] : null,
    ])}
    <div class="sub-head">Recent events</div>${eventRows(events, 'None in the log window.')}</div>`;
}

function rackDetail(id, s, v) {
  const r = s.racks.find((x) => x.id === id);
  if (!r) return `${head(`Rack ${id}`, badge('gone', 'var(--faint)'))}<div class="detail-body"><div class="empty">This rack is no longer in the fleet.</div></div>`;
  const hosts = s.hosts.filter((h) => h.rack === id && h.admin_state !== 'removed');
  const slots = hosts.flatMap((h) => h.slots);
  const count = (lease) => hosts.filter((h) => h.lease === lease).length;
  const events = last(v.events, 20, (e) => e.rack === id || hosts.some((h) => h.id === e.host_id));
  return `${head(`Rack ${id}`, r.partitioned ? badge('partitioned', 'var(--bad)') : badge('reachable', 'var(--ok)'))}
    <div class="detail-body">
    ${kv([
      ['Hosts', hosts.length],
      ['Slots', `${slots.filter((sl) => sl.vm_id).length} of ${slots.length} used`],
      ['Leases', `${count('healthy')} healthy · ${count('suspect')} suspect · ${count('lost')} lost`, true],
    ])}
    <div class="sub-head">Recent events</div>${eventRows(events, 'None in the log window.')}</div>`;
}

function schedulerDetail(s, v) {
  const c = s.counters;
  const status = s.scheduler || 'down';
  const rec = Object.entries(c.recoveries || {}).map(([k, n]) => `${n} ${esc(k)}`).join(' · ');
  const events = last(v.events, 20, (e) => ['reconcile', 'alert', 'fencing'].includes(e.kind) || (e.kind === 'chaos' && /scheduler|store/.test(e.detail)));
  return `${head('Scheduler', badge(status, SCHED[status] || 'var(--faint)'))}
    <div class="detail-body">
    ${kv([
      ['Epoch', s.epoch],
      ['Queue', `${c.queue_depth} of ${c.queue_cap}`],
      ['Placed', c.placed],
      ['Queued', c.queued],
      ['Refused (429)', c.rejected_429],
      ['Shed', c.shed],
      ['Recoveries', rec || '<span class="faint">none</span>', true],
    ])}
    <div class="sub-head">Reconcile, fencing and alerts</div>${eventRows(events, 'None in the log window.')}</div>`;
}

export function render(st, v) {
  const el = $('#detail');
  const s = v.snapshot;
  const sel = st.selection;
  el.hidden = !s || !sel;
  if (!s || !sel) {
    mount(el, '');
    return;
  }
  let html;
  if (sel.kind === 'vm') html = vmDetail(sel.id, s, v);
  else if (sel.kind === 'host') html = hostDetail(sel.id, s, v, st);
  else if (sel.kind === 'rack') html = rackDetail(sel.id, s, v);
  else html = schedulerDetail(s, v);
  mount(el, html);
}

export const actions = {
  deselect: () => update((st) => ((st.selection = null), (st.highlight = null))),
};
