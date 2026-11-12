// The detail panel for whatever is selected: a VM (with its last 20
// transitions), a host, a rack or the scheduler. Hidden until selected.
import { $, esc, mount, clock, dur, badge, prio, stateVar, EVENT_KINDS, vmName, shorten, hostFaults, eventDetail } from './util.js';
import { update } from './store.js';
const LEASE = { healthy: 'var(--ok)', suspect: 'var(--warn)', lost: 'var(--bad)' };
const SCHED = { up: 'var(--ok)', reconciling: 'var(--warn)', down: 'var(--bad)' };

const kv = (pairs) =>
  `<div class="kv">${pairs
    .filter(Boolean)
    .map(([k, value, wide]) => `<div class="${wide ? 'wide' : ''}"><span class="k">${esc(k)}</span><span class="v">${value}</span></div>`)
    .join('')}</div>`;

const head = (name, tag, kind, id, playback, guest) =>
  `<header class="panel-head"><span class="detail-title">${esc(name)}</span>${tag}<span class="spacer"></span>
    ${kind && !playback ? `<button class="btn sm" data-act="open-menu" data-kind="${kind === 'guest' ? 'vm' : kind === 'agent' ? 'host' : kind}" data-id="${esc(id)}" ${guest ? `data-layer="guest" data-host="${esc(guest.host)}" data-gen="${guest.gen}"` : ''} title="Open ${esc(kind)} fault actions without changing the current selection">Actions…</button>` : ''}
    <button class="btn sm" data-act="deselect" title="Close this inspector and clear the selection" aria-label="Close inspector">Close ×</button></header>`;

const link = (kind, id, label, vm) => `<button class="detail-link" data-act="pick" data-kind="${kind}" data-id="${esc(id)}" ${kind === 'guest' ? `data-host="${esc(vm.host)}" data-gen="${vm.gen}"` : ''} title="Inspect ${esc(label)}">${esc(label)}</button>`;

function navigation(sel, s) {
  const vm = ['vm', 'guest'].includes(sel.kind) ? s.vms.find((x) => x.id === sel.id) : null;
  const host = ['host', 'agent'].includes(sel.kind) ? s.hosts.find((x) => x.id === sel.id) : s.hosts.find((x) => x.id === (sel.kind === 'guest' ? sel.host : vm?.host));
  const rack = sel.kind === 'rack' ? sel.id : host?.rack || (sel.kind !== 'guest' ? vm?.rack : null);
  const parts = [link('scheduler', 'scheduler', 'Scheduler')];
  if (rack) parts.push(link('rack', rack, `Rack ${rack}`));
  if (host) parts.push(link('host', host.id, `Host ${host.id}`));
  else if (sel.kind === 'guest') parts.push(`<span>Host ${esc(sel.host)}</span>`);
  if (sel.kind === 'agent' || sel.kind === 'guest') parts.push(link('agent', sel.kind === 'guest' ? sel.host : sel.id, 'Host agent'));
  if (sel.kind === 'guest') {
    const label = `${vmName(sel.id)} · g${sel.gen}`;
    parts.push(vm?.host === sel.host && vm.gen === sel.gen ? link('vm', sel.id, label) : `<span>${esc(label)}</span>`, '<span>guestd</span>');
  } else if (vm) parts.push(link('vm', vm.id, vmName(vm.id)));
  return `<nav class="detail-path" aria-label="Component location">${parts.join('<span aria-hidden="true">›</span>')}</nav>`;
}

function eventRows(list, empty) {
  if (!list.length) return `<div class="empty">${empty}</div>`;
  return list
    .map((e) => {
      const move = e.to ? `<span class="st" style="--c:${stateVar(e.to)}">${esc(e.from || '—')} → ${esc(e.to)}</span> ` : `${badge(e.kind, EVENT_KINDS[e.kind] || 'var(--dim)')} `;
      return `<div class="trans"><span class="t">${clock(e.at)}</span><span>${move}${e.gen ? `<span class="faint mono">g${e.gen}</span> ` : ''}<span class="dim">${esc(shorten(eventDetail(e)))}</span></span></div>`;
    })
    .join('');
}

// last returns the newest n events that pass keep, newest first.
function last(events, n, keep) {
  const out = [];
  for (let i = events.length - 1; i >= 0 && out.length < n; i--) if (keep(events[i])) out.push(events[i]);
  return out;
}

function vmDetail(id, s, v, guest) {
  const vm = s.vms.find((x) => x.id === id);
  if (guest && (!vm || vm.host !== guest.host || vm.gen !== guest.gen || !s.hosts.some((h) => h.id === guest.host && h.lease === 'healthy'))) {
    return `${head('guestd', badge('unverified', 'var(--warn)'))}<div class="detail-body">
      <p class="component-role">This selection is the last-reported guestd in ${esc(vmName(id))}, generation ${guest.gen}, on ${esc(guest.host)}. Its current status is not verified. This inspector does not follow a replacement process. Fault actions are unavailable for an unverified or replaced generation.</p>
      ${vm ? `<div class="detail-links">${link('vm', id, `Current logical VM · g${vm.gen} · ${vm.host || 'unplaced'}`)}</div>` : ''}</div>`;
  }
  const history = last(v.events, 20, (e) => e.vm_id === id && ['transition', 'workload'].includes(e.kind));
  const title = `<div class="sub-head">Recent VM history</div>`;
  if (!vm) {
    return `${head(vmName(id), badge('gone', 'var(--faint)'))}<div class="detail-body">
      <div class="detail-id">${esc(id)}</div><div class="empty">This VM is no longer in the fleet.</div>${title}${eventRows(history, 'None in the log window.')}</div>`;
  }
  const p = prio(vm.priority);
  const queued = (s.queue || []).findIndex((q) => q.vm_id === id);
  const workload = (s.sim?.workloads || []).find((w) => w.vm_id === id);
  const phase = workload?.phase === 'idle' ? 'Idle' : 'Busy';
  const workloadStatus = workload && (workload.ends_at > s.at
    ? `${phase}, ${phase === 'Idle' ? 'reclaimed' : 'ends'} in ${dur(workload.ends_at - s.at)}`
    : `${phase}, ${vm.state !== 'Ready' ? 'overdue; waits for Ready' : 'ending now'}`);
  return `${head(guest ? 'guestd' : vmName(id), badge(vm.state, stateVar(vm.state)) + (vm.compromised ? badge('compromised', 'var(--bad)', 'solid') : ''), guest ? 'guest' : 'vm', id, v.playback, guest)}
    <div class="detail-body">
    <p class="component-role">${guest ? 'Runs inside this VM. Proves readiness and sends heartbeats to its local host agent, never directly to the scheduler. The state below is the scheduler’s VM state; a separate guest heartbeat age is not published.' : 'A macOS VM in a slot on its physical host. Its guestd process reports readiness to the host agent.'}</p>
    ${!guest && vm.host ? `<div class="detail-links">${link('guest', id, 'Inspect guestd', vm)} ${link('agent', vm.host, 'Inspect host agent')}</div>` : ''}
    <div class="detail-id">${esc(id)}</div>
    ${kv([
      workload ? ['Workload', esc(workloadStatus), true] : null,
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

function hostDetail(id, s, v, catalog, agent = false) {
  const h = s.hosts.find((x) => x.id === id);
  if (!h) return `${head(id, badge('gone', 'var(--faint)'))}<div class="detail-body"><div class="empty">This host is no longer in the fleet.</div></div>`;
  const used = h.slots.filter((sl) => sl.vm_id).length;
  const faults = hostFaults(h, catalog);
  const stale = (h.stale || []).map((x) => `${esc(vmName(x.vm_id))} g${x.gen}`).join(', ');
  const events = last(v.events, 20, (e) => e.host_id === id);
  return `${head(agent ? 'Host agent' : `Host ${id}`, badge(`lease ${h.lease}`, LEASE[h.lease] || 'var(--faint)'), agent ? 'agent' : 'host', id, v.playback)}
    <div class="detail-body">
    <p class="component-role">${agent ? 'One agent runs on this host. It admits VMs, owns the hypervisor driver and checks local guestd leases. It sends the scheduler one combined inventory heartbeat for this host.' : 'A physical Mac within this rack. Its host agent owns the driver and has final say over local VM-slot admission.'}</p>
    <div class="detail-links">${agent ? link('host', id, `Inspect host ${id}`) : link('agent', id, 'Inspect host agent')}</div>
    ${kv([
      ['Rack', esc(h.rack)],
      ['Admin state', h.admin_state === 'cordoned' ? 'Cordoned · no new VMs; existing VMs keep running' : esc(h.admin_state), true],
      ['Host agent', faults.some((f) => f === 'kill-host' || f === 'killed') ? 'Stopped by Kill host; restart host to resume' : h.lease === 'healthy' ? 'Reporting heartbeats' : 'Not reporting; agent or connection unavailable', true],
      [h.lease === 'healthy' ? 'Last heartbeat' : 'Silent for', dur(h.lease_age_ms)],
      ['Slots', `${used} of ${h.slots.length} used`],
      ['Faults', faults.length ? `<span class="bad">${esc(faults.join(', '))}</span>` : '<span class="faint">none</span>', true],
      stale ? ['Stale copies (fenced)', `<span class="bad">${stale}</span>`, true] : null,
    ])}
    <div class="sub-head">Reported slots → logical VMs</div><div class="detail-links">${h.slots.filter((sl) => sl.vm_id).map((sl) => link('vm', sl.vm_id, `${vmName(sl.vm_id)} · slot g${sl.gen} → current VM`)).join('') || '<span class="dim">No VMs assigned.</span>'}</div>
    <div class="sub-head">Recent events</div>${eventRows(events, 'None in the log window.')}</div>`;
}

function rackDetail(id, s, v) {
  const r = s.racks.find((x) => x.id === id);
  if (!r) return `${head(`Rack ${id}`, badge('gone', 'var(--faint)'))}<div class="detail-body"><div class="empty">This rack is no longer in the fleet.</div></div>`;
  const hosts = s.hosts.filter((h) => h.rack === id && h.admin_state !== 'removed');
  const slots = hosts.flatMap((h) => h.slots);
  const count = (lease) => hosts.filter((h) => h.lease === lease).length;
  const events = last(v.events, 20, (e) => e.rack === id || hosts.some((h) => h.id === e.host_id));
  return `${head(`Rack ${id}`, r.partitioned ? badge('partitioned', 'var(--bad)') : badge('reachable', 'var(--ok)'), 'rack', id, v.playback)}
    <div class="detail-body">
    <p class="component-role">A shared failure domain containing physical hosts. There is no rack scheduler: each host agent communicates directly with the single fleet scheduler.</p>
    ${kv([
      ['Hosts', hosts.length],
      ['Slots', `${slots.filter((sl) => sl.vm_id).length} of ${slots.length} used`],
      ['Leases', `${count('healthy')} healthy · ${count('suspect')} suspect · ${count('lost')} lost`, true],
    ])}
    <div class="sub-head">Hosts in this rack</div><div class="detail-links">${hosts.map((h) => link('host', h.id, `Host ${h.id}`)).join('') || '<span class="dim">No hosts.</span>'}</div>
    <div class="sub-head">Recent events</div>${eventRows(events, 'None in the log window.')}</div>`;
}

function schedulerDetail(s, v) {
  const c = s.counters;
  const status = s.scheduler || 'down';
  const rec = Object.entries(c.recoveries || {}).map(([k, n]) => `${n} ${esc(k)}`).join(' · ');
  const events = last(v.events, 20, (e) => ['reconcile', 'alert', 'fencing'].includes(e.kind) || (e.kind === 'chaos' && /scheduler|store/.test(e.detail)));
  return `${head('Scheduler', badge(status, SCHED[status] || 'var(--faint)'), 'scheduler', 'scheduler', v.playback)}
    <div class="detail-body">
    <p class="component-role">One logical scheduler places VMs across every rack. It sends commands to host agents and reconciles their inventories; guests do not connect to it directly.</p>
    ${kv([
      ['Epoch', s.epoch],
      ['Queue', `${c.queue_depth} of ${c.queue_cap}`],
      ['Placed', c.placed],
      ['Queued', c.queued],
      ['Refused (429)', c.rejected_429],
      ['Shed', c.shed],
      ['Recoveries', rec || '<span class="faint">none</span>', true],
    ])}
    <div class="sub-head">Racks in this fleet</div><div class="detail-links">${s.racks.map((r) => link('rack', r.id, `Rack ${r.id}`)).join('') || '<span class="dim">No racks.</span>'}</div>
    <div class="sub-head">Reconcile, fencing and alerts</div>${eventRows(events, 'None in the log window.')}</div>`;
}

export function render(st, v) {
  const el = $('#detail');
  const s = v.snapshot;
  const sel = st.selection;
  el.hidden = !s || !sel;
  if (!s || !sel) {
    mount(el, '');
    el.__selection = null;
    return;
  }
  const scroll = el.__selection === sel ? el.querySelector('.detail-body')?.scrollTop || 0 : 0;
  const active = el.__selection === sel && document.activeElement && el.contains(document.activeElement) ? document.activeElement : null;
  let html;
  if (sel.kind === 'vm' || sel.kind === 'guest') html = vmDetail(sel.id, s, v, sel.kind === 'guest' ? sel : null);
  else if (sel.kind === 'host' || sel.kind === 'agent') html = hostDetail(sel.id, s, v, st.catalog, sel.kind === 'agent');
  else if (sel.kind === 'rack') html = rackDetail(sel.id, s, v);
  else html = schedulerDetail(s, v);
  mount(el, navigation(sel, s) + html + '<div class="detail-scroll-hint">Scroll inspector for properties and history ↓</div>');
  if (active?.matches('button')) {
    const replacement = [...el.querySelectorAll('button')].find((button) => ['act', 'kind', 'id', 'host', 'gen'].every((key) => button.dataset[key] === active.dataset[key]));
    replacement?.focus({ preventScroll: true });
  }
  el.querySelector('.detail-body').scrollTop = scroll;
  el.querySelector('.detail-scroll-hint').hidden = el.querySelector('.detail-body').scrollHeight <= el.querySelector('.detail-body').clientHeight;
  el.__selection = sel;
}

export const actions = {
  deselect: () => update((st) => ((st.selection = null), (st.highlight = null))),
};
