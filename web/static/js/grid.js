// Physical containment, with the single scheduler above its host agents.
// Every component selects an inspector; faults are explicit inspector actions.
import { $, esc, mount, stateVar, stateMeaning, prio, dur, plural, vmName } from './util.js';

const LEASE = { healthy: 'var(--ok)', suspect: 'var(--warn)', lost: 'var(--bad)' };
const SCHED = { up: 'var(--ok)', reconciling: 'var(--warn)', down: 'var(--bad)' };
const UNPLACED = ['Pending', 'Failed', 'Terminated'];
const MAX_UNPLACED = 6;

const isSel = (st, kind, id) => st.selection?.kind === kind && st.selection.id === id;

export function vmChip(vm, st, slotState) {
  const cls = ['vm', `s-${vm.state.toLowerCase()}`];
  if (vm.compromised) cls.push('compromised');
  if (slotState === 'reserved' || slotState === 'releasing') cls.push(slotState);
  if (isSel(st, 'vm', vm.id)) cls.push('sel');
  if (st.highlight === vm.id) cls.push('hl');
  const p = prio(vm.priority);
  const note = vm.compromised
    ? 'hostile'
    : vm.state !== 'Ready' && vm.last_reason
      ? vm.last_reason.replace(/_/g, ' ')
      : '';
  return `<button class="${cls.join(' ')}" style="--c:${stateVar(vm.state)}" data-act="pick" data-kind="vm" data-id="${esc(vm.id)}"
    title="Inspect VM ${esc(vm.id)} and its history · ${esc(vm.state)} · gen ${vm.gen} · ${p.label}${note ? ' · ' + esc(note) : ''}">
    <span class="vm-top"><span class="vm-id">${esc(vmName(vm.id))}</span></span>
    <span class="vm-bot"><span class="vm-state" title="${esc(stateMeaning(vm.state))}">${esc(vm.state)}</span><span class="vm-gen" title="Generation: increases on re-placement">g${vm.gen}</span><span class="vm-prio" title="Priority: ${p.label}" style="--p:${p.color}">${p.short}</span></span>
  </button>`;
}

function slot(sl, vms, st) {
  if (!sl.vm_id) return `<span class="slot-free" title="An available VM slot on this physical host">Empty VM slot</span>`;
  const vm = vms.get(sl.vm_id);
  if (vm) return `<div class="vm-instance${isSel(st, 'guest', vm.id) ? ' guest-selected' : ''}">
    ${vmChip(vm, st, sl.state)}
    <button class="guest-node" data-act="pick" data-kind="guest" data-id="${esc(vm.id)}" title="Inspect guestd inside ${esc(vmName(vm.id))}. It reports readiness and heartbeats to this host’s agent, not the scheduler."><span aria-hidden="true">↳</span> guestd <span aria-hidden="true">›</span></button>
  </div>`;
  // The host holds a slot for a VM the scheduler no longer lists.
  return `<span class="slot-free orphan" title="${esc(sl.vm_id)} · ${esc(sl.state)}">${esc(vmName(sl.vm_id))} g${sl.gen ?? '?'}</span>`;
}

function hostCard(h, vms, st) {
  const cls = ['host', `lease-${h.lease}`, `admin-${h.admin_state}`];
  if (isSel(st, 'host', h.id) || isSel(st, 'agent', h.id)) cls.push('sel');
  const tags = [];
  if (h.admin_state !== 'active') tags.push(`<span class="tag admin">${esc(h.admin_state)}</span>`);
  for (const f of h.faults || []) tags.push(`<span class="tag fault">${esc(f)}</span>`);
  const stale = (h.stale || [])
    .map((s) => `<span class="stale" title="A copy the scheduler has fenced: its generation is older than the authoritative one.">stale copy <b>${esc(vmName(s.vm_id))}</b> g${s.gen}</span>`)
    .join('');
  return `<section class="${cls.join(' ')}" aria-label="Host ${esc(h.id)}" style="--lease:${LEASE[h.lease] || 'var(--faint)'}">
    <div class="host-head">
      <button class="host-id" data-act="pick" data-kind="host" data-id="${esc(h.id)}" title="Inspect physical host ${esc(h.id)} · ${h.slots.length} VM slots">Host ${esc(h.id)}</button>
      <button class="agent-node${isSel(st, 'agent', h.id) ? ' sel' : ''}" data-act="pick" data-kind="agent" data-id="${esc(h.id)}" title="Inspect the host agent on ${esc(h.id)} · lease ${esc(h.lease)} · last accepted heartbeat ${dur(h.lease_age_ms)} ago. Admits VMs, owns the driver and receives guestd reports."><i></i>Host agent${h.lease !== 'healthy' ? ` · ${esc(h.lease)}` : ''}<span aria-hidden="true">›</span></button>
    </div>
    ${tags.length ? `<div class="tags">${tags.join('')}</div>` : ''}
    <div class="slots" style="--n:${Math.max(1, h.slots.length)}">${h.slots.map((sl) => slot(sl, vms, st)).join('')}</div>
    ${stale ? `<div class="stales">${stale}</div>` : ''}
  </section>`;
}

function rackCol(r, hosts, vms, st) {
  const mine = r.hosts.map((id) => hosts.get(id)).filter((h) => h && h.admin_state !== 'removed');
  const used = mine.reduce((n, h) => n + h.slots.filter((s) => s.vm_id).length, 0);
  const total = mine.reduce((n, h) => n + h.slots.length, 0);
  const down = mine.filter((h) => h.lease === 'lost').length;
  const cls = ['rack'];
  if (r.partitioned) cls.push('partitioned');
  if (isSel(st, 'rack', r.id)) cls.push('sel');
  return `<section class="${cls.join(' ')}" aria-label="Rack ${esc(r.id)} failure domain">
    <button class="rack-head" data-act="pick" data-kind="rack" data-id="${esc(r.id)}" title="Inspect rack ${esc(r.id)}, its capacity and recent history">
      <span class="rack-title"><span class="rack-id">Rack ${esc(r.id)}</span><span class="rack-role">Failure domain${r.partitioned ? ' · <b class="bad">partitioned</b>' : ''}</span></span>
      <span class="rack-meta">${plural(mine.length, 'host')}<br>${used}/${total} slots${down ? ` · <span class="bad">${down} down</span>` : ''}</span>
      <span class="action-cue" aria-hidden="true">›</span>
    </button>
    ${mine.map((h) => hostCard(h, vms, st)).join('') || '<div class="empty">No hosts</div>'}
  </section>`;
}

function unplaced(s, st) {
  const loose = s.vms.filter((v) => !v.host && v.state !== 'Pending');
  if (!loose.length) return '';
  const order = [...UNPLACED, ...new Set(loose.map((v) => v.state).filter((x) => !UNPLACED.includes(x)))];
  const groups = order
    .map((name) => {
      const list = loose.filter((v) => v.state === name);
      if (!list.length) return '';
      // Newest first, so a long tail of old terminated VMs doesn't crowd out fresh ones.
      list.sort((a, b) => b.updated_at - a.updated_at);
      const more = list.length - MAX_UNPLACED;
      return `<div class="unplaced-group">
        <span class="unplaced-label"><i class="dot" style="--c:${stateVar(name)}"></i>${esc(name)} <b>${list.length}</b></span>
        <div class="unplaced-chips">${list.slice(0, MAX_UNPLACED).map((v) => vmChip(v, st)).join('')}${more > 0 ? `<span class="more">+${more} more</span>` : ''}</div>
      </div>`;
    })
    .join('');
  return `<div class="unplaced"><div class="unplaced-title">Not on a host</div>${groups}</div>`;
}

export function render(st, v) {
  const el = $('#fleet');
  const s = v.snapshot;
  if (!s) {
    const msg = v.playback ? v.run?.error || 'Loading recording…' : 'Waiting for the first snapshot…';
    mount(el, `<div class="empty">${esc(msg)}</div>`);
    return;
  }
  const vms = new Map(s.vms.map((x) => [x.id, x]));
  const hosts = new Map(s.hosts.map((h) => [h.id, h]));
  const sched = s.scheduler || 'down';
  mount(
    el,
    `<header class="fleet-heading">
      <h2>${st.source?.startsWith('Fixture') ? 'Fixture fleet' : 'Fleet topology'}</h2>
      <span class="dim">${plural(s.racks.length, 'rack')} · ${plural(s.hosts.filter((h) => h.admin_state !== 'removed').length, 'host')}</span>
      <span class="dim">Click any component to inspect</span>
    </header>
    <div class="control-plane">
      <button class="sched${isSel(st, 'scheduler', 'scheduler') ? ' sel' : ''}" style="--c:${SCHED[sched] || 'var(--faint)'}" data-act="pick" data-kind="scheduler" data-id="scheduler" title="Inspect scheduler status, authority epoch and recent history">
        <i></i><span class="sched-name"><strong>Scheduler</strong><small>Placement &amp; authority across every rack</small></span><span class="sched-status"><b>${esc(sched)}</b><small>epoch ${s.epoch}</small></span><span aria-hidden="true">›</span>
      </button>
      <div class="fleet-relationship"><span>↓ commands to host agents</span><span>↑ host inventory &amp; heartbeats</span></div>
    </div>
    <div class="racks">${s.racks.map((r) => rackCol(r, hosts, vms, st)).join('')}</div>
    ${unplaced(s, st)}`,
  );
}
