// Explicit fault menus for inspected components. Nothing here knows the action names in advance
// except the small tables of defaults below.
import { $, esc, mount, vmName, plural } from './util.js';
import { state, update } from './store.js';
import { api, attempt } from './api.js';

const LAYERS = [
  ['guest', 'Guest'],
  ['host', 'Host'],
  ['network', 'Network'],
  ['control', 'Control plane'],
  ['load', 'Load'],
  ['topology', 'Topology'],
];

// The catalogue names each action's parameters but not their defaults.
const DEFAULTS = {
  'slow-boot': { factor: 5 },
  'clock-skew': { ms: 30000 },
  'latency-spike': { ms: 200 },
  'packet-loss': { percent: 10 },
  'slow-store': { ms: 200 },
  'burst-after-rack-loss': { count: 30 },
  'add-host': { hosts: 1 },
};

// Faults that stay on until the same action is sent again with these values.
const OFF = {
  'slow-boot': { factor: 1 },
  'clock-skew': { ms: 0 },
  'fill-disk': { on: 0 },
  'latency-spike': { ms: 0 },
  'packet-loss': { percent: 0 },
  'duplicate-messages': { percent: 0 },
  'reorder-messages': { on: 0 },
  'slow-store': { ms: 0 },
};

// Fleet-wide actions that have their own control elsewhere on the page.
const ELSEWHERE = new Set(['reset', 'noise', 'sustained-load', 'flood', 'add-rack']);
const DESTRUCTIVE = new Set(['delete', 'wipe-store', 'remove-rack', 'power-off-rack']);

const EFFECTS = {
  'kill-guestd': 'Stop the in-VM health agent to test detection and replacement',
  'crash-vm': 'Crash the VM to test automatic recovery',
  'hang-next-boot': 'Make the next boot hang to test the boot deadline',
  'slow-boot': 'Multiply boot time on this host by the specified factor',
  compromise: 'Make the guest attempt unauthorized control-plane operations',
  'kill-host': 'Power off this host and its VMs to test replacement elsewhere',
  'pause-heartbeats': 'Stop host health reports while its VMs keep running',
  'restart-host': 'Restart the host agent and recover its running VM inventory',
  'clock-skew': 'Offset this host’s clock by the specified milliseconds',
  'fill-disk': 'Simulate a full disk so this host cannot accept new VMs',
  cordon: 'Stop placing new VMs on this host',
  'partition-rack': 'Disconnect this rack from the scheduler while its VMs keep running',
  'one-way-partition': 'Drop communication in one direction between this host and the scheduler',
  'latency-spike': 'Add the specified milliseconds of network delay',
  'packet-loss': 'Drop the specified percentage of network messages',
  'duplicate-messages': 'Deliver duplicate network messages to test safe retries',
  'reorder-messages': 'Deliver network messages out of order',
  'kill-scheduler': 'Stop scheduling while existing host VMs keep running',
  'wipe-store': 'Erase scheduler state to test recovery from host inventories',
  'slow-store': 'Delay each durable scheduler commit by the specified milliseconds',
  'burst-after-rack-loss': 'Power off this rack and submit the specified number of VM requests',
  'remove-rack': 'Drain the rack’s hosts and remove them after they are empty',
  'heal-rack': 'Restore communication between this rack and the scheduler',
  'power-off-rack': 'Power off every host and VM in this rack',
  'resume-heartbeats': 'Resume host health reports to the scheduler',
  'restart-scheduler': 'Restart the scheduler and reconcile its durable state',
  uncordon: 'Allow new VM placements on this host again',
  drain: 'Stop new placements, then move remaining VMs after the grace period',
  'add-host': 'Add hosts to this rack using the count beside this button',
  'impostor-register': 'Attempt a forged host registration from this guest',
  delete: 'Delete this VM and release its capacity',
};

// describe returns the menu's heading for a target, or null if it is gone.
function describe(kind, id, s) {
  if (kind === 'vm') {
    const vm = s.vms.find((x) => x.id === id);
    return vm && { name: vmName(id), sub: `${vm.state} · gen ${vm.gen}${vm.host ? ` · ${vm.host}` : ''}` };
  }
  if (kind === 'host') {
    const h = s.hosts.find((x) => x.id === id);
    return h && { name: id, sub: `lease ${h.lease}${h.admin_state !== 'active' ? ` · ${h.admin_state}` : ''}` };
  }
  if (kind === 'rack') {
    const r = s.racks.find((x) => x.id === id);
    return r && { name: `Rack ${id}`, sub: `${plural(r.hosts.length, 'host')}${r.partitioned ? ' · partitioned' : ''}` };
  }
  if (kind === 'scheduler') return { name: 'Scheduler', sub: `${s.scheduler} · epoch ${s.epoch}` };
  return { name: 'Whole fleet', sub: 'network faults' };
}

// recoveries names the actions that would undo what is wrong with the target
// right now. They are listed first.
function recoveries(kind, id, s) {
  const out = new Set();
  if (kind === 'host') {
    const h = s.hosts.find((x) => x.id === id);
    const faults = h?.faults || [];
    if (faults.includes('pause-heartbeats')) out.add('resume-heartbeats');
    if (h?.admin_state === 'cordoned') out.add('uncordon');
    if (faults.some((f) => f !== 'pause-heartbeats' && f !== 'cordon')) out.add('restart-host');
  }
  if (kind === 'rack' && s.racks.find((x) => x.id === id)?.partitioned) out.add('heal-rack');
  if (kind === 'scheduler' && s.scheduler === 'down') out.add('restart-scheduler');
  return out;
}

function item(a, recover) {
  const defaults = DEFAULTS[a.name] || {};
  const params = (a.params || [])
    .map(
      (p) =>
        `<span class="menu-param">${esc(p)}<input type="number" step="any" data-param="${esc(p)}" value="${defaults[p] ?? 1}" title="Set ${esc(p === 'ms' ? 'milliseconds' : p === 'factor' ? 'the slowdown multiplier' : p === 'percent' ? 'the percentage' : p)} before applying ${esc(a.label.toLowerCase())}" aria-label="${esc(a.label)} ${esc(p)}"></span>`,
    )
    .join('');
  const off = OFF[a.name]
    ? `<button class="menu-off" data-act="chaos" data-action="${esc(a.name)}" data-off="1" title="Turn ${esc(a.label.toLowerCase())} off">off</button>`
    : '';
  const cls = ['menu-item'];
  if (recover) cls.push('suggested');
  if (DESTRUCTIVE.has(a.name)) cls.push('danger');
  return `<div class="${cls.join(' ')}">
    <button data-act="chaos" data-action="${esc(a.name)}" title="${esc(EFFECTS[a.name] || `Apply ${a.label.toLowerCase()} to the selected ${a.target}`)}"><span>${esc(a.label)}</span></button>${params}${off}
  </div>`;
}

function build(m, s, catalog) {
  const head = describe(m.kind, m.id, s);
  if (!head) return '';
  const mine = catalog.actions.filter((a) => a.target === m.kind && (!m.layer || a.layer === m.layer) && !ELSEWHERE.has(a.name));
  const fix = recoveries(m.kind, m.id, s);
  const groups = [];
  const first = mine.filter((a) => fix.has(a.name));
  if (first.length) groups.push(['Recover', first, true]);
  for (const [layer, label] of LAYERS) {
    const list = mine.filter((a) => a.layer === layer && !fix.has(a.name));
    if (list.length) groups.push([label, list, false]);
  }
  const known = new Set(LAYERS.map(([layer]) => layer));
  const other = mine.filter((a) => !known.has(a.layer) && !fix.has(a.name));
  if (other.length) groups.push(['Other', other, false]);
  const body = groups.length
    ? groups
        .map(
          ([label, list, recover]) =>
            `<div class="menu-group"><div class="menu-label">${label}</div>${list.map((a) => item(a, recover)).join('')}</div>`,
        )
        .join('')
    : `<div class="menu-note">${catalog.actions.length ? 'No actions for this target.' : 'The action catalogue has not loaded.'}</div>`;
  return `<div class="menu" role="menu" aria-label="Chaos actions for ${esc(head.name)}">
    <div class="menu-head"><b>${esc(head.name)}</b><span class="sub">${esc(head.sub)}</span><button class="menu-x" data-act="menu-close" title="Close this action menu without applying a fault" aria-label="Close">×</button></div>
    ${body}
  </div>`;
}

export function render(st, v) {
  const el = $('#menu');
  const m = st.menu;
  if (!m || v.playback || !v.snapshot) {
    mount(el, '');
    return;
  }
  mount(el, build(m, v.snapshot, st.catalog));
  const box = el.firstElementChild;
  if (!box) return;
  // Open beside the pointer, pulled back inside the window.
  const r = box.getBoundingClientRect();
  box.style.left = `${Math.max(8, Math.min(m.x + 8, innerWidth - r.width - 8))}px`;
  box.style.top = `${Math.max(8, Math.min(m.y + 8, innerHeight - r.height - 8))}px`;
}

// anchor is where a menu opens: the pointer, or under the element when the
// click came from the keyboard.
function anchor(el, e) {
  if (e.clientX || e.clientY) return { x: e.clientX, y: e.clientY };
  const r = el.getBoundingClientRect();
  return { x: r.left, y: r.bottom };
}

export const actions = {
  pick(el) {
    const { kind, id } = el.dataset;
    update((st) => {
      st.selection = { kind, id };
      st.menu = null;
    });
  },
  'open-menu'(el, e) {
    const { kind, id = '', layer } = el.dataset;
    update((st) => (st.menu = { kind, id, layer, ...anchor(el, e) }));
  },
  'menu-close': () => update((st) => (st.menu = null)),
  chaos(el) {
    const m = state.menu;
    if (!m) return;
    const name = el.dataset.action;
    let params;
    if (el.dataset.off) {
      params = OFF[name];
    } else {
      for (const input of el.closest('.menu-item').querySelectorAll('[data-param]')) {
        const n = Number(input.value);
        if (input.value === '' || !Number.isFinite(n)) {
          input.focus();
          return;
        }
        (params ||= {})[input.dataset.param] = n;
      }
    }
    const target = m.kind === 'global' || m.kind === 'scheduler' ? '' : m.id;
    const label = state.catalog.actions.find((a) => a.name === name)?.label || name;
    const what = `${label}${el.dataset.off ? ' off' : ''}${target ? ` · ${m.kind === 'vm' ? vmName(target) : target}` : ''}`;
    update((st) => (st.menu = null));
    attempt(what, () => api.chaos(name, target, params));
  },
};
