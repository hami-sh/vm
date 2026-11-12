// Sim controls under the grid: load, topology, ambient chaos and presets.
// Sliders and filters reflect the snapshot's sim status; the small form
// fields (which pool, which rack) are the only values kept here.
import { $, esc, mount } from './util.js';
import { state, update, toast } from './store.js';
import { api, attempt } from './api.js';

const MAX_RATE = 5; // creates per sim second
const MIXES = {
  mixed: { label: 'Mixed', interactive: 2, standard: 5, batch: 3 }, // 20% / 50% / 30%
  interactive: { label: 'Interactive', interactive: 1, standard: 0, batch: 0 },
  batch: { label: 'Batch', interactive: 0, standard: 0, batch: 1 },
};
const LAYERS = [
  ['guest', 'Guest'],
  ['host', 'Host'],
  ['network', 'Network'],
  ['control', 'Control plane'],
  ['load', 'Load'],
  ['topology', 'Topology'],
];

const form = { pool: '', priority: 'standard', mix: 'mixed', rackHosts: 4, addTo: '', drain: '' };

const options = (list, selected) =>
  list.map(([value, label]) => `<option value="${esc(value)}"${value === selected ? ' selected' : ''}>${esc(label)}</option>`).join('');

// keep returns the chosen value if it is still on offer, else the first.
const keep = (value, list) => (list.includes(value) ? value : list[0] || '');

function load(s) {
  const pools = (s.pools || []).map((p) => p.name);
  form.pool = keep(form.pool, pools);
  return `<div class="ctl">
    <div class="ctl-title"><h2>Create a VM</h2><span class="dim">Starts a macOS VM. If there’s no space, it waits in the queue.</span></div>
    <button class="btn primary" data-act="create-vm" title="Create one macOS VM. Environment and queue priority can be changed in Advanced controls.">Create VM</button>
    <div class="ctl-row">
      <button class="btn" data-act="flood" data-count="10" title="Send 10 VM requests at once">Create 10 VMs</button>
      <button class="btn" data-act="flood" data-count="60" title="Send 60 requests at once to fill the fleet and test the queue limit of ${s.counters.queue_cap}. Excess requests may be refused.">Create 60 VMs</button></div>
  </div>`;
}

function requestSettings(s) {
  const rate = s.sim.load_rate || 0;
  const pools = (s.pools || []).map((p) => [p.name, `${p.image.replace(/macos-(\d+)-xcode(\d+)/, 'macOS $1 · Xcode $2')} · ${p.cpus} CPU · ${p.memory_mb / 1024} GB`]);
  return `<div class="ctl">
    <div class="ctl-title"><h2>Single-VM settings</h2><span class="dim">Optional settings for the Create VM button.</span></div>
    <div class="create-form">
      <label>Environment · software and VM size<select class="input" data-change="form" data-key="pool" title="Choose the macOS image, Xcode version, CPU and memory for the next single VM request" aria-label="VM environment">${options(pools, form.pool)}</select></label>
      <label>When the fleet is full, serve this request<select class="input" data-change="form" data-key="priority" title="Choose where the next single VM request waits in the queue: urgent before normal, background after normal" aria-label="Queue priority">${options([['interactive', 'First · urgent'], ['standard', 'Normally · default'], ['batch', 'Last · background work']], form.priority)}</select></label>
    </div>
    <div class="ctl-title"><h2>Automatic traffic</h2><span class="dim">Continuously creates VMs to test admission and recovery. Set to 0 to stop.</span></div>
    <div class="ctl-row"><span class="lbl">New VMs per sim-second</span>
      <input type="range" min="0" max="${MAX_RATE}" step="0.1" value="${rate}" data-input="slide" data-unit="/s" data-digits="1" data-change="load-rate" title="Set how many new VM requests arrive per simulated second. Set to 0 to stop automatic traffic." aria-label="Sustained create rate">
      <output>${rate.toFixed(1)}/s</output></div>
    <div class="ctl-row"><span class="lbl">Queue priority for automatic traffic</span>
      <div class="seg">${Object.entries(MIXES)
        .map(([key, m]) => `<button data-act="mix" data-mix="${key}" title="${key === 'mixed' ? '20% urgent, 50% normal, 30% background work' : key === 'interactive' ? 'All requests served ahead of normal or background work' : 'All requests can wait behind other work'}" class="${form.mix === key ? 'on' : ''}">${key === 'interactive' ? 'Urgent' : key === 'batch' ? 'Background' : m.label}</button>`)
        .join('')}</div></div>
  </div>`;
}

function topology(s) {
  const racks = s.racks.map((r) => r.id);
  const hosts = s.hosts.filter((h) => h.admin_state !== 'removed').map((h) => h.id);
  form.addTo = keep(form.addTo, racks);
  form.drain = keep(form.drain, hosts);
  return `<div class="ctl">
    <div class="ctl-title"><h2>Topology</h2><span class="dim">grow, shrink and drain</span></div>
    <div class="ctl-row"><span class="lbl">Rack</span>
      <button class="btn" data-act="add-rack" title="Add a new rack with the number of hosts specified beside this button">Add rack</button>
      <span class="dim">with</span>
      <input class="input num" type="number" min="1" max="8" value="${form.rackHosts}" data-change="form" data-key="rackHosts" title="Choose how many hosts the Add rack button creates, from 1 to 8" aria-label="Hosts in the new rack">
      <span class="dim">hosts</span></div>
    <div class="ctl-row"><span class="lbl">Host</span>
      <button class="btn" data-act="add-host" title="Add one host to the rack selected beside this button" ${racks.length ? '' : 'disabled'}>Add host</button>
      <span class="dim">to</span>
      <select class="input" data-change="form" data-key="addTo" title="Choose which rack receives the new host when you click Add host" aria-label="Rack to grow">${options(racks.map((r) => [r, `rack ${r}`]), form.addTo)}</select></div>
    <div class="ctl-row"><span class="lbl">Drain</span>
      <select class="input" data-change="form" data-key="drain" title="Choose the host to stop placing new VMs on when you click Drain" aria-label="Host to drain">${options(hosts.map((h) => [h, h]), form.drain)}</select>
      <button class="btn" data-act="drain" ${hosts.length ? '' : 'disabled'} title="Stop new placements now; move its VMs after the grace period">Drain</button></div>
  </div>`;
}

// active is the set of ambient fault names currently allowed. The API's empty
// filter list means all of them.
function active(s, ambient) {
  return s.sim.filters?.length ? new Set(s.sim.filters) : new Set(ambient.map((a) => a.name));
}

function chaos(s, catalog) {
  const intensity = s.sim.intensity || 0;
  const ambient = catalog.actions.filter((a) => a.ambient);
  const on = active(s, ambient);
  const layers = LAYERS.map(([layer, label]) => {
    const names = ambient.filter((a) => a.layer === layer).map((a) => a.name);
    if (!names.length) return '';
    const n = names.filter((x) => on.has(x)).length;
    return `<button class="chip${n ? ' on' : ''}" data-act="filter" data-layer="${layer}" title="Toggle random ${label.toLowerCase()} faults: ${esc(names.join(', '))}">${label} <b>${n}</b></button>`;
  }).join('');
  const scope = s.sim.racks || [];
  const racks = s.racks
    .map((r) => `<button class="chip${scope.includes(r.id) ? ' on' : ''}" data-act="scope" data-rack="${esc(r.id)}" title="Toggle rack ${esc(r.id)} in the set affected by random faults">${esc(r.id)}</button>`)
    .join('');
  return `<div class="ctl">
    <div class="ctl-title"><h2>Ambient chaos</h2><span class="dim">seeded random faults</span></div>
    <div class="ctl-row"><span class="lbl">Intensity</span>
      <input type="range" min="0" max="100" step="1" value="${Math.round(intensity * 100)}" data-input="slide" data-unit="%" data-digits="0" data-change="intensity" title="Control the frequency of random faults. Set to 0 to stop injecting new random faults." aria-label="Ambient chaos intensity">
      <output>${Math.round(intensity * 100)}%</output></div>
    <div class="ctl-row"><span class="lbl">Faults</span>${layers || '<span class="faint">catalogue not loaded</span>'}</div>
    <div class="ctl-row"><span class="lbl">Racks</span>
      <button class="chip${scope.length ? '' : ' on'}" data-act="scope" data-rack="" title="Allow random faults to affect any rack">all</button>${racks}</div>
  </div>`;
}

const SCENARIOS = {
  'split-brain': 'Disconnect a rack while its VMs keep running, then restore communication',
  'gray-failure': 'Silence health reports from up to three hosts, then resume them',
  'thundering-herd': 'Fill most capacity, power off a rack, then flood the remaining fleet with urgent requests',
  'rolling-maintenance': 'Drain and remove racks one at a time',
  'hostile-tenant': 'Compromise up to three existing VMs and submit a burst of new requests',
  'bad-day': 'Combine random faults, network delay, a request burst and a scheduler restart',
};

function presets(catalog, ready) {
  return `<div class="ctl wide">
    <div class="ctl-title"><h2>Scenarios</h2><span class="dim">Start a fault sequence and record it</span></div>
    <div class="ctl-row">
      <div class="presets">${catalog.presets
        .map((p) => `<button class="btn" data-act="preset" data-name="${esc(p.name)}" ${ready ? '' : 'disabled'} title="${ready ? `${esc(SCENARIOS[p.name] || `Start ${p.label}`)}. Records a run for playback and inspection in Runs.` : 'Scheduler is recovering; scenarios start once it accepts work'}">${esc(p.label)}</button>`)
        .join('')}</div>
      </div>
  </div>`;
}

export function render(st, v) {
  const el = $('#controls');
  const s = v.snapshot;
  if (!s || v.playback) {
    mount(el, '');
    return;
  }
  mount(el, load(s) + presets(st.catalog, s.scheduler === 'up') + '<h2 class="advanced-title">Advanced controls</h2><div class="advanced-controls" aria-label="Advanced controls">' + requestSettings(s) + topology(s) + chaos(s, st.catalog) + '<div class="ctl"><button class="btn" data-act="open-menu" data-kind="global" title="Inject latency, packet loss, duplication or reordering">Network faults…</button></div></div>');
}

// After a slider request the panel is redrawn from the snapshot even if the
// markup is unchanged, so a refused value does not stay on the slider.
async function settle(request) {
  await request;
  $('#controls').__html = null;
  update();
}

const sim = () => state.live.snapshot?.sim || {};

export const inputs = {
  slide(el) {
    el.nextElementSibling.textContent = `${Number(el.value).toFixed(Number(el.dataset.digits))}${el.dataset.unit}`;
  },
};

export const changes = {
  form(el) {
    form[el.dataset.key] = el.type === 'number' ? Math.max(1, Math.min(8, Number(el.value) || 1)) : el.value;
    update();
  },
  'load-rate'(el) {
    const rate = Number(el.value);
    settle(attempt(rate ? `Load ${rate.toFixed(1)} creates/s` : 'Load off', () => api.sim('chaos', { load_rate: rate })));
  },
  intensity(el) {
    const x = Number(el.value) / 100;
    settle(attempt(x ? `Ambient chaos ${el.value}%` : 'Ambient chaos off', () => api.sim('chaos', { intensity: x })));
  },
};

export const actions = {
  async mix(el) {
    const key = el.dataset.mix;
    const { label, ...weights } = MIXES[key];
    const ok = await attempt(`Priority mix: ${label.toLowerCase()}`, () => api.chaos('sustained-load', '', { rate: sim().load_rate || 0, ...weights }));
    if (ok) {
      form.mix = key;
      update();
    }
  },
  'create-vm': () => attempt(`Create ${form.priority} VM in ${form.pool}`, () => api.createVM(form.pool, form.priority)),
  flood: (el) => attempt(`Flood ${el.dataset.count}`, () => api.chaos('flood', '', { count: Number(el.dataset.count) })),
  'add-rack': () => attempt(`Add a rack of ${form.rackHosts}`, () => api.topology('add-rack', { hosts: form.rackHosts })),
  'add-host': () => attempt(`Add a host to rack ${form.addTo}`, () => api.topology('add-host', { rack: form.addTo })),
  drain: () => attempt(`Drain ${form.drain}`, () => api.topology('drain', { host: form.drain })),
  preset: (el) => attempt(`Scenario: ${el.textContent}`, () => api.chaos(el.dataset.name)),
  filter(el) {
    const ambient = state.catalog.actions.filter((a) => a.ambient);
    const on = active({ sim: sim() }, ambient);
    const names = ambient.filter((a) => a.layer === el.dataset.layer).map((a) => a.name);
    const wasOn = names.some((n) => on.has(n));
    for (const n of names) wasOn ? on.delete(n) : on.add(n);
    if (!on.size) {
      toast('error', 'No fault types left', 'Set intensity to 0 to stop ambient chaos.');
      return;
    }
    const filters = on.size === ambient.length ? [] : [...on];
    attempt(`${el.dataset.layer} faults ${wasOn ? 'off' : 'on'}`, () => api.sim('chaos', { filters }));
  },
  scope(el) {
    const rack = el.dataset.rack;
    const all = (state.live.snapshot?.racks || []).map((r) => r.id);
    let racks = [];
    if (rack) {
      const cur = new Set(sim().racks || []);
      cur.has(rack) ? cur.delete(rack) : cur.add(rack);
      racks = cur.size === all.length ? [] : all.filter((id) => cur.has(id));
    }
    attempt(racks.length ? `Ambient chaos only in ${racks.join(', ')}` : 'Ambient chaos in every rack', () => api.sim('chaos', { racks }));
  },
};
