// Run with: node --test web/static/hierarchy_test.mjs
import test from 'node:test';
import assert from 'node:assert/strict';
import { render as grid } from './js/grid.js';
import { render as detail } from './js/detail.js';
import { render as menu } from './js/menu.js';

const fleet = { innerHTML: '', classList: { toggle() {} } };
let body = { scrollTop: 0 };
let inspectorHTML = '';
const inspector = {
  hidden: true,
  get innerHTML() { return inspectorHTML; },
  set innerHTML(html) { inspectorHTML = html; body = { scrollTop: 0 }; },
  querySelector: () => body,
};
const actions = { innerHTML: '', firstElementChild: null };
globalThis.document = { querySelector: (selector) => ({ '#fleet': fleet, '#detail': inspector, '#menu': actions })[selector] };

function snapshot() {
  const host = (id, gen, lease = 'healthy') => ({ id, rack: id, lease, lease_age_ms: 0, admin_state: 'active', slots: [{ vm_id: 'vm-a', gen, state: 'occupied' }] });
  return {
    at: 10000, scheduler: 'up', epoch: 1,
    racks: ['current', 'old', 'wrong-host', 'unreachable'].map((id) => ({ id, hosts: [id] })),
    hosts: [host('current', 2), host('old', 1, 'lost'), host('wrong-host', 2), host('unreachable', 2, 'lost')],
    vms: [{ id: 'vm-a', host: 'current', rack: 'current', gen: 2, state: 'Ready', priority: 2, created_at: 0, updated_at: 1000 }],
    queue: [], counters: {},
  };
}

function hostHTML(id) {
  return fleet.innerHTML.split(`aria-label="Host ${id}"`)[1].split('</section>')[0];
}

test('guest action history is not shown as a host fault, while persistent host faults remain visible', () => {
  const s = snapshot();
  const vmActions = ['kill-guestd', 'crash-vm', 'hang-next-boot', 'compromise', 'impostor-register', 'delete'];
  const catalog = { actions: [
    ...[...vmActions, 'future-vm-action'].map((name) => ({ name, target: 'vm', layer: 'guest' })),
    { name: 'slow-boot', target: 'host', layer: 'guest' },
    { name: 'fill-disk', target: 'host', layer: 'host' },
  ] };
  for (const mode of [{ catalog, playback: false }, { catalog, playback: true }, { playback: true }, { catalog: { actions: [] }, playback: true }]) {
    s.hosts[0].faults = [...vmActions, 'future-vm-action', 'slow-boot', 'fill-disk'];
    grid({ catalog: mode.catalog }, { snapshot: s, playback: mode.playback });
    const results = [hostHTML('current')];
    for (const kind of ['host', 'agent']) {
      detail({ catalog: mode.catalog, selection: { kind, id: 'current' } }, { snapshot: s, events: [], playback: mode.playback });
      results.push(inspector.innerHTML);
    }
    for (const html of results) {
      for (const name of vmActions) assert.doesNotMatch(html, new RegExp(name));
      assert.match(html, /slow-boot/);
      assert.match(html, /fill-disk/);
      if (mode.catalog === catalog) assert.doesNotMatch(html, /future-vm-action/);
      else assert.match(html, /future-vm-action/);
    }
    s.hosts[0].faults = vmActions;
    detail({ catalog: mode.catalog, selection: { kind: 'host', id: 'current' } }, { snapshot: s, events: [], playback: mode.playback });
    assert.match(inspector.innerHTML, /Faults<\/span><span class="v"><span class="faint">none/);
  }
  // An available catalog overrides historical fallback names, even in playback.
  s.hosts[0].faults = ['crash-vm'];
  grid({ catalog: { actions: [{ name: 'crash-vm', target: 'host', layer: 'guest' }] } }, { snapshot: s, playback: true });
  assert.match(hostHTML('current'), /crash-vm/);
});

test('host slots show their own generation and physical guest target, never another incarnation’s Ready state', () => {
  const s = snapshot();
  grid({}, { snapshot: s });
  assert.match(hostHTML('current'), /data-kind="guest"/);
  assert.match(hostHTML('current'), />Ready</);
  for (const id of ['old', 'wrong-host', 'unreachable']) {
    const html = hostHTML(id);
    assert.doesNotMatch(html, />Ready</);
    assert.match(html, new RegExp(`data-kind="guest"[^>]*data-host="${id}"[^>]*data-gen="${id === 'old' ? 1 : 2}"`));
    assert.match(html, /Current VM g2/);
  }
  assert.match(hostHTML('old'), /vm-a · g1/);
  assert.match(hostHTML('old'), /Stale copy/);
  assert.match(hostHTML('wrong-host'), /Unverified copy/);
  assert.match(hostHTML('current'), /data-kind="guest"[^>]*data-host="current"[^>]*data-gen="2"/);
  // Correct host and generation are still not fresh evidence when its lease is lost.
  s.hosts[0].lease = 'lost';
  grid({}, { snapshot: s });
  assert.doesNotMatch(hostHTML('current'), />Ready</);
  assert.match(hostHTML('current'), /Unverified copy/);
});

test('an open guest inspector stays pinned to its selected host and generation', () => {
  const s = snapshot();
  const selection = { kind: 'guest', id: 'vm-a', host: 'current', gen: 2 };
  const actionState = {
    menu: { kind: 'vm', id: 'vm-a', layer: 'guest', host: 'current', gen: 2 },
    catalog: { actions: [{ name: 'kill-guestd', label: 'Kill guestd', target: 'vm', layer: 'guest', params: [] }] },
  };
  detail({ selection }, { snapshot: s, events: [] });
  menu(actionState, { snapshot: s });
  assert.match(inspector.innerHTML, /data-layer="guest"/);
  assert.match(actions.innerHTML, /Kill guestd/);
  s.vms[0] = { ...s.vms[0], host: 'wrong-host', rack: 'wrong-host', gen: 3 };
  detail({ selection }, { snapshot: s, events: [] });
  menu(actionState, { snapshot: s });
  assert.equal(actions.innerHTML, '');
  const path = inspector.innerHTML.split('</nav>')[0];
  assert.match(path, /Host current/);
  assert.doesNotMatch(path, /Host wrong-host/);
  assert.match(inspector.innerHTML, /unverified/);
  assert.doesNotMatch(inspector.innerHTML, /data-act="open-menu"/);
  assert.match(inspector.innerHTML, /Current logical VM · g3 · wrong-host/);
  detail({ selection: { kind: 'guest', id: 'vm-a', host: 'old', gen: 1 } }, { snapshot: s, events: [] });
  assert.match(inspector.innerHTML.split('</nav>')[0], /Host old/);
  assert.match(inspector.innerHTML.split('</nav>')[0], /Host agent/);
  assert.match(inspector.innerHTML, /generation 1, on old/);
});

test('snapshot redraws preserve inspector scroll; a new selection starts at the top', () => {
  const s = snapshot();
  const selection = { kind: 'agent', id: 'current' };
  detail({ selection }, { snapshot: s, events: [] });
  body.scrollTop = 144;
  s.hosts[0].lease_age_ms = 2000;
  detail({ selection }, { snapshot: s, events: [] });
  assert.equal(body.scrollTop, 144);
  detail({ selection: { kind: 'agent', id: 'old' } }, { snapshot: s, events: [] });
  assert.equal(body.scrollTop, 0);
});
