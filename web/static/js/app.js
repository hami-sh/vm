// Entry point: routing, the SSE connection, and one click handler for the
// whole page. Each module exports render(state, view) and a map of actions;
// elements name their action with data-act (click), data-change or data-input.
import { $, esc, mount } from './util.js';
import { state, update, onRender, view, hold, pushSnapshot, pushEvent } from './store.js';
import { api, connect } from './api.js';
import * as status from './status.js';
import * as grid from './grid.js';
import * as menu from './menu.js';
import * as controls from './controls.js';
import * as queue from './queue.js';
import * as events from './events.js';
import * as detail from './detail.js';
import * as evidence from './evidence.js';
import * as runs from './runs.js';

const modules = [status, grid, menu, controls, queue, events, detail, evidence, runs];
const pick = (key) => Object.assign({}, ...modules.map((m) => m[key] || {}));
const actions = pick('actions');
const changes = pick('changes');
const inputs = pick('inputs');

onRender((st) => {
  const v = view();
  $('#dash').hidden = st.route.page === 'runs';
  $('#runs').hidden = st.route.page !== 'runs';
  document.body.classList.toggle('playback', v.playback);
  for (const m of modules) m.render(st, v);
  mount(
    $('#toasts'),
    st.toasts.map((t) => `<div class="toast ${t.kind}"><b>${esc(t.head)}</b>${t.body ? `<span>${esc(t.body)}</span>` : ''}</div>`).join(''),
  );
});

// ----------------------------------------------------------------- routing
function route() {
  const [, page, id] = location.hash.replace(/^#/, '').split('/');
  const next = page === 'runs' ? (id ? { page: 'run', id: decodeURIComponent(id) } : { page: 'runs', id: null }) : { page: 'live', id: null };
  update((st) => {
    st.route = next;
    st.menu = null;
    st.selection = null;
    st.highlight = null;
  });
  runs.enter(next);
}
addEventListener('hashchange', route);

// ------------------------------------------------------------------ events
function dispatch(table, attr, e) {
  const el = e.target.closest?.(`[data-${attr}]`);
  const fn = el && table[el.dataset[attr]];
  if (fn) fn(el, e);
  return el;
}

document.addEventListener('click', (e) => {
  const el = dispatch(actions, 'act', e);
  // A click anywhere else dismisses the open menu.
  if (state.menu && !e.target.closest('#menu') && !el?.matches('[data-act=pick], [data-act=open-menu]')) {
    update((st) => (st.menu = null));
  }
});
document.addEventListener('change', (e) => dispatch(changes, 'change', e));
document.addEventListener('input', (e) => dispatch(inputs, 'input', e));
document.addEventListener('submit', (e) => {
  if (e.target.matches('[data-submit]')) {
    e.preventDefault();
    dispatch(actions, 'submit', e);
  }
});

// Hints are immediate and follow the pointer, rather than native titles
// appearing after a delay at an unrelated position.
const tip = $('#hover-tip');
let tipOwner = null;
function hideTip() {
  tip.hidden = true;
  tipOwner?.removeAttribute('aria-describedby');
  tipOwner = null;
}
document.addEventListener('pointermove', (e) => {
  const el = e.target.closest?.('[title], [data-tip]');
  if (!el) { hideTip(); return; }
  if (el.hasAttribute('title')) {
    el.dataset.tip = el.getAttribute('title');
    el.removeAttribute('title');
  }
  if (tipOwner !== el) {
    hideTip();
    tipOwner = el;
    el.setAttribute('aria-describedby', 'hover-tip');
  }
  tip.textContent = el.dataset.tip;
  tip.hidden = !tip.textContent;
  const box = tip.getBoundingClientRect();
  tip.style.left = `${Math.max(8, Math.min(e.clientX + 14, innerWidth - box.width - 8))}px`;
  tip.style.top = `${e.clientY + box.height + 22 < innerHeight ? e.clientY + 18 : Math.max(8, e.clientY - box.height - 12)}px`;
});
document.addEventListener('pointerdown', hideTip, true);
document.addEventListener('pointerleave', hideTip);
addEventListener('blur', hideTip);
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && (state.menu || state.invariant)) update((st) => ((st.menu = null), (st.invariant = null)));
  runs.key?.(e);
});

// Rendering waits while a pointer is down, so the element being clicked or
// dragged is never replaced under it. The run scrubber opts out: it must
// redraw the grid as it moves.
document.addEventListener('pointerdown', (e) => hold(!e.target.closest?.('[data-live]')), true);
for (const type of ['pointerup', 'pointercancel']) document.addEventListener(type, () => hold(false), true);
addEventListener('blur', () => hold(false));

// -------------------------------------------------------------------- start
connect({
  onSnapshot: (s) => update(() => pushSnapshot(s)),
  onEvent: (e) => update(() => pushEvent(e)),
  onState: (conn) => update((st) => (st.conn = conn)),
});
api.catalog().then(
  (catalog) => update((st) => (st.catalog = catalog)),
  () => {}, // the menus stay empty; the stream's state already shows the outage
);
api.source().then((source) => update((st) => {
  st.source = source.label;
  st.lifetimeConfig = source;
}), () => {});
route();
