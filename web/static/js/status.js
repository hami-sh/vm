// Top bar (navigation, time controls, connection) and the status bar.
import { $, esc, mount, clock, STATES, stateVar, stateMeaning, badge } from './util.js';
import { api, attempt } from './api.js';
import { state, update, pushSnapshot } from './store.js';

const SPEEDS = [1, 2, 5, 10];

function nav(st) {
  const page = st.route.page;
  mount(
    $('#nav'),
    `<a href="#/" title="Watch and control the live fleet" class="${page === 'live' ? 'on' : ''}">Live</a>
     <a href="#/runs" title="Browse recorded scenarios, replay snapshots and inspect their reports" class="${page !== 'live' ? 'on' : ''}">Runs</a>`,
  );
}

function sim(st, v) {
  const s = v.snapshot;
  if (st.route.page !== 'live' || !s) {
    mount($('#sim'), '');
    return;
  }
  const { speed, paused, seed, scenario } = s.sim;
  const speeds = SPEEDS.includes(speed) ? SPEEDS : [...SPEEDS, speed].sort((a, b) => a - b);
  mount(
    $('#sim'),
    `<span class="ctl-label">Simulation clock</span><button class="btn" data-act="${paused ? 'sim-play' : 'sim-pause'}" title="${paused ? 'Resume' : 'Freeze'} simulation time" aria-label="${paused ? 'Play' : 'Pause'}">${paused ? '▶ Resume' : '❚❚ Pause'}</button>
     <button class="btn sm" data-act="sim-step" ${paused ? '' : 'disabled'} title="Advance a paused clock by 1 s">Step 1 s</button>
     <div class="seg" role="group" aria-label="Simulation speed">${speeds
       .map((x) => `<button data-act="sim-speed" data-speed="${x}" title="Run at ${x} times real time" class="${x === speed ? 'on' : ''}">${x}×</button>`)
       .join('')}</div>
     <span class="sim-clock${paused ? ' paused' : ''}" title="Simulated time (UTC); pauses and speeds affect this clock">${clock(s.at)}</span>
     <span class="sim-meta" title="Seed used for repeatable chaos choices">seed <b>${seed}</b></span>
     ${scenario ? badge(scenario, 'var(--accent)') : ''}`,
  );
}

function statusbar(st, v) {
  const el = $('#status');
  const s = v.snapshot;
  el.hidden = st.route.page === 'runs';
  if (!s) {
    mount(el, '');
    return;
  }
  const c = s.counters;
  const states = STATES.filter((name) => name !== 'Pending' && (c.by_state?.[name] || name === 'Ready')).map((name) => {
    const n = c.by_state?.[name] || 0;
    return `<span class="state-count${n ? '' : ' zero'}" title="${esc(stateMeaning(name))}"><i class="dot" style="--c:${stateVar(name)}"></i>${name} <b>${n}</b></span>`;
  }).join('');
  mount(
    el,
    `<div class="stat stat-states">${states}</div>
     <div class="stat" title="Creates refused with 429 because the queue was full"><span class="stat-label">429s</span><span class="stat-value${c.rejected_429 ? ' warn' : ''}">${c.rejected_429}</span></div>
     <div class="stat" title="Queued batch work dropped to make room"><span class="stat-label">Shed</span><span class="stat-value">${c.shed}</span></div>`,
  );
}

export function render(st, v) {
  nav(st);
  sim(st, v);
  document.body.classList.toggle('disconnected', st.conn !== 'live');
  statusbar(st, v);
}

export const actions = {
  'sim-pause': () => setPaused(true),
  'sim-play': () => setPaused(false),
  'sim-step': () => attempt('Stepped 1 s', () => api.sim('step', { step_ms: 1000 })),
  'sim-speed': (el) => attempt(`Speed ${el.dataset.speed}×`, () => api.sim('speed', { speed: Number(el.dataset.speed) })),
  reset: async () => {
    if (!confirm('Reset to an empty fleet? Every VM, traffic source and scheduled fault is cleared.')) return;
    const result = await attempt('Fleet reset', () => api.chaos('reset'));
    if (!result) return;
    const snapshot = await api.snapshot();
    update((st) => {
      st.live = { snapshot: null, history: [], events: [], since: snapshot.at };
      st.selection = st.menu = st.highlight = st.invariant = null;
      pushSnapshot(snapshot);
    });
  },
};

async function setPaused(paused) {
  const result = await attempt(paused ? 'Paused' : 'Playing', () => api.sim(paused ? 'pause' : 'play'));
  if (result) update(() => { state.live.snapshot.sim.paused = result.paused; });
}
