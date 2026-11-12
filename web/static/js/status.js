// Top bar (navigation, time controls, connection) and the status bar.
import { $, esc, mount, clock, STATES, stateVar, badge } from './util.js';
import { api, attempt } from './api.js';

const SPEEDS = [1, 2, 5, 10];
const SCHED = { up: 'var(--ok)', reconciling: 'var(--warn)', down: 'var(--bad)' };

function nav(st) {
  const page = st.route.page;
  mount(
    $('#nav'),
    `<a href="#/" class="${page === 'live' ? 'on' : ''}">Live</a>
     <a href="#/runs" class="${page !== 'live' ? 'on' : ''}">Runs</a>`,
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
    `<button class="btn icon" data-act="${paused ? 'sim-play' : 'sim-pause'}" title="${paused ? 'Play' : 'Pause'} the sim clock" aria-label="${paused ? 'Play' : 'Pause'}">${paused ? '▶' : '❚❚'}</button>
     <button class="btn sm" data-act="sim-step" ${paused ? '' : 'disabled'} title="Advance a paused clock by 1 s">Step 1 s</button>
     <div class="seg" role="group" aria-label="Speed">${speeds
       .map((x) => `<button data-act="sim-speed" data-speed="${x}" class="${x === speed ? 'on' : ''}">${x}×</button>`)
       .join('')}</div>
     <span class="sim-clock${paused ? ' paused' : ''}" title="Sim clock (UTC)">${clock(s.at)}${paused ? ' paused' : ''}</span>
     <span class="sim-meta">seed <b>${seed}</b></span>
     ${scenario ? badge(scenario, 'var(--accent)') : ''}`,
  );
}

function right(st, v) {
  if (v.playback) {
    mount($('#topright'), `<span class="conn playback">Recording</span><a class="btn" href="#/">Back to live</a>`);
    return;
  }
  const label = { live: 'Live', reconnecting: 'Reconnecting…', connecting: 'Connecting…' }[st.conn];
  mount(
    $('#topright'),
    `<span class="conn ${st.conn}">${label}</span>
     ${st.route.page === 'live' ? '<button class="btn danger" data-act="reset" title="Back to a clean seeded fleet">Reset</button>' : ''}`,
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
  const fill = c.queue_cap ? c.queue_depth / c.queue_cap : 0;
  const states = STATES.map((name) => {
    const n = c.by_state?.[name] || 0;
    return `<span class="state-count${n ? '' : ' zero'}" title="${name}"><i class="dot" style="--c:${stateVar(name)}"></i>${name} <b>${n}</b></span>`;
  }).join('');
  const sched = s.scheduler || 'down';
  mount(
    el,
    `<div class="stat stat-states">${states}</div>
     <div class="stat"><span class="stat-label">Queue</span><span class="stat-value">${c.queue_depth}/${c.queue_cap}</span>
       <span class="meter${fill >= 1 ? ' full' : fill >= 0.75 ? ' hot' : ''}"><i style="width:${Math.min(100, fill * 100)}%"></i></span></div>
     <div class="stat" title="Creates refused with 429 because the queue was full"><span class="stat-label">429s</span><span class="stat-value${c.rejected_429 ? ' warn' : ''}">${c.rejected_429}</span></div>
     <div class="stat" title="Queued batch work dropped to make room"><span class="stat-label">Shed</span><span class="stat-value">${c.shed}</span></div>
     <div class="stat"><span class="stat-label">Placed</span><span class="stat-value">${c.placed}</span></div>
     <div class="stat"><span class="stat-label">Scheduler</span>${badge(sched, SCHED[sched] || 'var(--faint)')}<span class="dim mono">epoch ${s.epoch}</span></div>`,
  );
}

export function render(st, v) {
  nav(st);
  sim(st, v);
  right(st, v);
  statusbar(st, v);
}

export const actions = {
  'sim-pause': () => attempt('Paused', () => api.sim('pause')),
  'sim-play': () => attempt('Playing', () => api.sim('play')),
  'sim-step': () => attempt('Stepped 1 s', () => api.sim('step', { step_ms: 1000 })),
  'sim-speed': (el) => attempt(`Speed ${el.dataset.speed}×`, () => api.sim('speed', { speed: Number(el.dataset.speed) })),
  reset: () => {
    if (confirm('Reset to a clean seeded fleet? Every VM and all chaos is cleared.')) attempt('Fleet reset', () => api.chaos('reset'));
  },
};
