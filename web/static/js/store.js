// The one piece of client state. Panels never keep their own copy: they are
// rebuilt from view() each time update() is called.

const HISTORY_MS = 5 * 60 * 1000; // chart window, in sim time
const MAX_EVENTS = 500;

export const state = {
  route: { page: 'live', id: null }, // live | runs | run
  conn: 'connecting', // connecting | live | reconnecting
  catalog: { actions: [], presets: [] },
  live: { snapshot: null, events: [], history: [] },
  runs: null, // []RunSummary once loaded
  run: null, // { id, report, frames, cursor, playing, rate, loading, error }
  selection: null, // { kind: vm|host|rack|scheduler, id }
  highlight: null, // VM id picked in the event log
  menu: null, // { kind, id, x, y }
  popover: null, // name of the open controls popover
  log: { kinds: new Set(), text: '' }, // empty set shows every kind
  chartClass: 'interactive',
  invariant: null, // invariant whose last violation is shown
  toasts: [],
};

const renderers = [];
let scheduled = false;
let held = false;
let dirty = false;

export function onRender(fn) {
  renderers.push(fn);
}

// update applies a change and schedules one render for this frame.
export function update(change) {
  if (change) change(state);
  if (scheduled) return;
  scheduled = true;
  requestAnimationFrame(() => {
    scheduled = false;
    if (held) {
      dirty = true;
      return;
    }
    for (const fn of renderers) fn(state);
  });
}

// hold(true) postpones rendering until hold(false). State keeps updating;
// only the redraw waits.
export function hold(on) {
  held = on;
  if (!on && dirty) {
    dirty = false;
    update();
  }
}

// sample is one point of chart history.
function sample(s) {
  return { at: s.at, latency: s.latency || {}, queue: s.counters?.queue_depth ?? 0, intensity: s.sim?.intensity ?? 0 };
}

export function pushSnapshot(s) {
  const live = state.live;
  const prev = live.snapshot;
  // Sim time going backwards means the fleet was reset: start the chart over.
  if (prev && s.at < prev.at) live.history = [];
  live.snapshot = s;
  const last = live.history.at(-1);
  if (!last || s.at > last.at) live.history.push(sample(s));
  const from = s.at - HISTORY_MS;
  while (live.history.length && live.history[0].at < from) live.history.shift();
  for (const e of s.recent || []) pushEvent(e);
}

export function pushEvent(e) {
  if (e.at < (state.live.since || 0)) return;
  const events = state.live.events;
  // The stream and each snapshot's recent list overlap; keep one copy.
  for (let i = events.length - 1; i >= 0 && i >= events.length - 120; i--) {
    if (events[i].seq === e.seq) return;
  }
  events.push(e);
  if (events.length > MAX_EVENTS) events.splice(0, events.length - MAX_EVENTS);
}

// view is what every panel draws: the live fleet, or a recorded run at its
// cursor. A recording carries its whole history, so the chart spans the run.
export function view() {
  const run = state.route.page === 'run' ? state.run : null;
  if (run) {
    if (!run.frames?.length) return { playback: true, snapshot: null, events: [], history: [], run };
    const frame = run.frames[run.cursor];
    if (run.derivedFor !== run.frames) {
      run.history = run.frames.map(sample);
      // Each frame carries the last few events, so neighbours overlap.
      const bySeq = new Map();
      for (const f of run.frames) for (const e of f.recent || []) bySeq.set(e.seq, e);
      for (const e of run.report?.markers || []) bySeq.set(e.seq, e);
      run.events = [...bySeq.values()].sort((a, b) => a.at - b.at || a.seq - b.seq);
      run.derivedFor = run.frames;
    }
    const events = run.events.filter((e) => e.at <= frame.at);
    return {
      playback: true,
      run,
      snapshot: frame,
      events,
      history: run.history,
      span: [run.frames[0].at, run.frames.at(-1).at],
      cursorAt: frame.at,
    };
  }
  const s = state.live.snapshot;
  return {
    playback: false,
    snapshot: s,
    events: state.live.events,
    history: state.live.history,
    span: s ? [s.at - HISTORY_MS, s.at] : [0, 1],
  };
}

export function toast(kind, head, body = '') {
  const t = { id: Math.random().toString(36).slice(2), kind, head, body };
  update((st) => st.toasts.push(t));
  setTimeout(() => update((st) => (st.toasts = st.toasts.filter((x) => x !== t))), kind === 'error' ? 5000 : 2200);
}
