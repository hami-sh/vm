// The Runs page. The list comes from GET /v1/runs; opening a run plays its
// recorded snapshots through the same panels as the live fleet (see view() in
// store.js). This module adds the scrubber bar and the report.
import { $, esc, mount, clock, dur, secs, badge, EVENT_KINDS, shorten, title } from './util.js';
import { state, update } from './store.js';
import { api } from './api.js';

const RATES = [1, 2, 4, 8]; // frames per tick
const TICK_MS = 100;
let timer = null;

const verdict = (passed) => (passed ? badge('passed', 'var(--ok)') : badge('failed', 'var(--bad)', 'solid'));
const isViolation = (e) => e.kind === 'invariant';

// ------------------------------------------------------------------ loading
export function enter(route) {
  stop();
  if (route.page === 'runs') {
    update((st) => (st.run = null));
    api.runs().then(
      (list) => update((st) => (st.runs = list || [])),
      (err) => update((st) => (st.runs = { error: err.message })),
    );
    return;
  }
  if (route.page !== 'run') {
    update((st) => (st.run = null));
    return;
  }
  const run = { id: route.id, report: null, frames: null, cursor: 0, playing: false, rate: 2, loading: true, error: null };
  update((st) => (st.run = run));
  Promise.all([api.run(route.id), api.frames(route.id)]).then(
    ([report, frames]) => {
      if (state.run !== run) return; // the user moved on
      update(() => {
        run.report = report;
        run.frames = frames;
        run.loading = false;
        if (!frames.length) run.error = 'This run has no recorded snapshots.';
      });
    },
    (err) => {
      if (state.run !== run) return;
      update(() => {
        run.loading = false;
        run.error = `Could not load ${route.id}: ${err.message}`;
      });
    },
  );
}

// ----------------------------------------------------------------- playback
function stop() {
  clearInterval(timer);
  timer = null;
  if (state.run) state.run.playing = false;
}

function play() {
  const run = state.run;
  if (!run?.frames?.length) return;
  if (run.cursor >= run.frames.length - 1) run.cursor = 0;
  run.playing = true;
  timer = setInterval(() => {
    update(() => {
      run.cursor = Math.min(run.frames.length - 1, run.cursor + run.rate);
      if (run.cursor >= run.frames.length - 1) stop();
    });
  }, TICK_MS);
}

function seek(index) {
  const run = state.run;
  if (!run?.frames?.length) return;
  update(() => (run.cursor = Math.max(0, Math.min(run.frames.length - 1, index))));
}

// frameAt is the first frame at or after a sim time.
function frameAt(run, at) {
  const i = run.frames.findIndex((f) => f.at >= at);
  return i < 0 ? run.frames.length - 1 : i;
}

// --------------------------------------------------------------------- list
function list(st) {
  const runs = st.runs;
  if (!runs) return '<div class="empty">Loading runs…</div>';
  if (runs.error) return `<div class="empty">Could not load runs: ${esc(runs.error)}</div>`;
  if (!runs.length) {
    return `<div class="empty"><b>No recorded runs yet.</b><br><code>make verify</code> records every scenario it runs under <code>runs/</code>.</div>`;
  }
  const passed = runs.filter((r) => r.passed).length;
  const rows = runs
    .map(
      (r) => `<tr data-act="open-run" data-id="${esc(r.id)}" title="Open ${esc(r.scenario)} recording and its invariant report">
        <td>${verdict(r.passed)}</td>
        <td><a class="mono" href="#/runs/${encodeURIComponent(r.id)}" title="Replay recording ${esc(r.id)} and inspect its report">${esc(r.id)}</a></td>
        <td>${esc(r.scenario)}</td>
        <td class="num">${r.seed}</td>
        <td class="num">${r.speed}×</td>
        <td class="num">${r.p99_recover_ms ? secs(r.p99_recover_ms) : '–'}</td>
        <td class="num${r.violations ? ' bad' : ''}">${r.violations}</td>
        <td class="num">${dur(r.duration_ms)}</td>
        <td class="dim">${esc(started(r.started_at))}</td>
      </tr>`,
    )
    .join('');
  return `<section class="panel">
    <header class="panel-head"><h2>Recorded runs</h2><span class="dim">${runs.length} recorded · ${passed} passed · ${runs.length - passed} failed</span>
      <span class="spacer"></span><span class="hint">Open a run to play it back on the fleet grid</span></header>
    <table class="runs-table">
      <thead><tr><th>Result</th><th>Run</th><th>Scenario</th><th class="num">Seed</th><th class="num">Speed</th><th class="num">p99 recovery</th><th class="num">Violations</th><th class="num">Sim time</th><th>Started</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>
  </section>`;
}

function started(iso) {
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso || '' : `${d.toISOString().slice(0, 16).replace('T', ' ')} UTC`;
}

// ---------------------------------------------------------------- run bar
// The scrubber is mounted once per run and only its value is set afterwards,
// so the element under the pointer survives a drag.
function runbar(st) {
  const el = $('#runbar');
  const run = st.route.page === 'run' ? st.run : null;
  if (!run) {
    mount(el, '');
    return;
  }
  mount(el, `<div class="runbar"><div class="run-info" id="run-info"></div><div class="scrub" id="run-scrub"></div></div>`);
  const r = run.report;
  const frames = run.frames || [];
  const frame = frames[run.cursor];
  const ready = frames.length > 0;
  mount(
    $('#run-info'),
    `<a class="btn ghost sm" href="#/runs" title="Return to the list of recorded runs">← Runs</a>
     <b class="mono">${esc(run.id)}</b>
     ${r ? `${verdict(r.passed)}<span class="dim">${esc(r.scenario)} · seed ${r.seed} · recorded at ${r.speed}×</span>` : `<span class="dim">${esc(run.error || 'Loading…')}</span>`}
     <span class="spacer"></span>
     <button class="btn icon" data-act="run-step" data-by="-1" ${ready ? '' : 'disabled'} title="Back one snapshot (←)" aria-label="Step back">‹</button>
     <button class="btn icon primary" data-act="run-play" ${ready ? '' : 'disabled'} title="Play or pause (space)" aria-label="${run.playing ? 'Pause' : 'Play'}">${run.playing ? '❚❚' : '▶'}</button>
     <button class="btn icon" data-act="run-step" data-by="1" ${ready ? '' : 'disabled'} title="Forward one snapshot (→)" aria-label="Step forward">›</button>
     <div class="seg" role="group" aria-label="Playback rate">${RATES.map((x) => `<button data-act="run-rate" data-rate="${x}" title="Replay the recording at ${x} times its simulated speed" class="${x === run.rate ? 'on' : ''}">${x}×</button>`).join('')}</div>
     <span class="run-time mono">${frame ? clock(frame.at) : '--:--:--'}</span>
     <span class="dim mono">${ready ? `${run.cursor + 1}/${frames.length}` : ''}</span>`,
  );
  if (!ready) {
    mount($('#run-scrub'), '');
    return;
  }
  const from = frames[0].at;
  const span = Math.max(1, frames.at(-1).at - from);
  const marks = (r?.markers || [])
    .filter((m) => m.at >= from && m.at <= from + span)
    .map((m) => {
      const bad = isViolation(m);
      return `<button class="mark${bad ? ' violation' : ''}" style="left:${(((m.at - from) / span) * 100).toFixed(2)}%" data-act="seek" data-at="${m.at}"
        title="${clock(m.at)} · ${bad ? 'violation' : 'chaos'} · ${esc(shorten(m.detail))}" aria-label="${bad ? 'Violation' : 'Chaos action'} at ${clock(m.at)}"></button>`;
    })
    .join('');
  mount(
    $('#run-scrub'),
    `<div class="marks">${marks}</div>
     <input type="range" id="scrubber" data-live data-input="scrub" min="0" max="${frames.length - 1}" step="1" title="Drag to inspect a snapshot in the recording. Arrow keys step; Home and End jump to the start and finish." aria-label="Position in the recording">
     <div class="scrub-ends"><span>${clock(from)}</span>
       <span class="scrub-key"><i class="mark-key"></i>chaos action<i class="mark-key violation"></i>invariant violation</span>
       <span>${clock(from + span)}</span></div>`,
  );
  const input = $('#scrubber');
  if (Number(input.value) !== run.cursor) input.value = run.cursor;
}

// ------------------------------------------------------------------- report
function report(st) {
  const el = $('#report');
  const run = st.route.page === 'run' ? st.run : null;
  el.hidden = !run;
  if (!run) {
    mount(el, '');
    return;
  }
  const r = run.report;
  if (!r) {
    mount(el, `<header class="panel-head"><h2>Report</h2></header><div class="empty">${esc(run.error || 'Loading…')}</div>`);
    return;
  }
  const checks = (r.checks || [])
    .map((c) => {
      const bad = c.first_bad_seq ? (r.markers || []).find((m) => m.seq === c.first_bad_seq) : null;
      return `<div class="check ${c.passed ? 'pass' : 'fail'}">
        <i>${c.passed ? '✓' : '✕'}</i>
        <div><b>${esc(title(c.name))}</b>${c.detail ? `<span>${esc(shorten(c.detail))}</span>` : ''}</div>
        ${bad ? `<button class="btn sm danger" data-act="seek" data-at="${bad.at}" title="Jump to the first recorded violation of this check">Go to ${clock(bad.at)}</button>` : ''}
      </div>`;
    })
    .join('');
  const metrics = Object.entries(r.metrics || {})
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([k, n]) => `<div><span class="k">${esc(k.replace(/_ms$/, '').replace(/_/g, ' '))}</span><span class="v">${k.endsWith('_ms') ? secs(n) : n}</span></div>`)
    .join('');
  const cursorAt = run.frames?.[run.cursor]?.at ?? 0;
  const markers = (r.markers || [])
    .map((m) => {
      const bad = isViolation(m);
      return `<button class="marker-row${bad ? ' violation' : ''}${m.at <= cursorAt ? ' past' : ''}" data-act="seek" data-at="${m.at}" title="Jump to ${clock(m.at)}: ${esc(m.detail)}">
        <span class="t">${clock(m.at)}</span>${badge(bad ? 'violation' : m.kind, bad ? 'var(--bad)' : EVENT_KINDS[m.kind] || 'var(--dim)')}
        <span class="d">${esc(shorten(m.detail))}${m.host_id ? ` · ${esc(m.host_id)}` : ''}</span></button>`;
    })
    .join('');
  mount(
    el,
    `<header class="panel-head"><h2>Report</h2>${verdict(r.passed)}<span class="spacer"></span>
       <a class="btn ghost sm" href="/v1/runs/${encodeURIComponent(run.id)}" target="_blank" rel="noopener" title="Open the full machine-readable run report in a new tab">report.json</a></header>
     <div class="kv">
       <div><span class="k">Scenario</span><span class="v">${esc(r.scenario)}</span></div>
       <div><span class="k">Seed</span><span class="v">${r.seed}</span></div>
       <div><span class="k">p99 recovery</span><span class="v">${r.p99_recover_ms ? secs(r.p99_recover_ms) : '–'}</span></div>
       <div><span class="k">Violations</span><span class="v${r.violations ? ' bad' : ''}">${r.violations}</span></div>
       <div><span class="k">Sim time</span><span class="v">${dur(r.duration_ms)}</span></div>
       <div><span class="k">Started</span><span class="v">${esc(started(r.started_at))}</span></div>
     </div>
     <div class="sub-head">Checks</div><div class="checks">${checks || '<div class="empty">No checks recorded.</div>'}</div>
     ${metrics ? `<div class="sub-head">Metrics</div><div class="kv">${metrics}</div>` : ''}
     <div class="sub-head">Chaos actions and violations · ${(r.markers || []).length}</div>
     <div class="marker-list">${markers || '<div class="empty">None recorded.</div>'}</div>`,
  );
}

export function render(st) {
  mount($('#runs'), st.route.page === 'runs' ? list(st) : '');
  runbar(st);
  report(st);
}

export const actions = {
  'open-run': (el) => (location.hash = `#/runs/${encodeURIComponent(el.dataset.id)}`),
  'run-play': () => update(() => (state.run?.playing ? stop() : play())),
  'run-rate': (el) => update(() => state.run && (state.run.rate = Number(el.dataset.rate))),
  'run-step'(el) {
    stop();
    seek((state.run?.cursor || 0) + Number(el.dataset.by));
  },
  seek(el) {
    if (!state.run?.frames?.length) return;
    stop();
    seek(frameAt(state.run, Number(el.dataset.at)));
  },
};

export const inputs = {
  scrub(el) {
    stop();
    seek(Number(el.value));
  },
};

export function key(e) {
  if (state.route.page !== 'run' || !state.run?.frames?.length) return;
  if (e.target.closest?.('input, select, textarea, button, a')) return;
  if (e.key === ' ') {
    e.preventDefault();
    actions['run-play']();
  } else if (e.key === 'ArrowLeft' || e.key === 'ArrowRight') {
    e.preventDefault();
    stop();
    seek(state.run.cursor + (e.key === 'ArrowLeft' ? -1 : 1) * (e.shiftKey ? 10 : 1));
  }
}
