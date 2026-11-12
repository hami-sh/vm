// The evidence panel: what turns "it seems to work" into something checked.
// Invariant lights, the attack counter, time to Ready against its target,
// and how much room is left to absorb a rack failure.
import { $, esc, mount, clock, dur, secs, badge, INVARIANTS, PRIORITIES, title } from './util.js';
import { update } from './store.js';

const CHECKS = {
  one_authoritative_copy: 'At most one copy of a VM is authoritative: current generation and Ready in the scheduler.',
  stale_copy_stopped: 'A copy with an old generation is stopped within one heartbeat of its host reconnecting.',
  slot_capacity: 'No host runs more VMs than it has slots, counting slots still releasing.',
  generation_monotonic: "A VM's generation never decreases, including across scheduler restarts.",
  legal_transitions: 'Every transition in the event log is in the legal-move table.',
  queue_bounded: 'The queue never exceeds its cap, and a replacement is never shed.',
  no_successful_attack: 'Zero successful attacks from a malicious guest.',
  pending_placed_promptly: 'A Pending VM is placed within 2 s of capacity being free.',
};

// Short names for the lights; the full name and the rule show on click.
const SHORT = {
  one_authoritative_copy: 'One live copy',
  stale_copy_stopped: 'Stale copies',
  slot_capacity: 'Slot capacity',
  generation_monotonic: 'Generations rise',
  legal_transitions: 'Legal transitions',
  queue_bounded: 'Queue bounded',
  no_successful_attack: 'No attacks',
  pending_placed_promptly: 'Prompt starts',
};

function invariants(s, st) {
  const list = s.invariants || [];
  const total = list.reduce((n, x) => n + x.violations, 0);
  const lights = list
    .map((x) => {
      const cls = ['light'];
      if (x.violations) cls.push('bad');
      if (st.invariant === x.name) cls.push('on');
      return `<button class="${cls.join(' ')}" data-act="invariant" data-name="${esc(x.name)}" title="${esc(INVARIANTS[x.name] || title(x.name))}: ${esc(CHECKS[x.name] || '')}">
        <i></i><span class="name">${esc(SHORT[x.name] || INVARIANTS[x.name] || title(x.name))}</span>${x.violations ? `<span class="count">${x.violations}</span>` : ''}</button>`;
    })
    .join('');
  const open = list.find((x) => x.name === st.invariant);
  const detail = open
    ? `<div class="violation"><b>${esc(INVARIANTS[open.name] || title(open.name))}</b>
        <div class="dim">${esc(CHECKS[open.name] || '')}</div>
        ${
          open.violations
            ? `<div class="bad" style="margin-top:6px">${open.violations} violation${open.violations === 1 ? '' : 's'} · last at ${clock(open.last_at)}</div><div class="mono">${esc(open.last_detail || 'no detail recorded')}</div>`
            : '<div class="ok" style="margin-top:6px">Never violated since the last reset.</div>'
        }</div>`
    : '';
  return {
    total,
    html: `<div class="ev-section">
      <div class="ev-title"><h2>Invariants</h2><span class="dim">checked every 100 ms</span><span class="spacer"></span>
        <span class="mono ${total ? 'bad' : 'dim'}">${total} violation${total === 1 ? '' : 's'}</span></div>
      <div class="lights">${lights}</div>${detail}</div>`,
  };
}

function security(s) {
  const sec = s.security || { blocked: 0, succeeded: 0, by_attack: {} };
  const kinds = Object.entries(sec.by_attack || {})
    .sort((a, b) => b[1] - a[1])
    .map(([k, n]) => `<span>${esc(k.replace(/_/g, ' '))} <b>${n}</b></span>`)
    .join('');
  return `<div class="ev-section">
    <div class="ev-title"><h2>Security</h2><span class="dim">malicious guests</span></div>
    <div class="attacks">
      <span class="blocked">attacks blocked <span class="big">${sec.blocked}</span></span><span class="sep">·</span>
      <span class="succeeded${sec.succeeded ? ' bad' : ''}">succeeded <span class="big">${sec.succeeded}</span></span>
    </div>
    ${kinds ? `<div class="attack-kinds">${kinds}</div>` : '<div class="dim" style="margin-top:6px">No attack attempted yet. Compromise a VM from its menu.</div>'}
  </div>`;
}

// ------------------------------------------------------------------- chart
const W = 348;
const H = 176;
const X0 = 34;
const X1 = W - 4;
const Y0 = 8; // latency plot
const Y1 = 112;
const Q0 = 124; // queue strip
const Q1 = 156;

// ceil rounds a latency axis up to a readable number of seconds.
function ceil(ms) {
  for (const step of [2, 5, 10, 15, 20, 30, 45, 60, 90, 120, 180, 240, 300, 600]) if (ms <= step * 1000) return step * 1000;
  return Math.ceil(ms / 600_000) * 600_000;
}

// line builds an SVG path, lifting the pen where a sample has no data.
function line(points) {
  let d = '';
  let pen = false;
  for (const p of points) {
    if (!p) {
      pen = false;
      continue;
    }
    d += `${pen ? 'L' : 'M'}${p[0].toFixed(1)} ${p[1].toFixed(1)}`;
    pen = true;
  }
  return d;
}

function chart(s, st, v) {
  const cls = st.chartClass;
  const now = s.latency?.[cls] || { p50_ms: 0, p99_ms: 0, target_ms: 0, samples: 0 };
  const [from, to] = v.span;
  const history = v.history.filter((p) => p.at >= from && p.at <= to);
  const x = (at) => X0 + ((at - from) / Math.max(1, to - from)) * (X1 - X0);
  const target = now.target_ms || 0;
  const peak = history.reduce((m, p) => Math.max(m, p.latency[cls]?.p99_ms || 0), 0);
  const top = ceil(Math.max(peak, target * 1.25, 2000));
  const y = (ms) => Y1 - (Math.min(ms, top) / top) * (Y1 - Y0);
  const series = (key) => line(history.map((p) => (p.latency[cls]?.samples ? [x(p.at), y(p.latency[cls][key])] : null)));

  // Chaos intensity as amber bands behind the plot, one per run of equal value.
  let shade = '';
  for (let i = 0; i < history.length; ) {
    let j = i;
    while (j + 1 < history.length && history[j + 1].intensity === history[i].intensity) j++;
    const end = j + 1 < history.length ? history[j + 1].at : history[j].at;
    if (history[i].intensity > 0 && end > history[i].at) {
      shade += `<rect class="shade" x="${x(history[i].at).toFixed(1)}" y="${Y0}" width="${(x(end) - x(history[i].at)).toFixed(1)}" height="${Q1 - Y0}" fill-opacity="${(0.06 + history[i].intensity * 0.3).toFixed(2)}"/>`;
    }
    i = j + 1;
  }

  const cap = s.counters.queue_cap || 32;
  const qy = (n) => Q1 - (Math.min(n, cap) / cap) * (Q1 - Q0);
  const queue = history.length
    ? `<path class="queue" d="M${x(history[0].at).toFixed(1)} ${Q1}${history.map((p) => `L${x(p.at).toFixed(1)} ${qy(p.queue).toFixed(1)}`).join('')}L${x(history.at(-1).at).toFixed(1)} ${Q1}Z"/>`
    : '';

  const grid = [0, 0.5, 1]
    .map((f) => `<line class="grid" x1="${X0}" x2="${X1}" y1="${y(top * f)}" y2="${y(top * f)}"/><text x="${X0 - 5}" y="${y(top * f) + 3}" text-anchor="end">${(top * f) / 1000}s</text>`)
    .join('');
  const targetLine = target
    ? `<line class="target" x1="${X0}" x2="${X1}" y1="${y(target)}" y2="${y(target)}"/><text class="target-label" x="${X1 - 2}" y="${y(target) - 3}" text-anchor="end">target ${target / 1000}s</text>`
    : '';
  const cursor = v.cursorAt != null ? `<line class="cursor" x1="${x(v.cursorAt)}" x2="${x(v.cursorAt)}" y1="${Y0}" y2="${Q1}"/>` : '';
  const span = v.playback ? [clock(from), clock(to)] : [`−${dur(to - from)}`, 'now'];
  const over = target && now.p99_ms > target;

  return `<div class="ev-section">
    <div class="ev-title"><h2>Time to Ready</h2><span class="spacer"></span>
      <div class="seg">${PRIORITIES.slice(1)
        .map((p) => `<button data-act="chart-class" data-class="${p.key}" title="Show time-to-Ready percentiles and queue depth for ${p.label} requests" class="${p.key === cls ? 'on' : ''}">${p.label}</button>`)
        .join('')}</div></div>
    <svg class="chart" viewBox="0 0 ${W} ${H}" role="img" aria-label="p50 and p99 time to Ready, ${esc(cls)} class">
      ${shade}${grid}
      <line class="grid" x1="${X0}" x2="${X1}" y1="${Q1}" y2="${Q1}"/>
      <text x="${X0 - 5}" y="${Q0 + 4}" text-anchor="end">${cap}</text><text x="${X0 - 5}" y="${Q1 + 1}" text-anchor="end">queue</text>
      ${queue}${targetLine}
      <path class="p50" d="${series('p50_ms')}"/><path class="p99" d="${series('p99_ms')}"/>
      ${cursor}
      <text x="${X0}" y="${H - 6}">${span[0]}</text><text x="${X1}" y="${H - 6}" text-anchor="end">${span[1]}</text>
    </svg>
    <div class="chart-legend">
      <span><i style="--c:var(--booting)"></i>p50 <b>${now.samples ? secs(now.p50_ms) : '–'}</b></span>
      <span><i style="--c:#e8edf4"></i>p99 <b class="${over ? 'over' : ''}">${now.samples ? secs(now.p99_ms) : '–'}</b></span>
      <span><i class="box" style="--c:var(--terminating)"></i>queue history</span>
      <span><i class="box" style="--c:var(--warn)"></i>chaos intensity</span>
      <span class="faint">${now.samples} sample${now.samples === 1 ? '' : 's'}</span>
    </div>
  </div>`;
}

// ---------------------------------------------------------------- headroom
function headroom(s) {
  const c = s.counters;
  const n = c.headroom ?? 0;
  const cls = n < 0 ? ' neg' : n === 0 ? ' tight' : '';
  const text =
    n < 0
      ? `Losing the busiest rack would leave ${-n} replacement${n === -1 ? '' : 's'} without a slot. They would hold the front of the queue.`
      : n === 0
        ? 'Losing the busiest rack would use every free slot.'
        : 'Free slots left after re-placing every VM from the busiest rack.';
  const rec = c.recoveries || {};
  return `<div class="ev-section">
    <div class="ev-title"><h2>Rack-loss headroom</h2></div>
    <div class="headroom${cls}"><span class="big">${n > 0 ? '+' : ''}${n}</span><p>${text}</p></div>
    <div class="recov"><span>recoveries</span>${['recycle', 'replace', 'retry'].map((k) => `<span>${k} <b>${rec[k] || 0}</b></span>`).join('')}</div>
  </div>`;
}

export function render(st, v) {
  const el = $('#evidence');
  const s = v.snapshot;
  if (!s) {
    mount(el, '');
    return;
  }
  const inv = invariants(s, st);
  const attacks = s.security?.succeeded || 0;
  const bad = inv.total + attacks;
  mount(
    el,
    `<header class="panel-head"><h2>Evidence</h2><span class="verdict">${
      bad ? badge(`${bad} failure${bad === 1 ? '' : 's'}`, 'var(--bad)', 'solid') : badge('all checks hold', 'var(--ok)')
    }</span></header>
    ${inv.html}${security(s)}${chart(s, st, v)}${headroom(s)}`,
  );
}

export const actions = {
  invariant: (el) => update((st) => (st.invariant = st.invariant === el.dataset.name ? null : el.dataset.name)),
  'chart-class': (el) => update((st) => (st.chartClass = el.dataset.class)),
};
