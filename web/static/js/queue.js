// The pending queue, in the order the scheduler will dequeue it.
import { $, esc, mount, prio, dur, badge, vmName } from './util.js';
import { api, attempt } from './api.js';

const REASON = { new: 'var(--dim)', retry: 'var(--warn)', replace: 'var(--bad)' };

function row(q, i, now, live) {
  const eff = prio(q.effective_priority);
  const backoff = q.not_before > now;
  // Waiting promotes an entry: its effective class can be above its own.
  const aged = q.effective_priority < q.priority ? `<span class="aged">aged from ${prio(q.priority).label}</span>` : '';
  const move = live
    ? `<span class="q-move">
        <button class="btn sm" data-act="q-prio" data-vm="${esc(q.vm_id)}" data-to="${q.priority - 1}" ${q.priority <= 1 ? 'disabled' : ''} title="Raise priority" aria-label="Raise priority">▲</button>
        <button class="btn sm" data-act="q-prio" data-vm="${esc(q.vm_id)}" data-to="${q.priority + 1}" ${q.priority === 0 || q.priority >= 3 ? 'disabled' : ''} title="Lower priority" aria-label="Lower priority">▼</button>
      </span>`
    : '<span></span>';
  return `<div class="q-row">
    <span class="q-pos">${i + 1}</span>
    <span class="q-main">${badge(eff.label, eff.color)}<button class="ref" data-act="ref" data-kind="vm" data-id="${esc(q.vm_id)}" title="${esc(q.vm_id)}">${esc(vmName(q.vm_id))}</button>${
      q.reason !== 'new' ? badge(q.reason, REASON[q.reason] || 'var(--dim)') : ''
    }${aged}</span>
    <span class="q-wait">${backoff ? `<span class="backoff">retry in ${dur(q.not_before - now)}</span>` : `waited ${dur(now - q.enqueued_at)}`}</span>
    ${move}
  </div>`;
}

export function render(st, v) {
  const el = $('#queue');
  const s = v.snapshot;
  if (!s) {
    mount(el, '');
    return;
  }
  const queue = s.queue || [];
  const cap = s.counters.queue_cap || queue.length;
  const cells = Array.from({ length: Math.max(cap, queue.length) }, (_, i) => {
    const q = queue[i];
    if (!q) return '<i class="q-cell"></i>';
    return `<i class="q-cell used${q.not_before > s.at ? ' wait' : ''}" style="--c:${prio(q.effective_priority).color}" title="${i + 1}. ${esc(vmName(q.vm_id))} · ${prio(q.effective_priority).label}"></i>`;
  }).join('');
  const full = queue.length >= cap;
  mount(
    el,
    `<header class="panel-head">
      <h2>Queue</h2>
      <span class="mono${full ? ' bad' : ''}">${queue.length}/${cap}</span>
      ${full ? badge('full · new creates get 429', 'var(--bad)') : '<span class="dim">in dequeue order</span>'}
    </header>
    <div class="q-strip">${cells}</div>
    <div class="q-list">${queue.map((q, i) => row(q, i, s.at, !v.playback)).join('') || '<div class="empty">Nothing is waiting for a slot.</div>'}</div>`,
  );
}

const KEYS = ['', 'interactive', 'standard', 'batch'];

export const actions = {
  'q-prio'(el) {
    const to = KEYS[Number(el.dataset.to)];
    if (to) attempt(`${vmName(el.dataset.vm)} → ${to}`, () => api.setPriority(el.dataset.vm, to));
  },
};
