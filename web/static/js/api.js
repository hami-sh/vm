// The dashboard is one more client of the public API: everything here is a
// plain fetch you could make with curl.
import { toast } from './store.js';

async function request(method, path, body, headers = {}) {
  const res = await fetch(path, {
    method,
    headers: body === undefined ? headers : { 'Content-Type': 'application/json', ...headers },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = text;
  }
  if (!res.ok) {
    const err = new Error(data?.error || res.statusText || `HTTP ${res.status}`);
    err.status = res.status;
    err.retryAfter = Number(res.headers.get('Retry-After')) || data?.retry_after_s || 0;
    throw err;
  }
  return data;
}

const get = (path) => request('GET', path);
const post = (path, body = {}, headers) => request('POST', path, body, headers);

export const api = {
  snapshot: () => get('/v1/snapshot'),
  source: () => get('/v1/source'),
  catalog: () => get('/v1/chaos/actions'),
  chaos: (action, target, params) => post(`/v1/chaos/${action}`, { target: target || undefined, params }),
  sim: (action, body) => post(`/v1/sim/${action}`, body),
  topology: (action, body) => post(`/v1/topology/${action}`, body),
  createVM: (pool, priority) => post('/v1/vms', { pool, priority }, { 'Idempotency-Key': crypto.randomUUID() }),
  setPriority: (vm, priority) => post(`/v1/queue/${encodeURIComponent(vm)}/priority`, { priority }),
  runs: () => get('/v1/runs'),
  run: (id) => get(`/v1/runs/${encodeURIComponent(id)}`),
  // Recorded snapshots are JSON lines; ?max thins a long run server-side.
  async frames(id, max = 600) {
    const res = await fetch(`/v1/runs/${encodeURIComponent(id)}/snapshots?max=${max}`);
    if (!res.ok) throw new Error(`snapshots: HTTP ${res.status}`);
    const text = await res.text();
    return text.split('\n').filter(Boolean).map((line) => JSON.parse(line));
  },
};

// attempt runs an API call and reports its outcome as a toast. The server's
// own message is shown as is: 429 and 503 say when to retry.
export async function attempt(label, call) {
  try {
    const out = await call();
    toast('ok', label, out?.detail && out.detail !== 'applied' ? out.detail : '');
    return out;
  } catch (err) {
    const retry = err.retryAfter ? ` Retry in ${err.retryAfter} s.` : '';
    const head = err.status === 429 ? 'Rate limited' : err.status === 503 ? 'Unavailable' : `${label} failed`;
    toast('error', head, `${err.message}.${retry}`.replace('..', '.'));
    return null;
  }
}

// connect opens the SSE stream. EventSource reconnects by itself and sends
// Last-Event-ID, so the server replays only the events missed.
export function connect({ onSnapshot, onEvent, onState }) {
  const es = new EventSource('/v1/events');
  es.addEventListener('open', () => onState('live'));
  es.addEventListener('error', () => onState('reconnecting'));
  es.addEventListener('snapshot', (m) => onSnapshot(JSON.parse(m.data)));
  es.addEventListener('event', (m) => onEvent(JSON.parse(m.data)));
  return es;
}
