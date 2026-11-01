'use strict';

// PulseGrid live dashboard.
//
// - Reads /dashboard/stream (Server-Sent Events) with fetch, because the
//   browser's EventSource cannot send the X-API-Key header.
// - Each event is one worker's report for one shard and one alert. The shard
//   is drawn as the rectangle its geohash prefix covers.
// - The operator key lives only in this page's memory.
// - Everything from the server is shown with textContent, never as HTML.

const BASE32 = '0123456789bcdefghjkmnpqrstuvwxyz';
const MAX_ALERTS_SHOWN = 25;

const $ = (id) => document.getElementById(id);
const els = {
  key: $('key'),
  connect: $('connect'),
  status: $('status'),
  center: $('center'),
  radius: $('radius'),
  radiusLabel: $('radius-label'),
  severity: $('severity'),
  title: $('title'),
  message: $('message'),
  send: $('send'),
  result: $('result'),
  log: $('log'),
};

// ---------------------------------------------------------------- the map

const map = L.map('map').setView([24.8607, 67.0011], 11);
L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
  maxZoom: 19,
  attribution: '&copy; OpenStreetMap contributors',
}).addTo(map);

let center = L.latLng(24.8607, 67.0011);
const selection = L.circle(center, {
  radius: Number(els.radius.value),
  color: '#c33',
  weight: 2,
  fillOpacity: 0.08,
  interactive: false,
}).addTo(map);

function formatRadius(meters) {
  return meters >= 1000 ? (meters / 1000).toFixed(1) + ' km' : meters + ' m';
}

function redrawSelection() {
  const radius = Number(els.radius.value);
  selection.setLatLng(center);
  selection.setRadius(radius);
  els.center.textContent = center.lat.toFixed(5) + ', ' + center.lng.toFixed(5);
  els.radiusLabel.textContent = formatRadius(radius);
}

map.on('click', (event) => {
  center = event.latlng.wrap();
  redrawSelection();
});
els.radius.addEventListener('input', redrawSelection);
redrawSelection();

// ------------------------------------------------------------ shard cells

// The rectangle a geohash prefix covers, as [[south, west], [north, east]].
function geohashBounds(hash) {
  const lat = [-90, 90];
  const lon = [-180, 180];
  let evenBit = true; // geohash starts with a longitude bit
  for (const ch of hash) {
    const value = BASE32.indexOf(ch);
    if (value < 0) return null;
    for (let bit = 4; bit >= 0; bit--) {
      const range = evenBit ? lon : lat;
      const mid = (range[0] + range[1]) / 2;
      if ((value >> bit) & 1) range[0] = mid;
      else range[1] = mid;
      evenBit = !evenBit;
    }
  }
  return [[lat[0], lon[0]], [lat[1], lon[1]]];
}

const shardRects = new Map();

function flashShard(t) {
  let rect = shardRects.get(t.shard_prefix);
  if (!rect) {
    const bounds = geohashBounds(t.shard_prefix);
    if (!bounds) return;
    rect = L.rectangle(bounds, { color: '#3a6ea5', weight: 1, fillOpacity: 0.04 }).addTo(map);
    shardRects.set(t.shard_prefix, rect);
  }

  const tip = document.createElement('div');
  tip.textContent =
    t.shard_prefix + ': delivered ' + t.delivered + ', dropped ' + t.dropped +
    ', outside the circle ' + t.filtered_out + ' (' + t.broadcast_duration_us +
    ' \u00b5s, worker ' + String(t.worker_id).slice(0, 8) + ')';
  if (rect.getTooltip()) rect.setTooltipContent(tip);
  else rect.bindTooltip(tip, { sticky: true });

  const hit = t.delivered > 0;
  rect.setStyle({
    color: hit ? '#1f8a3c' : '#888',
    fillColor: hit ? '#2a9d4a' : '#999',
    fillOpacity: hit ? 0.45 : 0.12,
  });
  clearTimeout(rect._fade);
  rect._fade = setTimeout(() => rect.setStyle({ fillOpacity: 0.05 }), 1500);
}

function ripple(alert) {
  const ring = L.circle(alert.center, {
    radius: 1, color: '#d33', weight: 2, fill: false, interactive: false,
  }).addTo(map);
  const start = performance.now();
  const duration = 1200;
  (function step(now) {
    const p = Math.min(1, (now - start) / duration);
    ring.setRadius(Math.max(1, alert.radius * p));
    ring.setStyle({ opacity: 1 - p });
    if (p < 1) requestAnimationFrame(step);
    else ring.remove();
  })(start);
}

// ----------------------------------------------------------------- alerts

const alerts = new Map(); // alert_id -> record, oldest first

function alertRecord(id) {
  let record = alerts.get(id);
  if (!record) {
    record = {
      id, title: null, center: null, radius: 0, rippled: false,
      delivered: 0, dropped: 0, filteredOut: 0, shards: new Set(), maxUs: 0,
    };
    alerts.set(id, record);
    while (alerts.size > MAX_ALERTS_SHOWN) alerts.delete(alerts.keys().next().value);
  }
  return record;
}

function maybeRipple(record) {
  if (record.center && record.delivered > 0 && !record.rippled) {
    record.rippled = true;
    ripple(record);
  }
}

function onDelivery(t) {
  const record = alertRecord(t.alert_id);
  record.delivered += t.delivered;
  record.dropped += t.dropped;
  record.filteredOut += t.filtered_out;
  record.shards.add(t.shard_prefix);
  record.maxUs = Math.max(record.maxUs, t.broadcast_duration_us);
  flashShard(t);
  maybeRipple(record);
  renderLog();
}

function renderLog() {
  els.log.replaceChildren();
  for (const record of [...alerts.values()].reverse()) {
    const row = document.createElement('div');
    row.className = 'row';

    const name = document.createElement('div');
    name.className = 'name';
    name.textContent = record.title || 'alert ' + record.id.slice(0, 8);

    const numbers = document.createElement('div');
    numbers.className = 'numbers';
    numbers.textContent =
      'delivered ' + record.delivered + ', dropped ' + record.dropped +
      ', shards ' + record.shards.size + ', slowest fan-out ' + record.maxUs + ' \u00b5s';

    row.append(name, numbers);
    els.log.append(row);
  }
}

async function sendAlert() {
  const key = els.key.value.trim();
  if (!key) { els.result.textContent = 'Enter the operator key first.'; return; }

  const wrapped = center.wrap();
  const body = {
    lat: wrapped.lat,
    lon: wrapped.lng,
    radius_meters: Number(els.radius.value),
    severity: els.severity.value,
    title: els.title.value.trim() || 'Dashboard test',
    message: els.message.value.trim() || 'Sent from the dashboard',
  };

  els.send.disabled = true;
  els.result.textContent = 'Sending\u2026';
  try {
    const response = await fetch('/alerts', {
      method: 'POST',
      headers: { 'X-API-Key': key, 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok && response.status !== 207) {
      els.result.textContent = 'Refused (HTTP ' + response.status + '): ' + (data.error || 'no details');
      return;
    }
    const record = alertRecord(data.alert_id);
    record.title = body.title;
    record.center = L.latLng(body.lat, body.lon);
    record.radius = body.radius_meters;
    maybeRipple(record);
    renderLog();
    els.result.textContent = response.status === 207
      ? 'Sent, but some shards failed (HTTP 207).'
      : 'Sent.';
  } catch (error) {
    els.result.textContent = 'Could not send: ' + error.message;
  } finally {
    els.send.disabled = false;
  }
}
els.send.addEventListener('click', sendAlert);

// ----------------------------------------------------------------- stream

let streamAbort = null;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function handleBlock(block) {
  let event = 'message';
  const data = [];
  for (const line of block.split('\n')) {
    if (line.startsWith('event:')) event = line.slice(6).trim();
    else if (line.startsWith('data:')) data.push(line.slice(5).trimStart());
  }
  if (data.length === 0) return; // keep-alive comment
  if (event === 'delivery') {
    try { onDelivery(JSON.parse(data.join('\n'))); } catch (e) { /* ignore a bad event */ }
  } else if (event === 'lagged') {
    setStatus('connected, but this page fell behind and missed ' + data.join('') + ' events');
  }
}

async function readEvents(body, signal) {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  while (!signal.aborted) {
    const { value, done } = await reader.read();
    if (done) return;
    buffer += decoder.decode(value, { stream: true });
    let end;
    while ((end = buffer.indexOf('\n\n')) >= 0) {
      handleBlock(buffer.slice(0, end));
      buffer = buffer.slice(end + 2);
    }
  }
}

function setStatus(text) { els.status.textContent = text; }

async function connectStream() {
  const key = els.key.value.trim();
  if (!key) { setStatus('Enter the operator key first.'); return; }

  if (streamAbort) streamAbort.abort();
  streamAbort = new AbortController();
  const signal = streamAbort.signal;
  let attempt = 0;

  while (!signal.aborted) {
    try {
      const response = await fetch('/dashboard/stream', {
        headers: { 'X-API-Key': key },
        signal,
      });
      if (response.status === 401 || response.status === 403) {
        setStatus('The operator key was rejected.');
        return;
      }
      if (!response.ok || !response.body) throw new Error('HTTP ' + response.status);
      setStatus('connected, waiting for deliveries\u2026');
      attempt = 0;
      await readEvents(response.body, signal);
      if (!signal.aborted) setStatus('stream ended, reconnecting\u2026');
    } catch (error) {
      if (signal.aborted) return;
      setStatus('disconnected (' + error.message + '), retrying\u2026');
    }
    attempt += 1;
    await sleep(Math.min(1000 * 2 ** attempt, 15000));
  }
}
els.connect.addEventListener('click', connectStream);
