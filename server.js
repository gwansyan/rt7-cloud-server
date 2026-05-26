const express = require('express');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(express.json({ limit: '2mb' }));
app.use(express.urlencoded({ extended: true }));

const DATA_DIR = process.env.RT7_DATA_DIR || path.join(__dirname, 'data');
const EVENT_LOG = path.join(DATA_DIR, 'rt7_event_log.jsonl');
const DEVICES_FILE = path.join(DATA_DIR, 'rt7_devices.json');

function ensureDataDir() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  if (!fs.existsSync(DEVICES_FILE)) fs.writeFileSync(DEVICES_FILE, '[]', 'utf8');
}

function nowIso() { return new Date().toISOString(); }

function appendEvent(event) {
  ensureDataDir();
  const row = Object.assign({ time: nowIso(), source: 'rt7_cloud_server_v2' }, event || {});
  fs.appendFileSync(EVENT_LOG, JSON.stringify(row) + '\n', 'utf8');
  return row;
}

function readDevices() {
  ensureDataDir();
  try {
    const raw = fs.readFileSync(DEVICES_FILE, 'utf8');
    const data = JSON.parse(raw || '[]');
    return Array.isArray(data) ? data : (data.devices || []);
  } catch (e) {
    return [];
  }
}

function saveDevices(devices) {
  ensureDataDir();
  fs.writeFileSync(DEVICES_FILE, JSON.stringify(devices, null, 2), 'utf8');
}

let doorbellState = {
  ok: true,
  count: 0,
  last: null
};

app.get('/', (req, res) => {
  res.type('html').send(`<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>RT7 Cloud Server V2</title><style>body{font-family:system-ui,-apple-system,"Noto Sans TC",Arial;margin:0;background:#f6f8fb;color:#17262a}.top{background:#0b252b;color:white;padding:22px;text-align:center}.wrap{max-width:880px;margin:0 auto;padding:16px}.card{background:white;border:1px solid #d8e0e8;border-radius:14px;padding:16px;margin:12px 0}code{background:#eef4f8;padding:2px 6px;border-radius:6px}.ok{color:#16a34a;font-weight:900}</style></head><body><div class="top"><h1>RT7 CLOUD SERVER V2</h1><div>Doorbell API + Node-RED Map</div></div><main class="wrap"><section class="card"><h2 class="ok">OK</h2><p>Railway Node.js Server is running.</p></section><section class="card"><h3>API</h3><p><code>POST /api/rt7/phase9n/doorbell/event</code></p><p><code>POST /api/doorbell</code></p><p><code>GET /api/rt7/doorbell/state</code></p><p><code>GET /api/events/latest</code></p><p><code>GET /api/devices</code></p></section></main></body></html>`);
});

// Keep old Node-RED endpoint for ESP32 compatibility.
app.post('/api/rt7/phase9n/doorbell/event', (req, res) => handleDoorbell(req, res, 'legacy_phase9n'));

// New cloud endpoint.
app.post('/api/doorbell', (req, res) => handleDoorbell(req, res, 'cloud_v2'));

function handleDoorbell(req, res, endpoint) {
  const body = req.body || {};
  doorbellState.count += 1;
  const event = appendEvent({
    type: 'doorbell',
    endpoint,
    device_id: body.device_id || body.device || body.id || '#1',
    device_name: body.device_name || body.name || '',
    ip: body.ip || body.device_ip || req.headers['x-forwarded-for'] || req.socket.remoteAddress || '',
    source: body.source || 'esp32_button',
    count: Number(body.count || doorbellState.count),
    message: body.message || '有人按門鈴'
  });

  doorbellState.last = event;

  console.log('[RT7_CLOUD][DOORBELL]', event);

  res.json({
    ok: true,
    message: 'doorbell received',
    state: doorbellState
  });
}

app.get('/api/rt7/doorbell/state', (req, res) => {
  res.json({ ok: true, state: doorbellState });
});

app.get('/api/doorbell/state', (req, res) => {
  res.json({ ok: true, state: doorbellState });
});

app.get('/api/events/latest', (req, res) => {
  ensureDataDir();
  const limit = Math.max(1, Math.min(500, Number(req.query.limit || 50)));
  let rows = [];
  try {
    rows = fs.readFileSync(EVENT_LOG, 'utf8')
      .split(/\r?\n/)
      .filter(Boolean)
      .slice(-limit)
      .map(line => {
        try { return JSON.parse(line); }
        catch (e) { return { type: 'parse_error', message: line }; }
      });
  } catch (e) {}
  res.json({ ok: true, events: rows });
});

app.get('/api/devices', (req, res) => {
  res.json({ ok: true, devices: readDevices() });
});

app.post('/api/device/register', (req, res) => {
  const body = req.body || {};
  const devices = readDevices();
  const id = body.device_id || body.id || body.device || '#1';
  const idx = devices.findIndex(d => d.id === id || d.device_id === id);
  const dev = {
    id,
    name: body.name || body.device_name || id,
    ip: body.ip || body.device_ip || '',
    version: body.version || '',
    last_online: nowIso()
  };
  if (idx >= 0) devices[idx] = Object.assign({}, devices[idx], dev);
  else devices.push(dev);
  saveDevices(devices);
  appendEvent({ type: 'device_register', device_id: id, device_name: dev.name, ip: dev.ip, message: 'device registered' });
  res.json({ ok: true, device: dev, devices });
});

const port = process.env.PORT || 3000;
app.listen(port, () => {
  ensureDataDir();
  console.log('[RT7_CLOUD] server start port=' + port);
});
