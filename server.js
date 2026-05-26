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
  const row = Object.assign({ time: nowIso(), source: 'rt7_cloud_server_v3' }, event || {});
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

function htmlShell(title, body, extraHead = '') {
  return `<!doctype html>
<html lang="zh-Hant">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<title>${title}</title>
${extraHead}
</head>
<body>${body}</body>
</html>`;
}

app.get('/', (req, res) => {
  res.type('html').send(htmlShell('RT7 Cloud Server V3', `
<style>
body{font-family:system-ui,-apple-system,"Noto Sans TC","Microsoft JhengHei",Arial;margin:0;background:#f6f8fb;color:#17262a}
.top{background:#0b252b;color:white;padding:22px;text-align:center}
.wrap{max-width:880px;margin:0 auto;padding:16px}
.card{background:white;border:1px solid #d8e0e8;border-radius:14px;padding:16px;margin:12px 0}
code{background:#eef4f8;padding:2px 6px;border-radius:6px}
.ok{color:#16a34a;font-weight:900}
a.btn{display:inline-block;margin:6px 8px 6px 0;padding:12px 16px;border-radius:10px;background:#0b84d8;color:white;text-decoration:none;font-weight:900}
</style>
<div class="top"><h1>RT7 CLOUD SERVER V3</h1><div>Doorbell API + Phone Player</div></div>
<main class="wrap">
  <section class="card"><h2 class="ok">OK</h2><p>Railway Node.js Server is running.</p>
    <a class="btn" href="/rt7_cloud_doorbell_player">開啟門鈴播放器</a>
    <a class="btn" href="/api/rt7/doorbell/state">查看門鈴狀態</a>
  </section>
  <section class="card"><h3>API</h3>
    <p><code>POST /api/rt7/phase9n/doorbell/event</code></p>
    <p><code>POST /api/doorbell</code></p>
    <p><code>GET /api/rt7/doorbell/state</code></p>
    <p><code>GET /api/events/latest</code></p>
    <p><code>GET /api/devices</code></p>
    <p><code>GET /rt7_cloud_doorbell_player</code></p>
  </section>
</main>`));
});

// 手機門鈴播放器：輪詢 Railway state，偵測 count 增加後播放提示音並顯示文字。
app.get('/rt7_cloud_doorbell_player', (req, res) => {
  res.type('html').send(htmlShell('RT7 Cloud Doorbell Player', `
<style>
:root{--dark:#0b252b;--blue:#0b84d8;--green:#16a34a;--red:#dc2626;--line:#d8e0e8;--text:#17262a;--muted:#64748b}
*{box-sizing:border-box}
body{margin:0;background:#f4f7fa;color:var(--text);font-family:system-ui,-apple-system,"Noto Sans TC","Microsoft JhengHei",Arial,sans-serif}
.top{background:linear-gradient(90deg,#092228,#0d2c32);color:white;padding:18px 14px;text-align:center;font-weight:900}
.top h1{margin:0;font-size:22px;line-height:1.2}.top p{margin:6px 0 0;color:#d8eef3;font-size:13px}
.wrap{max-width:720px;margin:0 auto;padding:14px}
.card{background:white;border:1px solid var(--line);border-radius:16px;padding:16px;margin:12px 0;box-shadow:0 3px 18px rgba(10,30,40,.06)}
.statusLine{display:flex;align-items:center;gap:10px;font-size:20px;font-weight:900;color:#713018;min-height:54px;padding:12px;border-radius:12px;background:#fff7ed;border:1px solid #fed7aa}
.dot{width:18px;height:18px;border-radius:50%;background:#22c55e;box-shadow:0 0 0 4px rgba(34,197,94,.12)}
.bigBell{text-align:center;font-size:86px;line-height:1.1;margin:12px 0}
.count{font-size:18px;font-weight:900;color:#0b84d8}
.btns{display:grid;grid-template-columns:1fr 1fr;gap:10px}
button{border:0;border-radius:12px;padding:13px 14px;font-size:16px;font-weight:900;color:white;background:#0b84d8}
button.gray{background:#475569}button.red{background:#dc2626}button.green{background:#16a34a}
pre{white-space:pre-wrap;background:#0b1020;color:#d8f2ff;border-radius:12px;padding:12px;font-size:12px;max-height:240px;overflow:auto}
.small{font-size:13px;color:#64748b;line-height:1.5}
.warn{color:#b45309;font-weight:800}
@media(max-width:520px){.top h1{font-size:19px}.statusLine{font-size:18px}.bigBell{font-size:74px}}
</style>
<header class="top">
  <h1>RT7 PHASE10<br>Cloud Doorbell Player</h1>
  <p>手機輪詢 Railway /api/rt7/doorbell/state</p>
</header>
<main class="wrap">
  <section class="card">
    <div id="statusLine" class="statusLine"><span class="dot"></span><span id="statusText">等待門鈴事件...</span></div>
    <div class="bigBell">🔔</div>
    <div>目前 count：<span id="count" class="count">0</span></div>
    <div class="small">最後事件：<span id="lastTime">尚無</span></div>
  </section>
  <section class="card">
    <div class="btns">
      <button class="green" onclick="enableSound()">啟用提示音</button>
      <button onclick="testBeep()">測試提示音</button>
      <button class="gray" onclick="pollNow()">立即讀取</button>
      <button class="red" onclick="resetLocal()">本機重設顯示</button>
    </div>
    <p class="small warn">手機瀏覽器通常需要先按一次「啟用提示音」，之後門鈴事件才可自動播放提示音。</p>
  </section>
  <section class="card">
    <h3>狀態 JSON</h3>
    <pre id="jsonBox">ready</pre>
  </section>
</main>
<script>
let lastCount = 0;
let audioCtx = null;
let soundEnabled = false;
let firstLoad = true;

function $(id){return document.getElementById(id);}
function setStatus(text){ $('statusText').textContent = text; }
function setJson(obj){ $('jsonBox').textContent = JSON.stringify(obj, null, 2); }

function ensureAudio(){
  audioCtx = audioCtx || new (window.AudioContext || window.webkitAudioContext)();
  if(audioCtx.state === 'suspended') audioCtx.resume();
  return audioCtx;
}

function tone(freq, startMs, durMs, gainValue){
  const ctx = ensureAudio();
  const osc = ctx.createOscillator();
  const gain = ctx.createGain();
  osc.type = 'sine';
  osc.frequency.value = freq;
  gain.gain.setValueAtTime(0.0001, ctx.currentTime + startMs/1000);
  gain.gain.exponentialRampToValueAtTime(gainValue || 0.16, ctx.currentTime + (startMs+20)/1000);
  gain.gain.exponentialRampToValueAtTime(0.0001, ctx.currentTime + (startMs+durMs)/1000);
  osc.connect(gain);
  gain.connect(ctx.destination);
  osc.start(ctx.currentTime + startMs/1000);
  osc.stop(ctx.currentTime + (startMs+durMs+40)/1000);
}

function doorbellSound(){
  if(!soundEnabled) return;
  try{
    tone(880, 0, 180, 0.18);
    tone(660, 230, 240, 0.18);
  }catch(e){}
}

function enableSound(){
  try{
    ensureAudio();
    soundEnabled = true;
    testBeep();
    setStatus('提示音已啟用，等待門鈴...');
  }catch(e){
    setStatus('提示音啟用失敗：' + e.message);
  }
}

function testBeep(){
  soundEnabled = true;
  doorbellSound();
}

function resetLocal(){
  lastCount = 0;
  firstLoad = true;
  setStatus('已重設本機顯示，等待門鈴...');
}

async function pollNow(){
  try{
    const r = await fetch('/api/rt7/doorbell/state?_=' + Date.now(), {cache:'no-store'});
    const j = await r.json();
    setJson(j);
    const state = j && j.state ? j.state : {};
    const count = Number(state.count || 0);
    $('count').textContent = count;
    if(state.last && state.last.time) $('lastTime').textContent = new Date(state.last.time).toLocaleString('zh-TW', {hour12:false});

    if(firstLoad){
      lastCount = count;
      firstLoad = false;
      if(count > 0 && state.last){
        setStatus('目前已有門鈴紀錄 #' + count + '，等待下一次門鈴...');
      }
      return;
    }

    if(count > lastCount){
      lastCount = count;
      const msg = (state.last && (state.last.message || state.last.text)) || '有人按門鈴';
      setStatus('🔔 ' + msg + ' #' + count);
      doorbellSound();
      if(navigator.vibrate) navigator.vibrate([160,80,160]);
    }
  }catch(e){
    setStatus('讀取失敗：' + e.message);
  }
}

setInterval(pollNow, 1000);
pollNow();
</script>`));
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
    device_name: body.device_name || body.name || 'RT7 ESP32-S3-CAM',
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
