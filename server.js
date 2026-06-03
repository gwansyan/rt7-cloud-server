const express = require('express');
const fs = require('fs');
const path = require('path');
const http = require('http');
const WebSocket = require('ws');
const jpeg = require('jpeg-js');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: '/ws' });

app.use(express.json({ limit: '8mb' }));
app.use(express.urlencoded({ extended: true, limit: '8mb' }));

const DATA_DIR = process.env.RT7_DATA_DIR || path.join(__dirname, 'data');
const EVENT_LOG = path.join(DATA_DIR, 'rt7_event_log.jsonl');
const DEVICES_FILE = path.join(DATA_DIR, 'rt7_devices.json');

const SERVER_VERSION = 'RT7_CLOUD_SERVER_V5_4W_FACE_GATE_FAST8081_DISABLE_COMPILE_FIX';

function ensureDataDir() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  if (!fs.existsSync(DEVICES_FILE)) fs.writeFileSync(DEVICES_FILE, JSON.stringify(defaultDevices(), null, 2), 'utf8');
  if (!fs.existsSync(EVENT_LOG)) fs.writeFileSync(EVENT_LOG, '', 'utf8');
}

function defaultDevices() {
  return [
    { id: '#1', name: 'RT7 ESP32-S3-CAM', ip: '', enabled: true },
    { id: '#2', name: '#2', ip: '', enabled: true },
    { id: '#3', name: '#3', ip: '', enabled: true },
    { id: '#4', name: '#4', ip: '', enabled: true }
  ];
}

function nowIso() { return new Date().toISOString(); }
function safeString(v) { return (v === undefined || v === null) ? '' : String(v); }
function clientIp(req) {
  const fwd = safeString(req.headers['x-forwarded-for']).split(',')[0].trim();
  return fwd || req.socket.remoteAddress || '';
}

function readDevices() {
  ensureDataDir();
  try {
    const raw = fs.readFileSync(DEVICES_FILE, 'utf8');
    const data = JSON.parse(raw || '[]');
    return Array.isArray(data) ? data : (data.devices || []);
  } catch (e) {
    return defaultDevices();
  }
}

function saveDevices(devices) {
  ensureDataDir();
  const arr = Array.isArray(devices) ? devices : [];
  fs.writeFileSync(DEVICES_FILE, JSON.stringify(arr, null, 2), 'utf8');
  return arr;
}

function appendEvent(event) {
  ensureDataDir();
  const row = Object.assign({
    time: nowIso(),
    server: SERVER_VERSION
  }, event || {});
  fs.appendFileSync(EVENT_LOG, JSON.stringify(row) + '\n', 'utf8');
  return row;
}

function readEvents(limit = 500) {
  ensureDataDir();
  try {
    const lines = fs.readFileSync(EVENT_LOG, 'utf8').split(/\r?\n/).filter(Boolean);
    const tail = lines.slice(Math.max(0, lines.length - limit));
    return tail.map(line => {
      try { return JSON.parse(line); }
      catch (e) { return { type: 'parse_error', message: line }; }
    });
  } catch (e) {
    return [];
  }
}

function broadcast(type, payload) {
  const msg = JSON.stringify({ ok: true, type, payload, time: nowIso() });
  for (const ws of wss.clients) {
    if (ws.readyState === WebSocket.OPEN) ws.send(msg);
  }
}

function broadcastBinaryToViewers(buf) {
  for (const ws of wss.clients) {
    if (ws.readyState === WebSocket.OPEN && ws.rt7Role === 'viewer') {
      try { ws.send(buf, { binary: true }); } catch (_) {}
    }
  }
}


// V5.0I: WebSocket binary intercom relay. Phone sends PCM16 binary frames to Railway;
// Railway forwards them to ESP32 persistent esp32_pcm client over /ws.
function rt7IsPhonePcmRole_(role) {
  return role === 'phone_pcm' || role === 'intercom_phone' || role === 'phone' || role === 'webrtc_phone';
}
function rt7IsEspPcmRole_(role) {
  return role === 'esp32_pcm' || role === 'esp32_intercom' || role === 'esp32' || role === 'esp32_frame_upload' || role === 'esp32_pcm_client';
}
function rt7IsEspPcmClient_(c) {
  return !!c && (rt7IsEspPcmRole_(c.rt7Role) || rt7IsEspPcmRole_(c.rt7PcmRole) || c.rt7PcmClient === true);
}
function rt7SendToEspIntercom_(payload, opts) {
  let n = 0;
  for (const c of wss.clients) {
    if (c.readyState !== WebSocket.OPEN) continue;
    if (rt7IsEspPcmClient_(c)) {
      try { c.send(payload, opts || {}); n++; } catch (_) {}
    }
  }
  return n;
}

// V5.0N: Relay ESP32 mic PCM back only to phone/intercom clients.
// This is required for release-to-listen duplex mode.
function rt7SendToPhoneIntercom_(payload, opts) {
  let n = 0;
  for (const c of wss.clients) {
    if (c.readyState !== WebSocket.OPEN) continue;
    if (rt7IsPhonePcmRole_(c.rt7Role)) {
      try { c.send(payload, opts || {}); n++; } catch (_) {}
    }
  }
  return n;
}
const rt7WsTrace = {
  phonePcmPackets: 0,
  phonePcmBytes: 0,
  relayPcmPackets: 0,
  relayPcmBytes: 0,
  espPcmPackets: 0,
  espPcmBytes: 0,
  phoneRxPackets: 0,
  phoneRxBytes: 0,
  lastEspPcmTime: null,
  lastPhoneRxTime: null,
  lastPhonePcmTime: null,
  lastRelayTime: null
};
function rt7IntercomWsState_() {
  let phones=0, esp=0;
  for (const c of wss.clients) {
    if (c.readyState !== WebSocket.OPEN) continue;
    if (rt7IsPhonePcmRole_(c.rt7Role)) phones++;
    if (rt7IsEspPcmClient_(c)) esp++;
  }
  return {
    phones, esp,
    phone_pcm_packets: rt7WsTrace.phonePcmPackets,
    phone_pcm_bytes: rt7WsTrace.phonePcmBytes,
    relay_pcm_packets: rt7WsTrace.relayPcmPackets,
    relay_pcm_bytes: rt7WsTrace.relayPcmBytes,
    esp_pcm_packets: rt7WsTrace.espPcmPackets,
    esp_pcm_bytes: rt7WsTrace.espPcmBytes,
    phone_rx_packets: rt7WsTrace.phoneRxPackets,
    phone_rx_bytes: rt7WsTrace.phoneRxBytes,
    last_esp_pcm_time: rt7WsTrace.lastEspPcmTime,
    last_phone_rx_time: rt7WsTrace.lastPhoneRxTime,
    last_phone_pcm_time: rt7WsTrace.lastPhonePcmTime,
    last_relay_time: rt7WsTrace.lastRelayTime
  };
}

function normalizeDevice(body, req) {
  const ip = body.ip || body.device_ip || body.esp_ip || clientIp(req);
  return {
    id: body.device_id || body.device || body.id || '#1',
    name: body.device_name || body.name || 'RT7 ESP32-S3-CAM',
    ip,
    version: body.version || body.firmware || '',
    last_online: nowIso(),
    enabled: body.enabled !== false
  };
}

let doorbellState = {
  ok: true,
  count: 0,
  last: null
};

// ---------- V4.8F7 restored shared runtime state ----------
const SNAPSHOT_FILE = path.join(DATA_DIR, 'latest.jpg');
const STREAM_FRAME_FILE = path.join(DATA_DIR, 'latest_stream_frame.jpg');
let latestStreamFrame = null;
let rt7MjpegCongestUntilMs = 0;
// V5.0G: while phone PCM is active, skip Railway JPEG work to reduce audio jitter.
let rt7AudioActiveUntilMs = 0;
function rt7AudioHold_(ms) { rt7AudioActiveUntilMs = Math.max(rt7AudioActiveUntilMs, Date.now() + ms); }
function rt7AudioActive_() { return Date.now() < rt7AudioActiveUntilMs; }
const RT7_STREAM_FAST_MS = 100;
const RT7_STREAM_STABLE_MS = 140;
const RT7_STREAM_IDLE_MS = 1000;
const RT7_VIEWER_ACTIVE_TTL_MS = 12000;
const streamViewers = new Map();
let liveStreamState = {
  ok: true,
  enabled: true,
  transport: 'auto_lan_cloud',
  fps_mode: 'idle',
  adaptive_mode: 'idle_1fps',
  desired_interval_ms: RT7_STREAM_IDLE_MS,
  seq: 0,
  bytes: 0,
  time: null,
  viewer_count: 0,
  clients: 0
};
let cloudState = {
  current_device_id: '#1',
  ai_enabled: false,
  plugins: { motion:true, face:true, doorbell:true, intercom:true },
  last_snapshot: null,
  last_vision: null,
  face_gate_enabled: false,
  face_gate_auto_enabled: false,
  face_gate_auto_busy: false,
  face_gate_auto_last_ms: 0,
  face_gate_auto_cooldown_ms: 8000,
  last_face_gate: null
};
function getCurrentDevice(req) {
  const devices = readDevices();
  const qid = safeString(req?.query?.device_id || req?.query?.device || '').trim();
  const id = qid || cloudState.current_device_id || '#1';
  let dev = devices.find(d => d.id === id) || devices.find(d => d.id === '#1') || devices[0] || { id:'#1', name:'RT7 ESP32-S3-CAM', ip:'192.168.0.179' };
  if (!dev.ip) dev = Object.assign({}, dev, { ip:'192.168.0.179' });
  dev.base_url = /^https?:\/\//i.test(dev.ip) ? dev.ip.replace(/\/$/,'') : 'http://' + dev.ip;
  return dev;
}
async function proxyToEsp(req, res, espPath, method='GET') {
  const dev = getCurrentDevice(req);
  const url = dev.base_url + espPath + (req.url.includes('?') ? req.url.slice(req.url.indexOf('?')) : '');
  try {
    const opt = { method, headers: { 'Content-Type': req.headers['content-type'] || 'text/plain' } };
    if (method !== 'GET' && method !== 'HEAD') opt.body = typeof req.body === 'string' ? req.body : (Buffer.isBuffer(req.body) ? req.body : JSON.stringify(req.body || {}));
    const r = await fetch(url, opt);
    const buf = Buffer.from(await r.arrayBuffer());
    res.status(r.status);
    const ct = r.headers.get('content-type');
    if (ct) res.setHeader('content-type', ct);
    res.send(buf);
  } catch (e) {
    res.status(502).json({ ok:false, error:'ESP_PROXY_FAILED', url, message:String(e && e.message || e) });
  }
}


function registerOrUpdateDevice(dev) {
  const devices = readDevices();
  const idx = devices.findIndex(d => (d.id && d.id === dev.id) || (dev.ip && d.ip === dev.ip));
  if (idx >= 0) devices[idx] = Object.assign({}, devices[idx], dev);
  else devices.push(dev);
  saveDevices(devices);
  return dev;
}

function htmlShell(title, body, extraHead = '') {
  return `<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no"><title>${title}</title>${extraHead}</head><body>${body}
<script id="rt7-v48f4-button-layout-fix-js">
(function(){
  function bindTextButton(label, fn){
    Array.prototype.forEach.call(document.querySelectorAll('button'), function(b){
      if((b.textContent||'').trim() === label){
        b.style.pointerEvents='auto'; b.style.position='relative'; b.style.zIndex='2147483000';
        b.addEventListener('click', function(ev){ ev.preventDefault(); ev.stopPropagation(); try{ fn(); }catch(e){ console.log(e); } }, true);
      }
    });
  }
  function install(){
    var kill=function(el){ try{ el.style.pointerEvents='none'; el.style.zIndex='0'; }catch(e){} };
    Array.prototype.forEach.call(document.querySelectorAll('.audioOverlay,.modal,.modal-backdrop,.overlay,.backdrop,.mask,.loading,.blocker'), kill);
    bindTextButton('啟用 AI', function(){ if(window.enableAi) window.enableAi(); });
    bindTextButton('關閉 AI', function(){ if(window.disableAi) window.disableAi(); });
    bindTextButton('開始影像', function(){ if(window.startVideo) window.startVideo(); });
    bindTextButton('停止影像', function(){ if(window.stopVideo) window.stopVideo(); });
  }
  if(document.readyState==='loading') document.addEventListener('DOMContentLoaded', install); else install();
})();
</script>
</body></html>`;
}

const baseCss = `
<style>
:root{--dark:#0b252b;--blue:#0b84d8;--green:#16a34a;--red:#dc2626;--gray:#475569;--line:#d8e0e8;--bg:#f6f8fb;--text:#17262a}
*{box-sizing:border-box}body{font-family:system-ui,-apple-system,"Noto Sans TC","Microsoft JhengHei",Arial,sans-serif;margin:0;background:var(--bg);color:var(--text)}
.top{background:linear-gradient(90deg,#092228,#0d2c32);color:#fff;padding:20px;text-align:center;font-weight:900}.top h1{margin:0;font-size:22px}.top p{margin:6px 0 0;color:#cdebf0}
.wrap{max-width:980px;margin:0 auto;padding:14px}.card{background:#fff;border:1px solid var(--line);border-radius:14px;padding:16px;margin:12px 0;box-shadow:0 2px 14px rgba(10,30,40,.06)}
.btn{display:inline-flex;align-items:center;justify-content:center;min-height:46px;margin:6px 8px 6px 0;padding:12px 16px;border:0;border-radius:11px;background:var(--blue);color:white;text-decoration:none;font-weight:900;font-size:16px;cursor:pointer}.green{background:var(--green)}.red{background:var(--red)}.gray{background:var(--gray)}
code{background:#eef4f8;padding:2px 6px;border-radius:6px}.status{background:#08101f;color:#d8f2ff;border-radius:10px;padding:12px;white-space:pre-wrap;font-family:ui-monospace,Consolas,monospace;font-size:13px;max-height:260px;overflow:auto}.big{font-size:44px;font-weight:900}.muted{color:#64748b}.ok{color:#16a34a;font-weight:900}.warn{color:#b45309;font-weight:900}
table{width:100%;border-collapse:collapse;background:white}th,td{border-bottom:1px solid #e6edf3;padding:9px;text-align:left;vertical-align:top;font-size:14px}th{background:#edf6ff}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px}
@media(max-width:640px){.btn{width:100%;margin:6px 0}.top h1{font-size:19px}th,td{font-size:12px;padding:7px}.wrap{padding:10px}}
</style>`;

app.get('/', (req, res) => {
  res.type('html').send(htmlShell('RT7 Cloud Server V4.3', `${baseCss}
<header class="top"><h1>RT7 CLOUD SERVER V4.3</h1><p>Doorbell + Snapshot + Event Logger + Device Registry + WebSocket</p></header>
<main class="wrap">
<section class="card"><h2 class="ok">Server OK</h2><p>Railway Node.js Server is running.</p>
<a class="btn" href="/rt7_cloud_original_ui_doorbell">原始 UI 雲端門鈴</a>
<a class="btn green" href="/rt7_cloud_phase10_no_nodered">Phase10 雲端影像/對講/AI（無 Node-RED）</a>
<a class="btn" href="/rt7_cloud_doorbell_player">雲端門鈴播放器</a>
<a class="btn" href="/rt7_cloud_admin">雲端管理頁</a>
<a class="btn green" href="/rt7_snapshot_bridge_test">V4.2 Snapshot Bridge 測試頁</a>
<a class="btn" href="/api/rt7/camera/state">Snapshot 狀態 JSON</a>
<a class="btn" href="/api/rt7/doorbell/state">門鈴狀態 JSON</a>
<a class="btn" href="/api/events/latest">事件紀錄 JSON</a>
</section>
<section class="card"><h3>部署策略</h3><p>V4.3 採「原始手機 UI + 最新 Snapshot」：保留原始 UI 風格，只把 ESP32 主動上傳的最新照片整合到手機畫面；不混入對講或 Face Match。</p></section>
</main>`));
});

app.get('/health', (req, res) => res.json({ ok: true, version: SERVER_VERSION, time: nowIso() }));

// V4.9A product system status: one endpoint for user support and maintenance.
app.get('/api/rt7/system/status', (req, res) => {
  let devices = [];
  try { devices = readDevices(); } catch (_) {}
  let snapshot = null;
  try { snapshot = getSnapshotMeta_ ? getSnapshotMeta_() : null; } catch (_) {}
  let events = [];
  try { events = readEvents(10); } catch (_) {}
  streamViewerPrune_ && streamViewerPrune_();
  res.json({
    ok: true,
    version: SERVER_VERSION,
    product: 'NO_NODERED_NO_TAILSCALE',
    cleanup: 'V5.0B_CLOUD_STREAM_KEEPALIVE_FIX',
    stable_base: 'V4.8F11/V4.9A',
    time: nowIso(),
    railway: { ok: true, port: process.env.PORT || 3000 },
    dependencies: { nodered: false, tailscale: false },
    devices,
    current_device: cloudState.current_device_id || '#1',
    ai_enabled: !!cloudState.ai_enabled,
    doorbell: doorbellState,
    stream: liveStreamState,
    snapshot,
    latest_events: events
  });
});

// ---------- Doorbell API: keep legacy Node-RED endpoint compatible ----------
function handleDoorbell(req, res, endpointName) {
  const body = req.body || {};
  doorbellState.count += 1;
  const dev = normalizeDevice(body, req);
  registerOrUpdateDevice(dev);
  const last = {
    type: 'doorbell',
    endpoint: endpointName,
    device_id: dev.id,
    device_name: dev.name,
    ip: body.ip || dev.ip,
    source: body.source || 'esp32_button',
    count: body.count || doorbellState.count,
    message: body.message || '有人按門鈴',
    time: nowIso()
  };
  doorbellState.last = last;
  const event = appendEvent(last);
  broadcast('doorbell', event);
  console.log('[RT7][DOORBELL]', JSON.stringify(event));
  res.json({ ok: true, message: 'doorbell received', state: doorbellState, event });
}

app.post('/api/rt7/phase9n/doorbell/event', (req, res) => handleDoorbell(req, res, 'legacy_phase9n'));
app.get('/api/rt7/phase9n/doorbell/event', (req, res) => handleDoorbell(req, res, 'legacy_phase9n_get'));
app.post('/api/rt7/doorbell/ring', (req, res) => handleDoorbell(req, res, 'legacy_ring'));
app.get('/api/rt7/doorbell/ring', (req, res) => handleDoorbell(req, res, 'legacy_ring_get'));
app.post('/api/rt7/doorbell', (req, res) => handleDoorbell(req, res, 'compat_rt7_doorbell'));
app.get('/api/rt7/doorbell', (req, res) => handleDoorbell(req, res, 'compat_rt7_doorbell_get'));
app.post('/api/rt7/doorbell/event', (req, res) => handleDoorbell(req, res, 'compat_rt7_event'));
app.get('/api/rt7/doorbell/event', (req, res) => handleDoorbell(req, res, 'compat_rt7_event_get'));
app.post('/api/doorbell', (req, res) => handleDoorbell(req, res, 'cloud_v3'));
app.get('/api/doorbell', (req, res) => handleDoorbell(req, res, 'cloud_v3_get'));
app.get('/api/rt7/doorbell/state', (req, res) => res.json({ ok: true, state: doorbellState }));
app.get('/api/doorbell/state', (req, res) => res.json({ ok: true, state: doorbellState }));

// ---------- Event Logger ----------
app.post('/api/events/log', (req, res) => {
  const event = appendEvent(Object.assign({ type: req.body?.type || 'event' }, req.body || {}, { ip: req.body?.ip || clientIp(req) }));
  broadcast('event', event);
  res.json({ ok: true, event });
});

app.get('/api/events/latest', (req, res) => {
  const limit = Math.min(parseInt(req.query.limit || '200', 10) || 200, 2000);
  res.json({ ok: true, events: readEvents(limit) });
});

app.get('/api/events/clear', (req, res) => {
  ensureDataDir();
  fs.writeFileSync(EVENT_LOG, '', 'utf8');
  appendEvent({ type: 'events_clear', message: 'event log cleared' });
  res.json({ ok: true, message: 'cleared' });
});

// ---------- Device Registry ----------
app.post('/api/device/register', (req, res) => {
  const dev = registerOrUpdateDevice(normalizeDevice(req.body || {}, req));
  const event = appendEvent({ type: 'device_register', device_id: dev.id, device_name: dev.name, ip: dev.ip, version: dev.version, message: 'device registered' });
  broadcast('device_register', event);
  res.json({ ok: true, device: dev, devices: readDevices() });
});

app.get('/api/devices', (req, res) => res.json({ ok: true, devices: readDevices() }));
app.post('/api/devices/save', (req, res) => {
  const devices = saveDevices(req.body?.devices || req.body || []);
  const event = appendEvent({ type: 'devices_save', device_count: devices.length, message: 'devices saved' });
  broadcast('devices_save', event);
  res.json({ ok: true, devices });
});

// ---------- Test endpoint ----------
app.get('/api/test/doorbell', (req, res) => {
  req.body = { source: 'web_test', device_id: '#1', device_name: 'RT7 ESP32-S3-CAM', ip: req.query.ip || 'web' };
  handleDoorbell(req, res, 'web_test');
});

// ---------- Phone player page ----------
app.get('/rt7_cloud_doorbell_player', (req, res) => {
  res.type('html').send(htmlShell('RT7 Cloud Doorbell Player', `${baseCss}
<header class="top"><h1>RT7 Cloud Doorbell Player</h1><p>Railway → 手機提示音</p></header>
<main class="wrap">
<section class="card" style="text-align:center"><div class="big">🔔</div><h2 id="banner">等待門鈴事件</h2><p>目前 count：<b id="count">0</b></p><p class="muted">最後事件：<span id="lastTime">-</span></p></section>
<section class="card"><button class="btn green" onclick="enableAudio()">啟用提示音</button><button class="btn" onclick="playBell()">測試提示音</button><button class="btn gray" onclick="poll(true)">立即讀取</button><button class="btn red" onclick="resetLocal()">本機重設顯示</button><p class="warn">手機瀏覽器通常要先按一次「啟用提示音」，後續門鈴事件才可自動播放。</p></section>
<section class="card"><h3>狀態 JSON</h3><pre id="json" class="status">loading...</pre></section>
</main>
<script>
let audioCtx=null, audioEnabled=false, lastSeenCount=null;
function $(id){return document.getElementById(id)}
async function enableAudio(){
  try{
    audioCtx = audioCtx || new (window.AudioContext||window.webkitAudioContext)();
    await audioCtx.resume();
    audioEnabled=true;
    playBell();
    $('banner').textContent='提示音已啟用，等待門鈴';
  }catch(e){alert('啟用提示音失敗：'+e.message)}
}
function tone(freq, delay, dur){
  if(!audioCtx) return;
  setTimeout(()=>{
    const o=audioCtx.createOscillator(); const g=audioCtx.createGain();
    o.frequency.value=freq; g.gain.value=0.22;
    o.connect(g); g.connect(audioCtx.destination);
    o.start(); setTimeout(()=>{try{o.stop()}catch(e){}}, dur);
  }, delay);
}
function playBell(){
  if(!audioCtx){return;}
  tone(880,0,180); tone(660,260,220);
}
function resetLocal(){lastSeenCount=null; $('banner').textContent='本機已重設，下一次事件會提示';}
async function poll(manual){
  try{
    const r=await fetch('/api/rt7/doorbell/state?_='+Date.now(),{cache:'no-store'});
    const j=await r.json();
    $('json').textContent=JSON.stringify(j,null,2);
    const st=j.state||{}; const c=st.count||0; const last=st.last||null;
    $('count').textContent=c;
    $('lastTime').textContent=last?(new Date(last.time).toLocaleString('zh-TW',{hour12:false})):'-';
    if(lastSeenCount===null){ lastSeenCount=c; if(manual && c>0) $('banner').textContent='🔔 有人按門鈴 #'+c; return; }
    if(c>lastSeenCount){
      lastSeenCount=c;
      $('banner').textContent='🔔 有人按門鈴 #'+c;
      if(audioEnabled) playBell();
    }
  }catch(e){$('json').textContent='ERROR '+e.message;}
}
setInterval(()=>poll(false),1000);
poll(false);
try{
  const wsProto=location.protocol==='https:'?'wss':'ws';
  const ws=new WebSocket(wsProto+'://'+location.host+'/ws');
  ws.onmessage=(ev)=>{try{const m=JSON.parse(ev.data); if(m.type==='doorbell') poll(true);}catch(e){}};
}catch(e){}
</script>`));
});

// ---------- Admin page ----------
app.get('/rt7_cloud_admin', (req, res) => {
  res.type('html').send(htmlShell('RT7 Cloud Admin V3', `${baseCss}
<header class="top"><h1>RT7 Cloud Admin V3</h1><p>Devices / Events / Doorbell</p></header>
<main class="wrap">
<section class="card"><a class="btn" href="/rt7_cloud_doorbell_player">門鈴播放器</a><button class="btn gray" onclick="loadAll()">重新讀取</button><button class="btn red" onclick="clearEvents()">清除事件</button></section>
<section class="card"><h2>設備清單</h2><div id="devices">loading...</div></section>
<section class="card"><h2>事件紀錄</h2><div style="overflow:auto"><table><thead><tr><th>時間</th><th>設備</th><th>IP</th><th>事件</th><th>內容</th></tr></thead><tbody id="events"></tbody></table></div></section>
<section class="card"><h3>Status</h3><pre id="status" class="status">ready</pre></section>
</main>
<script>
function $(id){return document.getElementById(id)}
function esc(s){return String(s??'').replace(/[&<>\"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;'}[c]));}
async function j(url,opt){const r=await fetch(url,opt);return await r.json();}
async function loadAll(){
 const [d,e]=await Promise.all([j('/api/devices'),j('/api/events/latest?limit=200')]);
 $('devices').innerHTML=(d.devices||[]).map(x=>'<div class="card"><b>'+esc(x.id)+' '+esc(x.name)+'</b><br><code>'+esc(x.ip)+'</code><br><span class="muted">'+esc(x.version||'')+' '+esc(x.last_online||'')+'</span></div>').join('')||'no devices';
 $('events').innerHTML=(e.events||[]).reverse().map(x=>'<tr><td>'+esc(new Date(x.time||Date.now()).toLocaleString('zh-TW',{hour12:false}))+'</td><td>'+esc(x.device_id||'')+' '+esc(x.device_name||'')+'</td><td>'+esc(x.ip||'')+'</td><td>'+esc(x.type||'')+'</td><td>'+esc(x.message||JSON.stringify(x))+'</td></tr>').join('')||'<tr><td colspan="5">no events</td></tr>';
 $('status').textContent='devices='+(d.devices||[]).length+' events='+(e.events||[]).length;
}
async function clearEvents(){await j('/api/events/clear');loadAll();}
loadAll();
</script>`));
});




// ---------- V4.2 Snapshot Bridge test page ----------
app.get('/rt7_snapshot_bridge_test', (req, res) => {
  res.type('html').send(htmlShell('RT7 V4.2 Snapshot Bridge Test', `${baseCss}
<header class="top"><h1>RT7 V4.2 Snapshot Bridge</h1><p>只測 ESP32 → Railway Snapshot 上傳 / 手機讀取</p></header>
<main class="wrap">
<section class="card"><h2>測試目標</h2><p>本頁只驗證 Snapshot Bridge，不測對講、不測 Face Match、不測 AI Vision。</p><div class="grid"><a class="btn green" href="/api/rt7/camera/state">Snapshot 狀態</a><a class="btn" href="/api/rt7/camera/latest.jpg" target="_blank">開啟最新 JPG</a><button class="btn gray" onclick="refreshState()">重新讀取</button><button class="btn red" onclick="clearSnapshot()">清除 Snapshot</button></div></section>
<section class="card"><h2>最新 Snapshot</h2><div style="background:#000;aspect-ratio:4/3;border-radius:12px;display:flex;align-items:center;justify-content:center;overflow:hidden"><img id="img" style="max-width:100%;max-height:100%;display:none"><div id="empty" style="color:#cbd5e1;font-weight:900;text-align:center">尚無照片<br><span class="muted">請 ESP32 POST /api/rt7/camera/snapshot</span></div></div><p class="muted">圖片 URL：<code>/api/rt7/camera/latest.jpg</code></p></section>
<section class="card"><h2>ESP32 上傳方式</h2><p>方式 A：直接 POST JPEG binary：</p><pre class="status">POST https://rt7-cloud-server-production.up.railway.app/api/rt7/camera/snapshot
Content-Type: image/jpeg
Body: JPEG bytes</pre><p>方式 B：POST base64 JSON：</p><pre class="status">POST /api/rt7/camera/snapshot_json
Content-Type: application/json
{"image_b64":"...","device_id":"#1"}</pre></section>
<section class="card"><h2>目前狀態</h2><pre id="log" class="status">loading...</pre></section>
</main>
<script>
const $=id=>document.getElementById(id);
async function j(url,opt){const r=await fetch(url+(url.includes('?')?'&':'?')+'_='+Date.now(),Object.assign({cache:'no-store'},opt||{}));const t=await r.text();try{return JSON.parse(t)}catch(e){return{ok:r.ok,status:r.status,raw:t}}}
async function refreshState(){const s=await j('/api/rt7/camera/state');$('log').textContent=JSON.stringify(s,null,2);if(s.latest_url){$('img').src=s.latest_url+'?_='+Date.now();$('img').style.display='block';$('empty').style.display='none';}else{$('img').style.display='none';$('empty').style.display='block';}}
async function clearSnapshot(){const s=await j('/api/rt7/camera/clear',{method:'POST'});$('log').textContent=JSON.stringify(s,null,2);refreshState();}
setInterval(refreshState,3000);refreshState();
try{const ws=new WebSocket((location.protocol==='https:'?'wss://':'ws://')+location.host+'/ws');ws.onmessage=e=>{try{const m=JSON.parse(e.data);if(m.type==='snapshot')refreshState();}catch(_){}}}catch(e){}
</script>`));
});



// ---------- V5.2A Railway-only Face Detection + Face Match (no GPT face recognition) ----------
const FACES_FILE = path.join(DATA_DIR, 'rt7_faces.json');
const FACE_DEBUG_SNAPSHOT_FILE = path.join(DATA_DIR, 'rt7_face_last_ai_snapshot.jpg');
function rt7ReadFaces_() {
  ensureDataDir();
  try {
    const raw = fs.existsSync(FACES_FILE) ? fs.readFileSync(FACES_FILE, 'utf8') : '[]';
    const arr = JSON.parse(raw || '[]');
    return Array.isArray(arr) ? arr : [];
  } catch (_) { return []; }
}
function rt7SaveFaces_(arr) {
  ensureDataDir();
  fs.writeFileSync(FACES_FILE, JSON.stringify(Array.isArray(arr) ? arr : [], null, 2), 'utf8');
}
function rt7LatestJpegB64_() {
  ensureDataDir();
  if (!fs.existsSync(SNAPSHOT_FILE)) return null;
  const buf = fs.readFileSync(SNAPSHOT_FILE);
  if (!buf || buf.length < 800) return null;
  return { b64: buf.toString('base64'), bytes: buf.length };
}
function rt7ParseFaceJson_(txt) {
  const raw = String(txt || '').trim();
  const m = raw.match(/\{[\s\S]*\}/);
  if (m) { try { return JSON.parse(m[0]); } catch (_) {} }
  return { ok:true, known_face:false, matched_name:'', confidence:0, summary:raw.slice(0,240) };
}


function rt7GetLatestWithMeta_() {
  const latest = rt7LatestJpegB64_();
  if (!latest) return null;
  const latestMeta = getSnapshotMeta_() || {};
  latest.snap_time = latestMeta.time || nowIso();
  latest.snap_source = latestMeta.source || 'latest.jpg';
  latest.snap_hash = rt7QuickHash_(latest.b64);
  latest.snap_age_ms = latestMeta.time ? Math.max(0, Date.now() - new Date(latestMeta.time).getTime()) : null;
  latest.meta = latestMeta;
  return latest;
}
function rt7SendWsJsonToEsp_(obj) {
  // V5.2B/V5.2C: make face snapshot command delivery robust.
  // Previous build sent only to clients already tagged as ESP. In field tests the ESP32
  // persistent WS was uploading frames/keepalive, but the tag was not always visible to
  // the face-match request path, so WS_SENT stayed 0.
  // Safe fix: send command JSON to all non-phone viewer clients and all ESP-like clients.
  let n = 0;
  let seen = 0;
  try {
    const text = JSON.stringify(obj);
    for (const ws of wss.clients) {
      if (!ws || ws.readyState !== WebSocket.OPEN) continue;
      seen++;
      const role = safeString(ws.rt7Role || '').toLowerCase();
      const pcmRole = safeString(ws.rt7PcmRole || '').toLowerCase();
      const isPhone = role.includes('phone') || role.includes('viewer');
      const isEsp = role.includes('esp') || pcmRole.includes('esp') || ws.rt7PcmClient === true || role === 'esp32_frame_upload' || role === 'control' || !role;
      if (isPhone && !isEsp) continue;
      try { ws.send(text); n++; } catch (_) {}
    }
    console.log('[FACE_API][V54O][WS_CMD_RELAY] type=' + safeString(obj && obj.type) + ' sent=' + n + ' open=' + seen);
  } catch (e) { console.warn('[FACE_API][V54O][WS_CMD_RELAY_ERR] ' + String(e && e.message || e)); }
  return n;
}
function rt7Sleep_(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
async function rt7ForceRealtimeSnapshot_() {
  const startMs = Date.now();
  const beforeMeta = getSnapshotMeta_() || {};
  const beforeTimeMs = beforeMeta.time ? new Date(beforeMeta.time).getTime() : 0;
  const requestId = 'face_snap_' + startMs + '_' + Math.floor(Math.random()*1000);
  // V5.2D: use the same cloud command polling path that already works for door/open commands.
  // Queue as wildcard device_id first, because field tests showed WS_SENT may be >0 but
  // ESP32 still receives no WS control message while streaming. The ESP32 polls
  // /api/rt7/device/commands/next, so this command must be visible there.
  // V5.2S: clear stale pending face snapshot commands before creating a new one.
  // This restores the V5.2N successful face path, but prevents stacked old face commands.
  try {
    if (Array.isArray(cloudState.command_queue)) {
      cloudState.command_queue = cloudState.command_queue.filter(c => !(c && c.status === 'pending' && (c.command === 'face_snapshot_now' || c.action === 'face_snapshot_now' || c.priority === 'face_snapshot')));
      saveState();
    }
    if (Array.isArray(pendingCommands)) {
      pendingCommands = pendingCommands.filter(c => !(c && (c.command === 'face_snapshot_now' || c.action === 'face_snapshot_now' || c.priority === 'face_snapshot')));
    }
  } catch (e) { console.warn('[FACE_API][V54O][CLEAR_PENDING_WARN] ' + String(e && e.message || e)); }

  const cmd = queueCommand({
    command:'face_snapshot_now', action:'face_snapshot_now', request_id:requestId,
    device_id:'rt7-esp32-s3-cam-01', requested_device_id:'#1', target_all:true, interval_ms:100,
    priority:'face_snapshot',
    message:'V54O single-shot face snapshot trigger; duplicate face_snapshot_now suppressed'
  });
  const wsSentA = rt7SendWsJsonToEsp_({ type:'face_snapshot_now', command:'face_snapshot_now', request_id:requestId, phase:'V54O', time:nowIso() });
  const wsSentB = rt7SendToEspIntercom_(JSON.stringify({ type:'face_snapshot_now', command:'face_snapshot_now', request_id:requestId, phase:'V54O', relay:'intercom_path', time:nowIso() }));
  const wsSent = wsSentA + wsSentB;
  broadcast('face_snapshot_request', { ok:true, version:SERVER_VERSION, request_id:requestId, command:cmd, ws_sent:wsSent, time:nowIso() });
  console.log('[FACE_API][V54O][SNAPSHOT_REQUEST] request_id=' + requestId + ' ws_sent=' + wsSent + ' before_time=' + (beforeMeta.time||'') + ' before_source=' + (beforeMeta.source||''));

  let latest = null;
  for (let i=0; i<90; i++) {
    await rt7Sleep_(100);
    latest = rt7GetLatestWithMeta_();
    if (!latest) continue;
    const tMs = latest.snap_time ? new Date(latest.snap_time).getTime() : 0;
    const freshByTime = tMs >= startMs - 250;
    const newerThanBefore = !beforeTimeMs || tMs > beforeTimeMs;
    const freshByAge = latest.snap_age_ms !== null && latest.snap_age_ms <= 1800;
    const isLiveFrame = latest.snap_source === 'ws_frame' || latest.snap_source === 'live_frame' || latest.snap_source === 'raw_post' || latest.snap_source === 'json_b64';
    // V5.2C: in field tests WS_SENT may be 0 because the ESP32 stream client is not command-addressable,
    // but the live stream is still uploading a fresh frame every ~100ms. Treat a very fresh live frame as
    // realtime Snapshot, otherwise face recognition waits forever and returns NO_REALTIME_SNAPSHOT.
    if (latest.bytes >= 800 && ((freshByTime && newerThanBefore) || (freshByAge && isLiveFrame))) {
      latest.snap_request_id = requestId;
      latest.snap_request_ws_sent = wsSent;
      latest.snap_wait_ms = Date.now() - startMs;
      latest.snap_forced_realtime = true;
      latest.snap_live_frame_fallback = !(freshByTime && newerThanBefore);
      latest.snap_source = (latest.snap_source === 'raw_post' || latest.snap_source === 'json_b64') ? 'realtime_snapshot' : (latest.snap_live_frame_fallback ? 'realtime_live_ws_frame' : 'realtime_ws_frame');
      console.log('[FACE_API][V54O][SNAPSHOT_FRESH_OR_LIVE] request_id=' + requestId + ' wait_ms=' + latest.snap_wait_ms + ' bytes=' + latest.bytes + ' hash=' + latest.snap_hash + ' age_ms=' + latest.snap_age_ms + ' source=' + latest.snap_source + ' ws_sent=' + wsSent + ' fallback=' + latest.snap_live_frame_fallback);
      return latest;
    }
  }
  // V5.1C: realtime-only. Do NOT fall back to stale ws_frame/latest.jpg.
  // If ESP32 does not provide a new frame after the request, return null so face match stops before AI.
  const stale = rt7GetLatestWithMeta_();
  console.warn('[FACE_API][V54O][SNAPSHOT_REALTIME_TIMEOUT] request_id=' + requestId + ' wait_ms=' + (Date.now() - startMs) + ' ws_sent=' + wsSent + ' last_hash=' + (stale && stale.snap_hash || '') + ' last_age_ms=' + (stale && stale.snap_age_ms || '') + ' last_source=' + (stale && stale.snap_source || ''));
  return {
    realtime_failed: true,
    snap_request_id: requestId,
    snap_request_ws_sent: wsSent,
    snap_wait_ms: Date.now() - startMs,
    snap_forced_realtime: true,
    snap_stale_warning: true,
    stale_meta: stale || null
  };
}

function rt7FaceGateCheck_(latest) {
  const bytes = Number(latest?.bytes || 0);
  const ageMs = latest?.time ? Math.max(0, Date.now() - new Date(latest.time).getTime()) : 0;
  // V50W: test-mode gate. This is a safe cloud-side precheck so the known-good V5.0S recognition path remains usable.
  // Real ESP32 FACE_GATE can be reintroduced after this toggle confirms ON/OFF routing.
  const pass = bytes >= 2500;
  const gate = {
    enabled: !!cloudState.face_gate_enabled,
    source: 'V51A_CLOUD_GATE_TEST',
    pass,
    bytes,
    age_ms: ageMs,
    reason: pass ? 'GATE_PASS_BYTES_OK' : 'GATE_SKIP_JPEG_TOO_SMALL',
    time: nowIso()
  };
  cloudState.last_face_gate = gate;
  console.log('[RT7_FACE_GATE][TOGGLE][V52D] enabled=' + gate.enabled + ' pass=' + gate.pass + ' bytes=' + gate.bytes + ' age_ms=' + gate.age_ms + ' reason=' + gate.reason);
  return gate;
}

function rt7QuickHash_(bufOrB64) {
  let buf = Buffer.isBuffer(bufOrB64) ? bufOrB64 : Buffer.from(String(bufOrB64 || ''), 'base64');
  let h = 2166136261 >>> 0;
  for (let i = 0; i < buf.length; i += Math.max(1, Math.floor(buf.length / 2048))) {
    h ^= buf[i]; h = Math.imul(h, 16777619) >>> 0;
  }
  return ('00000000' + h.toString(16).toUpperCase()).slice(-8);
}

function rt7SkinLike_(r, g, b) {
  const y  =  0.299*r + 0.587*g + 0.114*b;
  const cb = 128 - 0.168736*r - 0.331264*g + 0.5*b;
  const cr = 128 + 0.5*r - 0.418688*g - 0.081312*b;
  const maxc = Math.max(r,g,b), minc = Math.min(r,g,b);
  // YCbCr + simple color spread. This avoids gray wall/curtain being counted as face.
  return y > 45 && cr >= 133 && cr <= 185 && cb >= 75 && cb <= 145 && (maxc - minc) > 12 && r > b * 0.85 && r > g * 0.72;
}

function rt7RealFaceCountDetect_(latest) {
  const empty = {
    face_found:false, face_count:0, face_box:{x:0,y:0,w:0,h:0}, face_ratio:0,
    face_quality:'NO_FACE', face_position:'UNKNOWN', reason:'NO_FACE',
    summary:'目前畫面未偵測到人臉。', raw:'LOCAL_REAL_FACE_COUNT_V52D'
  };
  let img;
  try { img = jpeg.decode(Buffer.from(latest.b64, 'base64'), { useTArray:true, maxMemoryUsageInMB:80 }); }
  catch (e) { return { ...empty, face_quality:'DECODE_FAIL', reason:'JPEG_DECODE_FAIL', summary:'JPEG 解碼失敗：' + String(e && e.message || e).slice(0,80) }; }
  const W = img.width || 0, H = img.height || 0;
  if (!W || !H || !img.data) return { ...empty, reason:'JPEG_EMPTY', summary:'JPEG 無有效影像資料。' };

  const sw = 160, sh = Math.max(1, Math.round(H * sw / W));
  const mask = new Uint8Array(sw * sh);
  const lum = new Uint8Array(sw * sh);
  let skinTotal = 0;
  for (let yy=0; yy<sh; yy++) {
    const sy = Math.min(H-1, Math.floor(yy * H / sh));
    for (let xx=0; xx<sw; xx++) {
      const sx = Math.min(W-1, Math.floor(xx * W / sw));
      const p = (sy * W + sx) * 4;
      const r = img.data[p], g = img.data[p+1], b = img.data[p+2];
      const yv = Math.max(0, Math.min(255, Math.round(0.299*r + 0.587*g + 0.114*b)));
      lum[yy*sw+xx] = yv;
      if (rt7SkinLike_(r,g,b)) { mask[yy*sw+xx] = 1; skinTotal++; }
    }
  }
  if (skinTotal < sw*sh*0.012) return { ...empty, reason:'NO_SKIN_FACE_CANDIDATE', summary:'未偵測到足夠的人臉膚色區塊。' };

  const seen = new Uint8Array(sw * sh);
  const comps = [];
  const qx = new Int16Array(sw*sh), qy = new Int16Array(sw*sh);
  for (let y=0; y<sh; y++) for (let x=0; x<sw; x++) {
    const idx=y*sw+x;
    if (!mask[idx] || seen[idx]) continue;
    let head=0, tail=0, area=0, minx=x, maxx=x, miny=y, maxy=y;
    seen[idx]=1; qx[tail]=x; qy[tail]=y; tail++;
    while (head<tail) {
      const cx=qx[head], cy=qy[head]; head++; area++;
      if (cx<minx) minx=cx; if (cx>maxx) maxx=cx; if (cy<miny) miny=cy; if (cy>maxy) maxy=cy;
      const nbs=[[cx+1,cy],[cx-1,cy],[cx,cy+1],[cx,cy-1]];
      for (const [nx,ny] of nbs) {
        if (nx<0||ny<0||nx>=sw||ny>=sh) continue;
        const ni=ny*sw+nx;
        if (mask[ni] && !seen[ni]) { seen[ni]=1; qx[tail]=nx; qy[tail]=ny; tail++; }
      }
    }
    if (area>20) comps.push({area,minx,maxx,miny,maxy,w:maxx-minx+1,h:maxy-miny+1});
  }
  comps.sort((a,b)=>b.area-a.area);

  function darkFeatureCount(c) {
    let sum=0, n=0;
    for (let y=c.miny; y<=c.maxy; y++) for (let x=c.minx; x<=c.maxx; x++) { sum += lum[y*sw+x]; n++; }
    const avg = n ? sum/n : 128;
    const dark = new Uint8Array(sw*sh);
    const y0 = c.miny + Math.floor(c.h*0.18), y1 = c.miny + Math.floor(c.h*0.72);
    const x0 = c.minx + Math.floor(c.w*0.12), x1 = c.maxx - Math.floor(c.w*0.12);
    let dtotal=0;
    for (let y=y0; y<=y1; y++) for (let x=x0; x<=x1; x++) {
      const idx=y*sw+x;
      if (lum[idx] < avg - 18) { dark[idx]=1; dtotal++; }
    }
    if (dtotal < Math.max(8, c.area*0.015)) return {count:0,dark_ratio:0};
    const seenD = new Uint8Array(sw*sh); let cnt=0;
    const qx2 = new Int16Array(sw*sh), qy2 = new Int16Array(sw*sh);
    for (let y=y0; y<=y1; y++) for (let x=x0; x<=x1; x++) {
      const idx=y*sw+x; if (!dark[idx] || seenD[idx]) continue;
      let head=0, tail=0, area=0, minx=x,maxx=x,miny=y,maxy=y;
      seenD[idx]=1; qx2[tail]=x; qy2[tail]=y; tail++;
      while(head<tail){ const cx=qx2[head],cy=qy2[head]; head++; area++; if(cx<minx)minx=cx;if(cx>maxx)maxx=cx;if(cy<miny)miny=cy;if(cy>maxy)maxy=cy;
        for (const [nx,ny] of [[cx+1,cy],[cx-1,cy],[cx,cy+1],[cx,cy-1]]) { if(nx<0||ny<0||nx>=sw||ny>=sh)continue; const ni=ny*sw+nx; if(dark[ni]&&!seenD[ni]){seenD[ni]=1;qx2[tail]=nx;qy2[tail]=ny;tail++;} }
      }
      const ww=maxx-minx+1, hh=maxy-miny+1;
      if (area>=3 && area<=c.area*0.20 && ww<=c.w*0.55 && hh<=c.h*0.45) cnt++;
    }
    return {count:cnt, dark_ratio: Math.round(dtotal * 1000 / Math.max(1,c.area))/10};
  }

  const candidates=[];
  for (const c of comps.slice(0,8)) {
    const ratioArea = c.area / (sw*sh);
    const asp = c.w / Math.max(1,c.h);
    const fill = c.area / Math.max(1, c.w*c.h);
    if (ratioArea < 0.018 || ratioArea > 0.70) continue;
    if (c.w < 18 || c.h < 18) continue;
    if (asp < 0.42 || asp > 1.45) continue;
    if (fill < 0.22) continue;
    const feat = darkFeatureCount(c);
    // Real face count rule: require skin blob AND internal dark facial features.
    if (feat.count < 2 && ratioArea < 0.22) continue;
    candidates.push({...c, ratioArea, asp, fill, features:feat.count, dark_ratio:feat.dark_ratio});
  }
  if (!candidates.length) {
    const c = comps[0];
    return { ...empty, reason:'NO_FACE_DETECTED', summary:'未偵測到符合人臉形狀與五官特徵的區塊。', local_skin_total:skinTotal, largest_skin_box:c?{x:c.minx,y:c.miny,w:c.w,h:c.h}:null };
  }

  const best = candidates[0];
  const scaleX = W / sw, scaleY = H / sh;
  const box = { x:Math.round(best.minx*scaleX), y:Math.round(best.miny*scaleY), w:Math.round(best.w*scaleX), h:Math.round(best.h*scaleY) };
  const faceRatio = Math.round((box.w * box.h) * 100 / Math.max(1, W*H));
  const cx = best.minx + best.w/2, cy = best.miny + best.h/2;
  let pos = 'CENTER';
  if (cx < sw*0.30) pos='LEFT'; else if (cx > sw*0.70) pos='RIGHT';
  if (cy < sh*0.25) pos = pos==='CENTER' ? 'TOP' : 'CORNER'; else if (cy > sh*0.78) pos = pos==='CENTER' ? 'BOTTOM' : 'CORNER';
  let quality = 'OK';
  if (faceRatio >= 18 && best.features >= 2) quality='GOOD';
  if (faceRatio < 7 || best.features < 2) quality='LOW';
  return {
    face_found:true,
    face_count:candidates.length,
    face_box:box,
    face_ratio:faceRatio,
    face_quality:quality,
    face_position:pos,
    reason:'FACE_OK',
    summary:'本機影像偵測到 ' + candidates.length + ' 個人臉候選區塊。',
    raw:'LOCAL_REAL_FACE_COUNT_V52D features=' + best.features + ' skin=' + skinTotal + ' blob=' + JSON.stringify({w:best.w,h:best.h,area:best.area,fill:best.fill,dark_ratio:best.dark_ratio}),
    local_debug:{ width:W, height:H, skin_total:skinTotal, candidates:candidates.slice(0,3).map(c=>({w:c.w,h:c.h,area:c.area,features:c.features,dark_ratio:c.dark_ratio,fill:Number(c.fill.toFixed(2))})) }
  };
}

async function rt7DetectFaceOnly_(latest) {
  // V5.0Z: real local face-count gate first. It does not see registered photos, so an empty room cannot match gwansyan.
  return rt7RealFaceCountDetect_(latest);
}


function rt7Clamp_(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }
function rt7DecodeJpeg_(b64) {
  return jpeg.decode(Buffer.from(String(b64 || ''), 'base64'), { useTArray:true, maxMemoryUsageInMB:80 });
}
function rt7ExtractFaceEmbedding_(b64, detectOpt) {
  let img;
  try { img = rt7DecodeJpeg_(b64); }
  catch (e) { return { ok:false, reason:'DECODE_FAIL', error:String(e && e.message || e) }; }
  const W = img.width || 0, H = img.height || 0;
  if (!W || !H || !img.data) return { ok:false, reason:'JPEG_EMPTY' };

  let detect = detectOpt || null;
  if (!detect || !detect.face_found) {
    detect = rt7RealFaceCountDetect_({ b64 });
  }

  let box = detect && detect.face_found && detect.face_box ? detect.face_box : null;
  if (!box || !box.w || !box.h) {
    // Enrollment fallback: use center crop so old registered photos still remain usable.
    const sideW = Math.round(W * 0.58), sideH = Math.round(H * 0.72);
    box = { x:Math.round((W-sideW)/2), y:Math.round((H-sideH)/2), w:sideW, h:sideH };
  }

  const x0 = rt7Clamp_(Math.round(box.x), 0, W-1);
  const y0 = rt7Clamp_(Math.round(box.y), 0, H-1);
  const bw = rt7Clamp_(Math.round(box.w), 8, W-x0);
  const bh = rt7Clamp_(Math.round(box.h), 8, H-y0);

  // 16x16 luma embedding + 4x4 color bins. Pure JS, deterministic, Railway-side only.
  const grid = 16;
  const luma = [];
  const crcb = [];
  let mean = 0;
  for (let gy=0; gy<grid; gy++) {
    for (let gx=0; gx<grid; gx++) {
      let rs=0, gs=0, bs=0, n=0;
      const sx0 = x0 + Math.floor(gx * bw / grid);
      const sx1 = x0 + Math.floor((gx+1) * bw / grid);
      const sy0 = y0 + Math.floor(gy * bh / grid);
      const sy1 = y0 + Math.floor((gy+1) * bh / grid);
      for (let yy=sy0; yy<Math.max(sy0+1,sy1); yy++) {
        for (let xx=sx0; xx<Math.max(sx0+1,sx1); xx++) {
          const i=(rt7Clamp_(yy,0,H-1)*W + rt7Clamp_(xx,0,W-1))*4;
          rs += img.data[i]; gs += img.data[i+1]; bs += img.data[i+2]; n++;
        }
      }
      const r=rs/Math.max(1,n), g=gs/Math.max(1,n), b=bs/Math.max(1,n);
      const y=0.299*r+0.587*g+0.114*b;
      const cb=128 - 0.168736*r - 0.331264*g + 0.5*b;
      const cr=128 + 0.5*r - 0.418688*g - 0.081312*b;
      luma.push(y); crcb.push(cr); crcb.push(cb); mean += y;
    }
  }
  mean /= luma.length;
  let variance=0;
  for (const v of luma) variance += (v-mean)*(v-mean);
  const std = Math.sqrt(variance / Math.max(1,luma.length)) || 1;
  const vec = [];
  for (const v of luma) vec.push((v-mean)/std);
  // Edge structure helps distinguish empty wall from face-like color patches.
  for (let y=1; y<grid-1; y++) {
    for (let x=1; x<grid-1; x++) {
      const i=y*grid+x;
      const dx=(luma[i+1]-luma[i-1])/255;
      const dy=(luma[i+grid]-luma[i-grid])/255;
      vec.push(dx); vec.push(dy);
    }
  }
  // Low-weight color signature.
  let crm=0, cbm=0;
  for (let i=0; i<crcb.length; i+=2) { crm += crcb[i]; cbm += crcb[i+1]; }
  crm /= (crcb.length/2); cbm /= (crcb.length/2);
  for (let i=0; i<crcb.length; i+=2) { vec.push((crcb[i]-crm)/80); vec.push((crcb[i+1]-cbm)/80); }

  return { ok:true, vector:vec, box, detect, width:W, height:H, embedding_len:vec.length, reason:'EMBED_OK' };
}

// ---------- V5.3A Face Fast Cache Match ----------
// Store Railway-local face embedding in rt7_faces.json at enrollment time.
// Matching can then reuse cached reference vectors instead of decoding all enrolled JPEGs every request.
// This does not use GPT/OpenAI; it is deterministic Railway-side face detect + embedding compare.
function rt7FaceEmbeddingCacheKey_(face) {
  return safeString((face && face.id) || '') + ':' + safeString((face && face.time) || '') + ':' + Number((face && face.bytes) || 0);
}
function rt7FaceEmbeddingToCache_(emb) {
  if (!emb || !emb.ok || !Array.isArray(emb.vector) || !emb.vector.length) return null;
  // Round to 4 decimals to keep rt7_faces.json compact while preserving similarity behavior.
  return {
    ok:true,
    model:'rt7_js_luma_edge_v1',
    vector:emb.vector.map(v => Number(Number(v || 0).toFixed(4))),
    box:emb.box || null,
    width:emb.width || 0,
    height:emb.height || 0,
    embedding_len:emb.embedding_len || emb.vector.length,
    reason:emb.reason || 'EMBED_OK',
    cache_time:nowIso()
  };
}
function rt7GetCachedRefEmbedding_(face) {
  if (!face) return { ok:false, reason:'NO_FACE_ROW' };
  const c = face.embedding_cache;
  if (c && c.ok && c.model === 'rt7_js_luma_edge_v1' && Array.isArray(c.vector) && c.vector.length > 64) {
    return { ok:true, vector:c.vector, box:c.box || null, width:c.width||0, height:c.height||0, embedding_len:c.embedding_len||c.vector.length, reason:'CACHE_HIT', cache_hit:true };
  }
  const emb = rt7ExtractFaceEmbedding_(face.image_b64, null);
  if (!emb.ok) return emb;
  try {
    face.embedding_cache = rt7FaceEmbeddingToCache_(emb);
    face.embedding_cache_key = rt7FaceEmbeddingCacheKey_(face);
  } catch(_) {}
  emb.cache_hit = false;
  return emb;
}
function rt7RefreshFaceEmbeddingCaches_(faces) {
  let changed = false;
  for (const f of faces || []) {
    if (!f || !f.image_b64) continue;
    if (f.embedding_cache && f.embedding_cache.ok && Array.isArray(f.embedding_cache.vector)) continue;
    const emb = rt7ExtractFaceEmbedding_(f.image_b64, null);
    if (emb.ok) {
      f.embedding_cache = rt7FaceEmbeddingToCache_(emb);
      f.embedding_cache_key = rt7FaceEmbeddingCacheKey_(f);
      changed = true;
    }
  }
  return changed;
}
function rt7Cosine_(a,b) {
  const n=Math.min(a.length,b.length); let dot=0, na=0, nb=0;
  for (let i=0;i<n;i++){ const x=Number(a[i]||0), y=Number(b[i]||0); dot += x*y; na += x*x; nb += y*y; }
  if (!na || !nb) return 0;
  return dot / Math.sqrt(na*nb);
}
function rt7RailwayFaceCompare_(latest, refs, detect) {
  const t0 = Date.now();
  const cur = rt7ExtractFaceEmbedding_(latest.b64, detect);
  if (!cur.ok) return { known_face:false, matched_name:'', confidence:0, reason:cur.reason || 'CURRENT_EMBED_FAIL', summary:'目前照片無法建立人臉特徵。', raw:'RAILWAY_FACE_MATCH_V54O', match_ms:Date.now()-t0, cache_mode:'FAST_CACHE' };
  let best = null;
  let cacheHits = 0, cacheMiss = 0, compared = 0;
  for (const f of refs || []) {
    const ref = rt7GetCachedRefEmbedding_(f);
    if (!ref.ok) continue;
    if (ref.cache_hit) cacheHits++; else cacheMiss++;
    compared++;
    const cos = rt7Cosine_(cur.vector, ref.vector);
    // map cosine to 0-100. This is intentionally conservative for door access.
    const conf = Math.round(rt7Clamp_(((cos + 1) / 2) * 100, 0, 100));
    const row = { name:safeString(f.name||''), confidence:conf, cosine:Number(cos.toFixed(4)), ref_box:ref.box, ref_reason:ref.reason, cache_hit:!!ref.cache_hit };
    if (!best || row.confidence > best.confidence) best = row;
  }
  const matchMs = Date.now() - t0;
  if (!best) return { known_face:false, matched_name:'', confidence:0, reason:'NO_VALID_REFERENCE_EMBEDDING', summary:'註冊照片無法建立可比對的人臉特徵。', raw:'RAILWAY_FACE_MATCH_V54O', match_ms:matchMs, cache_mode:'FAST_CACHE', cache_hits:cacheHits, cache_miss:cacheMiss, compared };
  // V5.4O threshold tuning: field tests showed GOOD+CENTER faces scoring 37-41%.
  // Use 40% as a practical single-user doorbell threshold while ESP32 FACE_GATE still filters candidates.
  const RT7_FACE_MATCH_THRESHOLD = 40;
  const pass = best.confidence >= RT7_FACE_MATCH_THRESHOLD;
  return {
    known_face: pass,
    matched_name: pass ? best.name : '',
    confidence: best.confidence,
    reason: pass ? 'FACE_OK_RAILWAY_MATCH' : 'LOW_SIMILARITY_RAILWAY',
    summary: pass ? ('Railway 快取比對通過：' + best.name + ' / ' + best.confidence + '% / threshold=40%') : ('Railway 已偵測到人臉，但與註冊名單相似度不足：' + best.confidence + '% / threshold=40%'),
    raw:'RAILWAY_FACE_MATCH_V54O cosine=' + best.cosine + ' name=' + best.name + ' match_ms=' + matchMs + ' cache_hits=' + cacheHits + ' cache_miss=' + cacheMiss,
    best,
    match_ms:matchMs,
    cache_mode:'FAST_CACHE',
    cache_hits:cacheHits,
    cache_miss:cacheMiss,
    compared
  };
}

async function rt7MatchKnownFaceOnly_(latest, refs, detect) {
  // V5.3A: Railway-only fast cached face match. No GPT/OpenAI call is used for door face recognition.
  return rt7RailwayFaceCompare_(latest, refs, detect);
}

async function rt7FaceMatchLatestCore_(providedLatest, opt) {
  opt = opt || {};
  const autoMode = !!opt.auto_face_gate;
  console.log('[FACE_API][V54O] /api/rt7/face/match CORE ENTER detect_first=1 force_realtime=' + (providedLatest ? '0' : '1') + ' auto_face_gate=' + (autoMode ? '1' : '0'));
  const latest = providedLatest || await rt7ForceRealtimeSnapshot_();
  if (!latest || latest.realtime_failed || !latest.b64) {
    const stale = latest && latest.stale_meta || null;
    const fail = {
      ok:true, version:SERVER_VERSION, api_entered:true, api_path:'/api/rt7/face/match', type:'face_match', stage:'REALTIME_SNAPSHOT_REQUIRED', engine:'railway_local', gpt_used:false,
      known_face:false, matched_name:'', confidence:0, face_found:false, face_count:0, face_box:{x:0,y:0,w:0,h:0}, face_ratio:0,
      face_quality:'NO_REALTIME_SNAPSHOT', face_position:'UNKNOWN', fail_stage:'SNAPSHOT', reason:'NO_REALTIME_SNAPSHOT',
      summary:'未取得即時 Snapshot，已停止：不使用舊 ws_frame，也不做人臉比對。請確認 ESP32 有持續上傳最新影像。',
      snap_time: stale && stale.snap_time || '', snap_source: stale && stale.snap_source || 'none', snap_hash: stale && stale.snap_hash || '', snap_age_ms: stale && stale.snap_age_ms || null,
      latest_bytes: stale && stale.bytes || 0, snap_wait_ms: latest && latest.snap_wait_ms || 0, snap_forced_realtime:true, snap_stale_warning:true, snap_request_ws_sent: latest && latest.snap_request_ws_sent || 0, snap_live_frame_fallback: latest && !!latest.snap_live_frame_fallback,
      debug_text:'REALTIME_ONLY=YES NO_REALTIME_SNAPSHOT STALE_HASH=' + (stale && stale.snap_hash || '') + ' AGE=' + (stale && stale.snap_age_ms || ''),
      time:nowIso()
    };
    cloudState.last_face_match = fail; broadcast('face_match', fail);
    console.warn('[FACE_API][V54O] stop before AI: NO_REALTIME_SNAPSHOT ws_sent=' + fail.snap_request_ws_sent + ' wait_ms=' + fail.snap_wait_ms + ' stale_hash=' + fail.snap_hash + ' stale_age_ms=' + fail.snap_age_ms);
    return fail;
  }
  try { fs.writeFileSync(FACE_DEBUG_SNAPSHOT_FILE, Buffer.from(latest.b64, 'base64')); } catch (e) { console.warn('[FACE_API][V54O] save face debug snapshot failed', e && e.message || e); }

  const gate = rt7FaceGateCheck_(latest);
  if (cloudState.face_gate_enabled && !gate.pass) {
    const skip = { ok:true, version:SERVER_VERSION, api_entered:true, type:'face_match', known_face:false, face_found:false, face_count:0, face_box:{x:0,y:0,w:0,h:0}, face_ratio:0, confidence:0, face_quality:'SKIP', reason:gate.reason, fail_stage:'FACE_GATE', face_gate:gate, snap_time:latest.snap_time, snap_hash:latest.snap_hash, snap_age_ms:latest.snap_age_ms, latest_bytes:latest.bytes, snap_wait_ms:latest.snap_wait_ms, snap_forced_realtime:latest.snap_forced_realtime, snap_stale_warning:!!latest.snap_stale_warning, snap_request_ws_sent:latest.snap_request_ws_sent, snap_live_frame_fallback:!!latest.snap_live_frame_fallback, face_snapshot_url:'/api/rt7/face/last_snapshot.jpg?h='+latest.snap_hash, summary:'FACE_GATE 測試模式阻擋，未做 Railway 比對。' };
    cloudState.last_face_match = skip; broadcast('face_match', skip);
    console.log('[FACE_API][V54O] FACE_GATE_SKIP hash=' + latest.snap_hash + ' reason=' + gate.reason);
    return skip;
  }

  const detect = await rt7DetectFaceOnly_(latest);
  console.log('[FACE_API][V54O] detect face_found=' + detect.face_found + ' count=' + detect.face_count + ' box=' + JSON.stringify(detect.face_box) + ' ratio=' + detect.face_ratio + ' reason=' + detect.reason + ' hash=' + latest.snap_hash);
  if (!detect.face_found || detect.face_count <= 0) {
    const noface = {
      ok:true, version:SERVER_VERSION, api_entered:true, api_path:'/api/rt7/face/match', type:'face_match', stage:'DETECT_ONLY', engine:'railway_local', gpt_used:false,
      face_gate:gate, face_found:false, face_count:0, face_box:detect.face_box, face_ratio:detect.face_ratio,
      known_face:false, matched_name:'', confidence:0, backlight_tolerant:true, pass_threshold:40,
      face_quality:detect.face_quality, face_position:detect.face_position, fail_stage:'DETECT', reason:detect.reason || 'NO_FACE',
      summary:detect.summary || '即時 Snapshot 未偵測到清楚人臉，已直接結束，未做人臉比對。', count:rt7ReadFaces_().length,
      latest_bytes:latest.bytes, snap_time:latest.snap_time, snap_source:latest.snap_source, snap_hash:latest.snap_hash, snap_age_ms:latest.snap_age_ms, snap_wait_ms:latest.snap_wait_ms, snap_forced_realtime:latest.snap_forced_realtime, snap_stale_warning:!!latest.snap_stale_warning, snap_request_ws_sent:latest.snap_request_ws_sent, snap_live_frame_fallback:!!latest.snap_live_frame_fallback, face_snapshot_url:'/api/rt7/face/last_snapshot.jpg?h='+latest.snap_hash,
      debug_text:'RAILWAY_FACE=YES SNAP=' + latest.snap_time + ' HASH=' + latest.snap_hash + ' FACE_FOUND=NO COUNT=0 REASON=' + (detect.reason || 'NO_FACE'),
      time:nowIso()
    };
    cloudState.last_face_match = noface; broadcast('face_match', noface);
    appendEvent({ type:'face_match_detect_no_face', known_face:false, confidence:0, message:noface.summary });
    return noface;
  }

  const faces = rt7ReadFaces_();
  if (!faces.length) return { ok:false, version:SERVER_VERSION, error:'NO_ENROLLED_FACE', answer:'尚未註冊人臉，請先輸入姓名後按「註冊」。', count:0 };
  try { if (rt7RefreshFaceEmbeddingCaches_(faces)) rt7SaveFaces_(faces); } catch(e) { console.warn('[FACE_API][V54O][CACHE_REFRESH_WARN] ' + String(e && e.message || e)); }
  const refs = faces.slice(0, 6);
  const match = await rt7MatchKnownFaceOnly_(latest, refs, detect);
  const confidence = Number(match.confidence || 0);
  const known = !!match.known_face && confidence >= 40;
  const result = {
    ok:true, version:SERVER_VERSION, api_entered:true, api_path:'/api/rt7/face/match', type:'face_match', stage:'RAILWAY_DETECT_THEN_MATCH', engine:'railway_local', gpt_used:false,
    face_gate:gate,
    face_found:true,
    face_count:detect.face_count,
    face_box:detect.face_box,
    face_ratio:detect.face_ratio,
    known_face:known,
    matched_name: known ? safeString(match.matched_name || refs[0]?.name || '') : '',
    confidence,
    backlight_tolerant:true,
    pass_threshold:40,
    face_quality:detect.face_quality,
    face_position:detect.face_position,
    fail_stage:known ? 'NONE' : 'MATCH',
    reason: known ? 'FACE_OK' : (match.reason || 'LOW_SIMILARITY'),
    summary: known ? ('人臉通過：' + (match.matched_name || refs[0]?.name || '已註冊') + ' / ' + confidence + '% / threshold=40%') : (match.summary || '已偵測到人臉，但與註冊名單相似度不足。'),
    count:faces.length,
    latest_bytes:latest.bytes,
    snap_time:latest.snap_time,
    snap_source:latest.snap_source,
    snap_hash:latest.snap_hash,
    snap_age_ms:latest.snap_age_ms,
    face_snapshot_url:'/api/rt7/face/last_snapshot.jpg?h='+latest.snap_hash,
    match_ms:match.match_ms || 0,
    cache_mode:match.cache_mode || 'FAST_CACHE',
    cache_hits:match.cache_hits || 0,
    cache_miss:match.cache_miss || 0,
    compared:match.compared || refs.length,
    ref_names:refs.map(f => safeString(f.name || '')),
    debug_text:'RAILWAY_FACE=YES SNAP=' + latest.snap_time + ' HASH=' + latest.snap_hash + ' FACE_FOUND=YES COUNT=' + detect.face_count + ' BOX=' + JSON.stringify(detect.face_box) + ' RATIO=' + detect.face_ratio + '% KNOWN=' + known + ' NAME=' + (match.matched_name || '') + ' CONF=' + confidence + ' QUALITY=' + detect.face_quality + ' MATCH_MS=' + (match.match_ms || 0) + ' CACHE_HITS=' + (match.cache_hits || 0) + ' POS=' + detect.face_position + ' REASON=' + (known ? 'FACE_OK' : (match.reason || 'LOW_SIMILARITY')),
    time:nowIso()
  };
  cloudState.last_face_match = result;
  appendEvent({ type:'face_match', name:result.matched_name, known_face:result.known_face, confidence:result.confidence, message:result.summary });
  console.log('[FACE_API][V54O] result stage=' + result.stage + ' hash=' + result.snap_hash + ' face_found=' + result.face_found + ' count=' + result.face_count + ' box=' + JSON.stringify(result.face_box) + ' ratio=' + result.face_ratio + '% known=' + result.known_face + ' name=' + result.matched_name + ' confidence=' + result.confidence + ' quality=' + result.face_quality + ' pos=' + result.face_position + ' reason=' + result.reason + ' fail_stage=' + result.fail_stage);
  broadcast('face_match', result);
  return result;
}

// V5.3B: server-side single-shot guard.  The phone UI can accidentally send
// multiple face requests while the stream is being stopped/restarted.  Do not
// queue multiple face_snapshot_now commands; reuse the in-flight/recent result.
let rt7FaceMatchServerBusy_ = false;
let rt7FaceMatchServerStartedMs_ = 0;
let rt7FaceMatchServerLastResult_ = null;
async function rt7FaceMatchLatest_() {
  const now = Date.now();
  if (rt7FaceMatchServerBusy_) {
    const base = cloudState.last_face_match || rt7FaceMatchServerLastResult_ || {};
    const out = Object.assign({}, base, {
      ok: true,
      version: SERVER_VERSION,
      type: 'face_match',
      face_single_shot: 'busy_reuse',
      duplicate_suppressed: true,
      answer: '人臉辨識進行中，請稍候，不重複拍照。',
      summary: base.summary || '人臉辨識進行中，已阻擋重複 face_snapshot_now。',
      time: nowIso()
    });
    console.log('[FACE_API][V54O][SINGLE_SHOT_BUSY] reuse last result, age_ms=' + (now - rt7FaceMatchServerStartedMs_));
    return out;
  }
  if (rt7FaceMatchServerLastResult_ && (now - rt7FaceMatchServerStartedMs_) < 2200) {
    const out = Object.assign({}, rt7FaceMatchServerLastResult_, {
      version: SERVER_VERSION,
      face_single_shot: 'cooldown_reuse',
      duplicate_suppressed: true,
      time: nowIso()
    });
    console.log('[FACE_API][V54O][SINGLE_SHOT_COOLDOWN] reuse recent result, age_ms=' + (now - rt7FaceMatchServerStartedMs_));
    return out;
  }
  rt7FaceMatchServerBusy_ = true;
  rt7FaceMatchServerStartedMs_ = now;
  try {
    const result = await rt7FaceMatchLatestCore_();
    rt7FaceMatchServerLastResult_ = result;
    return result;
  } finally {
    setTimeout(() => { rt7FaceMatchServerBusy_ = false; }, 900);
  }
}

app.get('/api/rt7/face/last_snapshot.jpg', (req,res) => {
  ensureDataDir();
  if (!fs.existsSync(FACE_DEBUG_SNAPSHOT_FILE)) return res.status(404).json({ok:false, version:SERVER_VERSION, error:'NO_FACE_DEBUG_SNAPSHOT'});
  res.set('Cache-Control','no-store, no-cache, must-revalidate, max-age=0');
  res.type('image/jpeg').send(fs.readFileSync(FACE_DEBUG_SNAPSHOT_FILE));
});

app.get('/api/rt7/faces', (req,res) => {
  const faces = rt7ReadFaces_().map(f => ({ id:f.id, name:f.name, time:f.time, bytes:f.bytes, device_id:f.device_id || '#1' }));
  res.json({ ok:true, version:SERVER_VERSION, count:faces.length, faces, last_face_match:cloudState.last_face_match || null });
});
app.post('/api/rt7/faces/cache/rebuild', (req,res) => {
  const faces = rt7ReadFaces_();
  const changed = rt7RefreshFaceEmbeddingCaches_(faces);
  if (changed) rt7SaveFaces_(faces);
  res.json({ ok:true, version:SERVER_VERSION, changed, count:faces.length, cached:faces.filter(f => f.embedding_cache && f.embedding_cache.ok).length, engine:'railway_local_fast_cache' });
});
app.get('/api/rt7/phase6c3_plugin/faces', (req,res) => {
  const faces = rt7ReadFaces_().map(f => ({ id:f.id, name:f.name, time:f.time, bytes:f.bytes, device_id:f.device_id || '#1' }));
  res.json({ ok:true, version:SERVER_VERSION, count:faces.length, faces });
});
app.post('/api/rt7/face/enroll', rt7EnrollHandler_);
function rt7EnrollHandler_(req,res){
  const name = safeString(req.body?.name || req.query.name || req.query.face_id || req.query.id || '').trim() || '未命名';
  const latest = rt7LatestJpegB64_();
  if (!latest) return res.status(200).json({ ok:false, version:SERVER_VERSION, error:'NO_LATEST_SNAPSHOT', answer:'尚無最新照片，請先開始影像或讓 ESP32 上傳 snapshot。' });
  const faces = rt7ReadFaces_();
  const id = 'face_' + Date.now();
  const face = { id, name, image_b64:latest.b64, bytes:latest.bytes, time:nowIso(), device_id:safeString(req.body?.device_id || req.query.device_id || '#1') };
  try {
    const emb = rt7ExtractFaceEmbedding_(latest.b64, null);
    if (emb && emb.ok) { face.embedding_cache = rt7FaceEmbeddingToCache_(emb); face.embedding_cache_key = rt7FaceEmbeddingCacheKey_(face); }
  } catch(e) { console.warn('[FACE_API][V54O][ENROLL_CACHE_WARN] ' + String(e && e.message || e)); }
  faces.unshift(face);
  rt7SaveFaces_(faces.slice(0, 20));
  const ev = appendEvent({ type:'face_enroll', id, name, bytes:latest.bytes, message:'enrolled face '+name });
  broadcast('face_enroll', { id, name, bytes:latest.bytes, time:face.time });
  res.json({ ok:true, version:SERVER_VERSION, enrolled:{ id, name, bytes:latest.bytes, time:face.time }, count:faces.length, event:ev, answer:'已註冊：' + name });
}
app.get('/api/rt7/phase6c3_plugin/face/enroll_now', rt7EnrollHandler_);
app.post('/api/rt7/phase6c3_plugin/face/enroll_now', rt7EnrollHandler_);
app.post('/api/rt7/phase6c3_plugin/face/enroll', rt7EnrollHandler_);
app.post('/api/rt7/face/match', async (req,res) => {
  try { res.json(await rt7FaceMatchLatest_()); }
  catch(e) { res.status(200).json({ ok:false, version:SERVER_VERSION, error:String(e.message || e), answer:'人臉辨識失敗，請確認 Railway 已設定 OPENAI_API_KEY。' }); }
});
app.get('/api/rt7/face/match', async (req,res) => {
  try { res.json(await rt7FaceMatchLatest_()); }
  catch(e) { res.status(200).json({ ok:false, version:SERVER_VERSION, error:String(e.message || e), answer:'人臉辨識失敗，請確認 Railway 已設定 OPENAI_API_KEY。' }); }
});
app.get('/api/rt7/phase6c3_plugin/face/match', async (req,res) => {
  try { res.json(await rt7FaceMatchLatest_()); }
  catch(e) { res.status(200).json({ ok:false, version:SERVER_VERSION, error:String(e.message || e), answer:'人臉辨識失敗，請確認 Railway 已設定 OPENAI_API_KEY。' }); }
});
app.post('/api/face/recognize', async (req,res) => {
  try { res.json(await rt7FaceMatchLatest_()); }
  catch(e) { res.status(200).json({ ok:false, version:SERVER_VERSION, error:String(e.message || e), answer:'人臉辨識失敗，請確認 Railway 已設定 OPENAI_API_KEY。' }); }
});
app.get('/api/face/recognize', async (req,res) => {
  try { res.json(await rt7FaceMatchLatest_()); }
  catch(e) { res.status(200).json({ ok:false, version:SERVER_VERSION, error:String(e.message || e), answer:'人臉辨識失敗，請確認 Railway 已設定 OPENAI_API_KEY。' }); }
});

app.get('/api/rt7/face_gate/state', (req,res) => {
  res.json({ ok:true, version:SERVER_VERSION, enabled:!!cloudState.face_gate_enabled, auto_enabled:!!cloudState.face_gate_auto_enabled, auto_busy:!!cloudState.face_gate_auto_busy, auto_cooldown_ms:cloudState.face_gate_auto_cooldown_ms, last_face_gate:cloudState.last_face_gate || null, last_face_match:cloudState.last_face_match || null });
});
app.get('/api/rt7/face_gate/serial_result', (req,res) => {
  const m = cloudState.last_face_match || {};
  const g = cloudState.last_face_gate || {};
  res.json({
    ok:true,
    version:SERVER_VERSION,
    time:nowIso(),
    has_result:!!(cloudState.last_face_match),
    auto_face_gate:!!m.auto_face_gate,
    trigger_source:safeString(m.trigger_source || ''),
    known_face:!!m.known_face,
    matched_name:safeString(m.matched_name || ''),
    confidence:Number(m.confidence || 0),
    face_found:!!m.face_found,
    face_count:Number(m.face_count || 0),
    face_box:m.face_box || {x:0,y:0,w:0,h:0},
    face_ratio:Number(m.face_ratio || 0),
    face_quality:safeString(m.face_quality || ''),
    face_position:safeString(m.face_position || ''),
    fail_stage:safeString(m.fail_stage || ''),
    reason:safeString(m.reason || ''),
    summary:safeString(m.summary || m.answer || '').slice(0,220),
    snap_hash:safeString(m.snap_hash || ''),
    snap_age_ms:Number(m.snap_age_ms || 0),
    latest_bytes:Number(m.latest_bytes || 0),
    gate_pass:!!g.pass,
    gate_reason:safeString(g.reason || ''),
    gate_score:Number(g.candidate || g.score || 0)
  });
});
app.post('/api/rt7/face_gate/auto', (req,res) => {
  const mode = safeString(req.body?.mode || req.query.mode || '');
  if (/^(on|1|true|enable)$/i.test(mode)) cloudState.face_gate_auto_enabled = true;
  else if (/^(off|0|false|disable)$/i.test(mode)) cloudState.face_gate_auto_enabled = false;
  else cloudState.face_gate_auto_enabled = !cloudState.face_gate_auto_enabled;
  res.json({ ok:true, version:SERVER_VERSION, auto_enabled:!!cloudState.face_gate_auto_enabled, auto_busy:!!cloudState.face_gate_auto_busy });
});
app.post('/api/rt7/face_gate/toggle', (req,res) => {
  const mode = safeString(req.body?.mode || req.query.mode || '');
  if (/^(on|1|true|enable)$/i.test(mode)) cloudState.face_gate_enabled = true;
  else if (/^(off|0|false|disable)$/i.test(mode)) cloudState.face_gate_enabled = false;
  else cloudState.face_gate_enabled = !cloudState.face_gate_enabled;
  console.log('[RT7_FACE_GATE][TOGGLE][V52D] set enabled=' + cloudState.face_gate_enabled);
  res.json({ ok:true, version:SERVER_VERSION, enabled:!!cloudState.face_gate_enabled, auto_enabled:!!cloudState.face_gate_auto_enabled, last_face_gate:cloudState.last_face_gate || null });
});
app.get('/api/rt7/face/state', (req,res) => {
  res.json({ ok:true, version:SERVER_VERSION, api:'/api/rt7/face/match', alias:'/api/face/recognize', faces:rt7ReadFaces_().length, last_face_match:cloudState.last_face_match || null, latest_snapshot:getSnapshotMeta_(), face_gate_enabled:!!cloudState.face_gate_enabled, last_face_gate:cloudState.last_face_gate || null });
});

app.get('/api/rt7/faces/reset', (req,res) => {
  rt7SaveFaces_([]);
  const ev = appendEvent({ type:'faces_reset', message:'face list reset' });
  broadcast('faces_reset', ev);
  res.json({ ok:true, version:SERVER_VERSION, count:0, event:ev });
});
app.get('/api/rt7/phase6c3_plugin/faces/reset', (req,res) => {
  rt7SaveFaces_([]);
  res.json({ ok:true, version:SERVER_VERSION, count:0 });
});

// ---------- Original RT7 mobile-style cloud doorbell UI ----------
app.get('/rt7_cloud_original_ui_doorbell', (req, res) => {
  const q = req.query || {};
  const mode = safeString(q.mode || 'idle').toLowerCase();
  const ip = safeString(q.ip || '192.168.0.179').replace(/[^0-9.]/g, '') || '192.168.0.179';
  const aiOn = q.face === '1' || cloudState.face_gate_auto_enabled === true;
  const doorLast = doorbellState.last || null;
  const doorText = doorLast && doorLast.time ? ('最後：' + new Date(doorLast.time).toLocaleTimeString('zh-TW')) : '等待事件';
  let modeLabel = mode === 'lan' ? 'LAN' : (mode === 'cloud' ? 'CLOUD' : (mode === 'auto' ? 'AUTO' : 'AUTO'));
  let answer = mode === 'idle' ? '雲端門鈴待機中' : '自動判斷影像來源中';
  let hint = mode === 'idle' ? '等待影像串流' : '自動判斷：內網直連 / Railway 雲端';
  res.type('html').send(`<!doctype html><html lang="zh-Hant"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<title>RT7 Cloud Original UI V5.4L</title>
<style>
:root{--dark:#0b252b;--dark2:#0d2c32;--red:#ef2b24;--blue:#17a8e5;--green:#22a951;--text:#17262a;--line:#e5e7eb;--orange:#9a3b18}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent} html,body{margin:0;padding:0;background:#fff;color:var(--text);font-family:system-ui,-apple-system,"Noto Sans TC","Microsoft JhengHei",Arial,sans-serif} body{max-width:520px;margin:0 auto;min-height:100vh;padding-bottom:28px}
a,button,input,select{pointer-events:auto!important;touch-action:manipulation!important}.noTouch,.video img,.emptyVideo,.badge{pointer-events:none!important}
.top{height:66px;background:linear-gradient(90deg,var(--dark),var(--dark2));color:#fff;display:flex;align-items:center;justify-content:space-between;padding:0 16px;font-weight:900}.hamb{font-size:34px}.title{text-align:center;line-height:1.15;font-size:17px;letter-spacing:.4px}.spacer{width:34px}
.deviceBar{padding:8px 12px;background:#fff;border-bottom:1px solid var(--line)}.deviceText{height:42px;border:1px solid #334155;border-radius:8px;font-weight:900;padding:0 10px;background:#fff;font-size:17px;display:flex;align-items:center;justify-content:space-between;color:#111827}.deviceText select{border:0;background:#fff;font:inherit;font-weight:900;width:100%;outline:0}
.video{position:relative;background:#000;aspect-ratio:4/3;overflow:hidden}.video img{width:100%;height:100%;object-fit:cover;background:#000;display:block;border:0}.emptyVideo{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;text-align:center;color:#cbd5e1;font-weight:900;font-size:18px;line-height:1.45;padding:12px}.badge{position:absolute;top:12px;border-radius:7px;padding:7px 12px;color:white;font-weight:900;box-shadow:0 2px 8px rgba(0,0,0,.22)}.idle{left:14px;background:#71839d}.idle.aiOn{background:#16a34a}.live{right:14px;background:var(--red)}
.videoBtns{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:6px;background:#fff;padding:6px 8px;border-bottom:1px solid var(--line);align-items:center}.vbtn{display:flex;align-items:center;justify-content:center;border:0;border-radius:8px;color:#fff;font-weight:900;padding:8px 3px;font-size:13px;line-height:1;min-width:0;width:100%;height:38px;text-decoration:none;white-space:nowrap;overflow:hidden}.vblue{background:var(--blue)}.vred{background:var(--red)}.vdark{background:#102a31}.vorange{background:#f59e0b}
.statusLine{min-height:46px;display:grid;grid-template-columns:1fr 1fr;gap:8px;border-bottom:1px solid var(--line);align-items:center;padding:8px 12px;background:#fff;font-size:15px;font-weight:800}.faceSnapBox{display:none;border-bottom:1px solid var(--line);padding:8px 12px;background:#fff}.faceSnapTitle{font-weight:900;color:#0f172a;margin-bottom:6px}.faceSnapBox img{width:128px;max-width:40%;border:2px solid #cbd5e1;border-radius:8px;background:#000;vertical-align:top}.faceSnapMeta{display:inline-block;vertical-align:top;margin-left:10px;font-size:12px;font-weight:900;color:#5b1f14;line-height:1.5;max-width:55%;word-break:break-all}.dot{display:inline-block;width:11px;height:11px;border-radius:50%;background:var(--green);margin-right:8px}.answer{color:#5b1f14}.door{color:#8a2f15;text-align:right}.door.bellNow{color:#9a3412;font-weight:900}.doorAlert{display:none!important}
.micZone{text-align:center;padding:18px 0 8px}.bigMic{width:128px;height:128px;border-radius:50%;border:3px solid #cbd5e1;background:#eef2f7;display:inline-flex;align-items:center;justify-content:center;font-size:72px;box-shadow:0 4px 18px rgba(20,40,60,.08);text-decoration:none;color:#24333a}
.actions{display:flex;justify-content:center;gap:10px;padding:10px 8px 4px}.act{width:66px;text-align:center;font-size:12px;font-weight:900;color:#24333a}.circle{width:58px;height:58px;border:3px solid var(--red);border-radius:50%;background:#fff;display:flex;align-items:center;justify-content:center;font-size:28px;margin:0 auto 4px;box-shadow:0 2px 10px rgba(0,0,0,.1);text-decoration:none;color:#24333a}.circle.aiActive{border-color:#22c55e;background:#ecfdf5}.circle.talking{border-color:#ef4444;background:#fff1f2;box-shadow:0 0 0 4px rgba(239,68,68,.18)}.reg{display:flex;align-items:center;gap:10px;padding:8px 20px}.reg label{font-size:14px;font-weight:900}.reg input{flex:1;height:36px;border:1px solid #cbd5e1;border-radius:7px;padding:0 10px;font-size:16px}.small{font-size:12px;color:#64748b}.debug{display:none!important}
@media(max-height:740px){.top{height:56px}.videoBtns{gap:4px;padding:5px 6px}.vbtn{height:34px;font-size:12px;padding:7px 2px}.title{font-size:15px}.video{aspect-ratio:16/9}.bigMic{width:104px;height:104px;font-size:58px}.circle{width:50px;height:50px;font-size:24px}.act{font-size:11px}.statusLine{font-size:13px;min-height:38px}.reg{padding-top:4px}}
</style></head><body>
<header class="top"><div class="hamb">☰</div><div class="title">RT7 PHASE10<br>AI MODE ROUTER</div><div class="spacer"></div></header>
<div class="deviceBar"><div class="deviceText"><select id="deviceSel"><option value="${ip}">#1 / RT7 ESP32-S3-CAM / ${ip}</option></select></div></div>
<section class="video"><div id="emptyVideo" class="emptyVideo">${hint}<br><span class="small">網內使用 ESP32 直連；網外使用 Railway 雲端</span></div><img id="stream" alt=""><div id="aiBadge" class="badge idle ${aiOn?'aiOn':''}">${aiOn?'FACE_ENABLE':'IDLE'}</div><div id="streamModeBadge" class="badge live">${modeLabel}</div></section>
<section class="videoBtns"><button id="btnAiOn" class="vbtn vblue" type="button">啟用人臉</button><button id="btnAiOff" class="vbtn vred" type="button">關閉人臉</button><button id="btnAudio" class="vbtn vorange" type="button">啟用提示音</button><button id="btnStart" class="vbtn vdark" type="button">開始影像</button><button id="btnStop" class="vbtn vdark" type="button">停止影像</button></section>
<section class="statusLine"><div class="answer"><span class="dot"></span>回答：<span id="answerText">${answer}</span></div><div class="door">門鈴：<span id="doorText">${doorText}</span></div><div id="doorAlert" class="doorAlert">🔔 有人按門鈴</div></section>

<section class="micZone"><button id="btnVoice" class="bigMic" type="button">🎙️</button></section>
<section class="actions"><div class="act"><button id="btnOpenDoor" class="circle" type="button">🚪</button>開門</div><div class="act"><button id="btnFaceList" class="circle" type="button">👥</button>名單</div><div class="act"><button id="btnEndTalk" class="circle" type="button">◼</button>對講</div><div class="act"><button id="btnFaceEnroll" class="circle" type="button">＋</button>註冊</div><div class="act"><button id="btnAiVoice" class="circle" type="button">🎙️</button>AI語音助理</div></section>
<div class="reg"><label>註冊名稱</label><input id="regName" value="gwansyan"></div>
<script>
(function(){
  var ip=${JSON.stringify(ip)}; var mode=${JSON.stringify(mode)}; var ai=false; try{ ai=(localStorage.getItem('RT7_FACE_MODE')==='1'); }catch(e){ ai=${aiOn?'true':'false'}; } var img=document.getElementById('stream'); var empty=document.getElementById('emptyVideo'); var badge=document.getElementById('streamModeBadge'); var answer=document.getElementById('answerText'); var debug=null; var audioCtx=null; var audioOK=false; var audioTried=false;
  // V5.4W: FACE_GATE is default OFF and persistent. UI state alone must never re-enable it.
  setTimeout(function(){ setAiUi(ai); rt7FaceGateEspEnable(ai, true); }, 600);
  function setAnswer(t){ if(answer) answer.textContent=t; }
  function setDoorText(t, bell){ var d=document.getElementById('doorText'); var box=d?d.closest('.door'):null; if(d)d.textContent=t; if(box){ if(bell) box.classList.add('bellNow'); else box.classList.remove('bellNow'); } }
  function showDoorbellInline(){ setDoorText('⚠️ 有人按門鈴', true); setAnswer('收到門鈴提示音'); playDingdong(); setTimeout(function(){ setDoorText('最後：'+new Date().toLocaleTimeString('zh-TW'), false); }, 8000); }
  function setDebug(t){ /* V5.0E: hidden debug; no UI repaint */ }
  function tone(freq, delay, dur){ if(!audioCtx) return; try{ setTimeout(function(){ var o=audioCtx.createOscillator(); var g=audioCtx.createGain(); o.frequency.value=freq; g.gain.value=0.22; o.connect(g); g.connect(audioCtx.destination); o.start(); setTimeout(function(){try{o.stop()}catch(e){}}, dur); }, delay); }catch(e){} }
  function playDingdong(){ if(!audioOK) return; tone(880,0,180); tone(660,260,220); }
  async function enableDoorbellAudio(){ try{ audioCtx = audioCtx || new (window.AudioContext||window.webkitAudioContext)(); await audioCtx.resume(); audioOK=true; audioTried=true; setAnswer('門鈴提示音已啟用'); setDebug('audio enabled'); playDingdong(); return true; }catch(e){ setAnswer('提示音啟用失敗：'+(e.message||e)); setDebug('audio failed'); return false; } }
  function tryUnlockAudioSilently(){ if(audioOK || audioTried) return; audioTried=true; try{ audioCtx = audioCtx || new (window.AudioContext||window.webkitAudioContext)(); audioCtx.resume().then(function(){ audioOK=true; setDebug('audio auto-unlocked by touch'); }).catch(function(){ audioTried=false; }); }catch(e){ audioTried=false; } }
  document.addEventListener('touchend', tryUnlockAudioSilently, {once:true, passive:true});
  document.addEventListener('click', tryUnlockAudioSilently, {once:true, passive:true});
  var videoWanted=false; var currentStreamMode='IDLE'; var lanReconnectTimer=null; var lanRetryCount=0; var lanProbeDone=false;
  function clearLanReconnect(){ if(lanReconnectTimer){ clearTimeout(lanReconnectTimer); lanReconnectTimer=null; } }
  function stopVideo(){ videoWanted=false; currentStreamMode='IDLE'; try{localStorage.setItem('RT7_V50_WANTED_VIDEO','0');localStorage.setItem('RT7_V50_STREAM_MODE','IDLE');}catch(e){} clearLanReconnect(); if(img){ img.onerror=null; img.onload=null; try{ img.src='about:blank'; }catch(e){} img.removeAttribute('src'); } if(badge) badge.textContent='AUTO'; if(empty) empty.innerHTML='等待影像串流<br><span class="small">自動判斷：內網直連 / Railway 雲端</span>'; setAnswer('雲端門鈴待機中'); setDebug('stop video'); }
  function cloud(){ videoWanted=true; currentStreamMode='CLOUD'; try{localStorage.setItem('RT7_V50_WANTED_VIDEO','1');localStorage.setItem('RT7_V50_STREAM_MODE','CLOUD');}catch(e){} clearLanReconnect(); if(badge) badge.textContent='CLOUD'; if(empty) empty.innerHTML='Railway 雲端遠端影像<br><span class="small">外網或內網偵測失敗，自動切換</span>'; if(img){ img.onerror=function(){ if(!videoWanted || currentStreamMode!=='CLOUD') return; setAnswer('雲端影像暫停，5 秒後重連'); clearLanReconnect(); lanReconnectTimer=setTimeout(function(){ if(videoWanted && currentStreamMode==='CLOUD'){ img.src='/api/rt7/camera/stream.mjpg?_cloud_re='+Date.now(); } },5000); }; img.onload=function(){ setDebug('cloud mjpeg loaded'); }; img.src='/api/rt7/camera/stream.mjpg?_cloud='+Date.now(); } setAnswer('雲端遠端影像模式'); setDebug('cloud stream'); }
  function lan(){ videoWanted=true; currentStreamMode='LAN'; try{localStorage.setItem('RT7_V50_WANTED_VIDEO','1');localStorage.setItem('RT7_V50_STREAM_MODE','LAN');}catch(e){} clearLanReconnect(); lanRetryCount=0; if(badge) badge.textContent='LAN'; if(empty) empty.innerHTML='內網直連 ESP32 流暢影像<br><span class="small">'+ip+'</span>'; if(img){ img.style.backgroundImage='url("/api/rt7/camera/latest.jpg?_hold='+Date.now()+'")'; img.style.backgroundSize='cover'; img.style.backgroundPosition='center'; img.onerror=function(){ if(!videoWanted || currentStreamMode!=='LAN') return; lanRetryCount++; setAnswer('LAN 串流暫停，5 秒後重連（保留畫面，不清空黑屏）'); setDebug('lan onerror retry='+lanRetryCount); clearLanReconnect(); lanReconnectTimer=setTimeout(function(){ if(!videoWanted || currentStreamMode!=='LAN') return; // Do NOT clear img.src here. Clearing src causes Android Chrome black screen. Replace source directly.
        var next='http://'+ip+'/api/camera/stream?_lan_re='+Date.now(); try{ img.src=next; }catch(e){} },5000); }; img.onload=function(){ lanRetryCount=0; setDebug('lan mjpeg loaded'); }; var first='http://'+ip+'/api/camera/stream?_lan='+Date.now(); try{ img.src=first; }catch(e){} } setAnswer('內網直連影像模式'); setDebug('lan stream '+ip); }
  function startAuto(){ try{localStorage.setItem('RT7_V50_WANTED_VIDEO','1');localStorage.setItem('RT7_V50_STREAM_MODE','AUTO');}catch(e){} if(videoWanted && (currentStreamMode==='LAN' || currentStreamMode==='CLOUD')){ setAnswer(currentStreamMode==='LAN'?'內網直連影像模式':'雲端遠端影像模式'); return; } videoWanted=true; currentStreamMode='AUTO'; clearLanReconnect(); setAnswer('自動判斷影像來源中'); if(badge) badge.textContent='AUTO'; if(empty) empty.innerHTML='自動判斷中：先用單張 snapshot 測內網，成功才開啟 LAN 串流'; var probe=new Image(); var done=false; var t=setTimeout(function(){ if(done||!videoWanted)return; done=true; try{probe.src='about:blank'}catch(e){} cloud(); },1800); probe.onload=function(){ if(done||!videoWanted)return; done=true; clearTimeout(t); try{probe.src='about:blank'}catch(e){} lan(); }; probe.onerror=function(){ if(done||!videoWanted)return; done=true; clearTimeout(t); try{probe.src='about:blank'}catch(e){} cloud(); }; probe.src='http://'+ip+'/api/camera/snapshot?_probe_once='+Date.now(); }
  async function j(url,opt){ var r=await fetch(url+(url.indexOf('?')>=0?'&':'?')+'_='+Date.now(), Object.assign({cache:'no-store'}, opt||{})); var tx=await r.text(); try{return JSON.parse(tx)}catch(e){return{ok:r.ok,status:r.status,raw:tx}} }
  function rt7EspImgBeacon(path){
    var url='http://'+ip+path+(path.indexOf('?')>=0?'&':'?')+'_='+Date.now();
    try{ var im=new Image(); im.onload=function(){}; im.onerror=function(){}; im.src=url; }catch(e){}
    try{ fetch(url,{mode:'no-cors',cache:'no-store',keepalive:true}).catch(function(){}); }catch(e){}
  }
  function rt7EspFast8081(path){
    var url='http://'+ip+':8081'+path+(path.indexOf('?')>=0?'&':'?')+'_='+Date.now();
    try{ var im=new Image(); im.onload=function(){}; im.onerror=function(){}; im.src=url; }catch(e){}
    try{ fetch(url,{mode:'no-cors',cache:'no-store',keepalive:true}).catch(function(){}); }catch(e){}
  }
  function rt7FaceGateEspEnable(on, silent){
    try{ localStorage.setItem('RT7_FACE_MODE', on ? '1' : '0'); }catch(e){}
    if(on){
      // V5.4W: enable only when the user explicitly presses 啟用人臉.
      rt7EspImgBeacon('/api/motion/config?threshold=1500&cooldown=8000&warmup=1000&face_gate=1&face_threshold=2100&face_min_jpeg=3800&face_center_min_jpeg=3800&face_center_min_motion=1800&face_center_min_candidate=2100&step=31');
      rt7EspImgBeacon('/api/motion/enable');
      try{ rt7Json('/api/rt7/face_gate/auto?mode=on',{method:'POST'}); }catch(_){ }
      try{ rt7Json('/api/rt7/face_gate/toggle?mode=on',{method:'POST'}); }catch(_){ }
    } else {
      // V5.4W: hard OFF. Send disable twice/order-safe so stream restart cannot leave FACE_GATE running.
      rt7EspImgBeacon('/api/motion/disable');
      rt7EspImgBeacon('/api/motion/config?enabled=0&face_gate=0');
      setTimeout(function(){ rt7EspImgBeacon('/api/motion/disable'); rt7EspImgBeacon('/api/motion/config?enabled=0&face_gate=0'); }, 350);
      try{ rt7Json('/api/rt7/face_gate/auto?mode=off',{method:'POST'}); }catch(_){ }
      try{ rt7Json('/api/rt7/face_gate/toggle?mode=off',{method:'POST'}); }catch(_){ }
      rt7AutoFaceLastKey='';
    }
  }
  function bind(id,fn){ var el=document.getElementById(id); if(el) el.addEventListener('click', function(ev){ ev.preventDefault(); ev.stopPropagation(); fn(); }, false); }
  bind('btnStart', startAuto); bind('btnStop', stopVideo); bind('btnAudio', enableDoorbellAudio);
  bind('btnAiOn', async function(){ setAiUi(true,'人臉辨識已啟用：靠近鏡頭會自動辨識'); rt7FaceGateEspEnable(true); setDebug('face mode on'); });
  bind('btnAiOff', async function(){ setAiUi(false,'人臉辨識已關閉'); rt7FaceGateEspEnable(false); setDebug('face mode off'); });
  bind('btnOpenDoor', async function(){
    setAnswer('開門命令送出中...');
    if(currentStreamMode==='LAN'){
      // HTTPS page cannot reliably fetch() HTTP ESP32 due mixed-content rules.
      // Use an Image beacon; browsers allow LAN image GET and ESP32 8081 handles it while MJPEG occupies port 80.
      try{
        var beacon=new Image();
        beacon.onload=function(){ setDebug('door fast 8081 beacon loaded'); };
        beacon.onerror=function(){ setDebug('door fast 8081 beacon sent/error ok'); };
        beacon.src='http://'+ip+':8081/api/door/open_fast?_door='+Date.now();
        setAnswer('內網開門');
        return;
      }catch(e){ setDebug('fast 8081 failed '+e.message); }
    }
    try{ var r=await j('/api/rt7/door/open?device_id='+encodeURIComponent('#1')); setAnswer('外網開門'); setDebug('door open cloud '+JSON.stringify(r).slice(0,160)); }catch(e){ setAnswer('開門失敗：'+e.message); }
  });
  function speakAnswer(txt){ if(window.speechSynthesis && (txt||'').length){ try{ speechSynthesis.cancel(); var u=new SpeechSynthesisUtterance(txt); u.lang='zh-TW'; speechSynthesis.speak(u); }catch(e){} } }
  function setAiUi(on, msg){
    // V5.4L: this top button controls FACE_GATE auto recognition only, not AI voice.
    ai=!!on; try{localStorage.setItem('RT7_FACE_MODE', ai?'1':'0');}catch(e){}
    var b=document.getElementById('aiBadge');
    if(b){ b.textContent=ai?'FACE_ENABLE':'IDLE'; if(ai)b.classList.add('aiOn'); else b.classList.remove('aiOn'); }
    if(msg) setAnswer(msg);
  }
  async function routeVoiceQuestion(text){
    text=(text||'').trim();
    if(!text){ setAnswer('沒有收到語音內容，請再按一次 AI語音助理後說話'); setDebug('voice empty'); return; }
    setAnswer('你說：'+text+'，AI 分析中...');
    setDebug('voice question: '+text);
    try{
      var r=await j('/api/rt7/phase9j/voice_vision',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({text:text,mode:'auto'})});
      var ans=r.answer||r.error||'AI 無回應';
      setAnswer(ans);
      speakAnswer(ans);
      setDebug('voice_vision ok');
    }catch(e){
      setAnswer('AI語音助理失敗：'+e.message);
      setDebug('voice_vision failed');
    }finally{
      // V5.4W: AI voice must not change FACE_GATE mode.
    }
  }
  function startVoiceAsk(){ setAnswer('請開始說話'); var SR=window.SpeechRecognition||window.webkitSpeechRecognition; if(!SR){ var t=prompt('請輸入要問 AI語音助理的內容：','')||''; routeVoiceQuestion(t); return; } try{ var rec=new SR(); rec.lang='zh-TW'; rec.continuous=false; rec.interimResults=false; rec.maxAlternatives=1; setAnswer('請開始說話'); setDebug('speech recognition start'); rec.onresult=function(ev){ var text=''; try{text=ev.results[0][0].transcript||'';}catch(e){} routeVoiceQuestion(text); }; rec.onerror=function(ev){ setAnswer('語音辨識失敗：'+(ev.error||'unknown')+'。請再按一次 AI語音助理。'); setDebug('speech error '+(ev.error||'')); }; rec.onend=function(){ setDebug('speech recognition end'); }; rec.start(); }catch(e){ var t2=prompt('語音辨識無法啟動，請輸入問題：','')||''; routeVoiceQuestion(t2); } }
  bind('btnAiVoice', startVoiceAsk); // btnVoice 是中央對講按鍵，不再啟動 AI 語音助理

  // V5.0K: 雙向 PTT WebSocket 對講。
  // 按住中央「對講」：手機 Mic -> ESP32 Speaker；放開：ESP32 Mic -> 手機 Speaker；按下方「◼ 對講」才結束。
  var rt7WsIc=null, rt7WsIcOn=false, rt7WsTxActive=false, rt7WsListenActive=false;
  var rt7WsMicStream=null, rt7WsMicCtx=null, rt7WsMicSource=null, rt7WsMicProc=null;
  var rt7WsTxBytes=[], rt7WsSent=0, rt7WsBeginMs=0, rt7WsListenTimer=null;
  var rt7RxAudioCtx=null, rt7RxPlayAt=0, rt7RxPackets=0, rt7RxBytes=0, rt7RxLastMs=0, rt7RxJitterMaxMs=0;
  async function rt7Json(url,opt){ var r=await fetch(url+(url.indexOf('?')>=0?'&':'?')+'_='+Date.now(),Object.assign({cache:'no-store'},opt||{})); var t=await r.text(); try{return JSON.parse(t)}catch(e){return{ok:r.ok,raw:t}} }
  async function rt7FaceEnroll(){
    try{
      var name=(document.getElementById('regName')&&document.getElementById('regName').value||'').trim()||'未命名';
      setAnswer('人臉註冊中：'+name);
      var j=await rt7Json('/api/rt7/face/enroll',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name:name,device_id:'#1'})});
      setAnswer(j.ok ? ('已註冊人臉：'+(j.enrolled&&j.enrolled.name||name)) : ('註冊失敗：'+(j.answer||j.error||'NO_SNAPSHOT')));
    }catch(e){ setAnswer('註冊失敗：'+(e.message||e)); }
  }
  async function rt7FaceList(){
    try{
      var j=await rt7Json('/api/rt7/faces');
      if(!j.ok){ setAnswer('名單讀取失敗'); return; }
      var names=(j.faces||[]).map(function(f){return f.name;}).filter(Boolean);
      setAnswer(names.length ? ('已註冊 '+names.length+' 人：'+names.join('、')) : '尚未註冊人臉');
    }catch(e){ setAnswer('名單讀取失敗：'+(e.message||e)); }
  }

  async function rt7FaceGateToggle(){
    try{
      var btn=document.getElementById('btnFaceGate');
      setAnswer('切換 FACE_GATE 測試中...');
      var j=await rt7Json('/api/rt7/face_gate/toggle',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});
      if(btn) btn.textContent = j.enabled ? 'FACE_GATE ON' : 'FACE_GATE OFF';
      if(btn) btn.className = j.enabled ? 'vbtn vorange' : 'vbtn vgreen';
      setAnswer('FACE_GATE 測試模式：'+(j.enabled?'ON（先 Gate，通過才做 Railway 比對）':'OFF（直接 AI 辨識，等同 V5.0S）'));
    }catch(e){ setAnswer('FACE_GATE 切換失敗：'+(e.message||e)); }
  }

  function rt7ShowFaceSnapshot(j){
    // V5.4M: UI minimal mode - hide Snapshot debug block on phone page.
    return;

    try{
      var box=document.getElementById('faceSnapBox'); var im=document.getElementById('faceSnapImg'); var meta=document.getElementById('faceSnapMeta');
      if(!box||!im||!j) return;
      var h=j.snap_hash||j.snapshot_hash||''; var u=j.face_snapshot_url||('/api/rt7/face/last_snapshot.jpg?_='+(Date.now())+'&h='+encodeURIComponent(h));
      im.src=u; box.style.display='block';
      if(meta) meta.innerHTML='SNAP='+(j.snap_time||'')+'<br>BYTES='+(j.latest_bytes||j.bytes||'')+'<br>HASH='+(h||'')+'<br>AGE='+(j.snap_age_ms!=null?j.snap_age_ms+'ms':'')+'<br>SOURCE='+(j.snap_source||'none')+'<br>FORCE='+(j.snap_forced_realtime?'YES':'NO')+'<br>WAIT='+(j.snap_wait_ms!=null?j.snap_wait_ms+'ms':'')+'<br>WS_SENT='+(j.snap_request_ws_sent!=null?j.snap_request_ws_sent:'')+'<br>LIVE_FB='+(j.snap_live_frame_fallback?'YES':'NO')+'<br>CACHE='+(j.cache_mode||'')+'<br>MATCH_MS='+(j.match_ms!=null?j.match_ms+'ms':'');
    }catch(_){ }
  }

  // V5.4D: FACE_GATE auto recognition result polling.
  // ESP32 FACE_GATE PASS now uploads snapshot and Railway auto-matches it.
  // The phone UI must poll the last_face_match result because no manual button callback runs.
  var rt7AutoFaceLastKey='';
  var rt7AutoFacePollBusy=false;
  function rt7FaceResultText_(j, autoLabel){
    if(!j) return '';
    var box=j.face_box?((j.face_box.w||0)+'x'+(j.face_box.h||0)):'0x0';
    var prefix=autoLabel?'FACE_GATE 自動辨識：':'';
    if(j.ok && j.known_face){
      return prefix+'人臉通過：'+(j.matched_name||'已註冊')+' / '+(j.confidence||0)+'%｜FACE_FOUND='+(j.face_found?'YES':'NO')+'｜COUNT='+(j.face_count||0)+'｜BOX='+box+'｜RATIO='+(j.face_ratio||0)+'%｜ENGINE='+(j.engine||'railway_local');
    }
    if(j.ok){
      return prefix+'人臉未通過：'+(j.reason||'UNKNOWN')+'｜FACE_FOUND='+(j.face_found?'YES':'NO')+'｜COUNT='+(j.face_count||0)+'｜BOX='+box+'｜RATIO='+(j.face_ratio||0)+'%｜FAIL='+(j.fail_stage||'-');
    }
    return prefix+'人臉辨識錯誤：'+(j.error||j.reason||'UNKNOWN');
  }
  async function rt7PollAutoFaceResult_(){
    if(rt7AutoFacePollBusy) return;
    if(!ai) return;
    if(rt7FaceBusy || rt7FaceMatchBusy || rt7FaceRestoreBusy) return;
    rt7AutoFacePollBusy=true;
    try{
      var s=await rt7Json('/api/rt7/face_gate/state?_='+Date.now());
      var m=s && s.last_face_match;
      if(!m) return;
      var isAuto = !!(m.auto_face_gate || m.trigger_source==='esp32_face_gate' || (m.snap_source||'').indexOf('face_gate_auto')>=0);
      if(!isAuto) return;
      // V5.4E: PASS-only UI update.
      // ESP32 SKIP does not POST to Railway, so the server may still hold the previous PASS result.
      // Do not redisplay stale PASS results while current FACE_GATE samples are SKIP.
      var t = Date.parse(m.time || m.snap_time || '');
      var ageMs = isFinite(t) ? (Date.now() - t) : 999999;
      if(ageMs < 0) ageMs = 0;
      if(ageMs > 9000) {
        if(rt7AutoFaceLastKey) {
          rt7AutoFaceLastKey='';
          setDebug('auto face waiting for new PASS');
        }
        return;
      }
      var key=[m.snap_hash||'',m.snap_time||m.time||'',m.confidence||0,m.reason||'',m.known_face?'1':'0'].join('|');
      if(!key || key===rt7AutoFaceLastKey) return;
      rt7AutoFaceLastKey=key;
      rt7ShowFaceSnapshot(m);
      setAnswer(rt7FaceResultText_(m,true));
      setDebug('auto face PASS result '+key);
    }catch(e){
      // Keep silent to avoid disturbing normal stream UI.
    }finally{
      rt7AutoFacePollBusy=false;
    }
  }
  setInterval(rt7PollAutoFaceResult_, 1800);
  var rt7FaceMatchBusy=false;
  var rt7FaceRestoreBusy=false;
  var rt7FaceLastDoneAt=0;
  var rt7FaceResumeTimer=null;
  var rt7FaceResumeStarted=false;
  function rt7FaceSetButtonBusy(faceBtn,busy){
    try{ if(faceBtn){ faceBtn.disabled=!!busy; faceBtn.style.opacity=busy?'0.55':''; faceBtn.textContent=busy?'辨識中':'人臉辨識'; } }catch(_){}
  }
  function rt7FacePauseStreamOnce(){
    // V5.3C: exactly one intentional MJPEG close before face snapshot.
    clearLanReconnect();
    try{ if(img){ img.onerror=null; img.onload=null; img.src='about:blank'; img.removeAttribute('src'); } }catch(_){}
    videoWanted=false;
    currentStreamMode='FACE_PAUSE';
    if(badge) badge.textContent='FACE';
  }
  function rt7FaceResumeStreamOnce(prevMode,hold,faceBtn){
    // V5.3C: exactly one restore after result; block focus/resize restore while doing it.
    if(rt7FaceResumeStarted) return;
    rt7FaceResumeStarted=true;
    clearTimeout(rt7FaceResumeTimer);
    rt7FaceRestoreBusy=true;
    rt7FaceResumeTimer=setTimeout(function(){
      try{
        if(prevMode==='LAN') lan();
        else if(prevMode==='CLOUD') cloud();
        else startAuto();
      }catch(_){ try{ startAuto(); }catch(__){} }
      setTimeout(function(){ if(hold) setAnswer(hold); }, 180);
      setTimeout(function(){
        rt7FaceBusy=false; rt7FaceMatchBusy=false; rt7FaceRestoreBusy=false; rt7FaceLastDoneAt=Date.now(); rt7FaceResumeStarted=false;
        rt7FaceSetButtonBusy(faceBtn,false);
      }, 1500);
    }, 650);
  }
  async function rt7FaceMatch(){
    if(rt7FaceMatchBusy || rt7FaceRestoreBusy){ setAnswer('人臉辨識進行中，請稍候...'); return; }
    if(Date.now() - rt7FaceLastDoneAt < 3500){ setAnswer('剛完成辨識，請稍候再按。'); return; }
    rt7FaceMatchBusy=true; rt7FaceBusy=true; rt7FaceResumeStarted=false;
    var faceBtn=document.getElementById('btnFaceCheck');
    rt7FaceSetButtonBusy(faceBtn,true);
    var wasVideo = !!videoWanted;
    var prevMode = currentStreamMode || 'AUTO';
    var keepResult = '';
    try{
      // V5.3C: pause stream only once, then resume only once after result.
      if(wasVideo){
        setAnswer('暫停影像，準備人臉辨識...');
        rt7FacePauseStreamOnce();
        await new Promise(function(resolve){ setTimeout(resolve, 850); });
      }

      setAnswer('人臉辨識中...');
      var j=await rt7Json('/api/rt7/face/match',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({pause_stream:true,no_stream_reload:true,mode:'face_result_no_stream_reload_v52n'})});
      rt7ShowFaceSnapshot(j);
      if(j.ok && j.known_face) {
        keepResult=((j.reason==='BACKLIGHT_PASS')?'逆光但人臉通過：':'人臉通過：')+(j.matched_name||'已註冊')+' / '+(j.confidence||0)+'%｜FACE_FOUND='+(j.face_found?'YES':'NO')+'｜COUNT='+(j.face_count||0)+'｜BOX='+(j.face_box?((j.face_box.w||0)+'x'+(j.face_box.h||0)):'0x0')+'｜RATIO='+(j.face_ratio||0)+'%｜品質='+(j.face_quality||'UNKNOWN')+'｜SNAP='+(j.snap_hash||'')+'｜ENGINE='+(j.engine||'railway_local')+'｜CACHE='+(j.cache_mode||'')+'｜MS='+(j.match_ms||'')+'｜REASON='+(j.reason||'FACE_OK');
      } else if(j.ok) {
        keepResult='人臉未通過：'+(j.reason||'UNKNOWN')+'｜FACE_FOUND='+(j.face_found?'YES':'NO')+'｜COUNT='+(j.face_count||0)+'｜BOX='+(j.face_box?((j.face_box.w||0)+'x'+(j.face_box.h||0)):'0x0')+'｜RATIO='+(j.face_ratio||0)+'%｜FAIL='+(j.fail_stage||'UNKNOWN')+'｜品質='+(j.face_quality||'UNKNOWN')+'｜SNAP='+(j.snap_hash||'')+'｜ENGINE='+(j.engine||'railway_local')+'｜CACHE='+(j.cache_mode||'')+'｜MS='+(j.match_ms||'')+'｜'+(j.summary||'');
      } else {
        keepResult='人臉辨識失敗：'+(j.answer||j.error||'UNKNOWN');
      }
      setAnswer(keepResult);
      try{ setDebug((j.debug_text||JSON.stringify(j)).slice(0,240)); }catch(_){ }
    }catch(e){
      keepResult='人臉辨識失敗：'+(e.message||e);
      setAnswer(keepResult);
    }finally{
      if(wasVideo){
        var hold = keepResult || (answer && answer.textContent) || '';
        rt7FaceResumeStreamOnce(prevMode, hold, faceBtn);
      } else {
        rt7FaceBusy=false; rt7FaceMatchBusy=false; rt7FaceLastDoneAt=Date.now();
        rt7FaceSetButtonBusy(faceBtn,false);
      }
    }
  }
  var _faceEnrollBtn=document.getElementById('btnFaceEnroll'); if(_faceEnrollBtn)_faceEnrollBtn.addEventListener('click',function(ev){ev.preventDefault();rt7FaceEnroll();});
  var _faceListBtn=document.getElementById('btnFaceList'); if(_faceListBtn)_faceListBtn.addEventListener('click',function(ev){ev.preventDefault();rt7FaceList();});
  var _faceCheckBtn=document.getElementById('btnFaceCheck'); if(_faceCheckBtn)_faceCheckBtn.addEventListener('click',function(ev){ev.preventDefault();rt7FaceMatch();});
  try{ if(document.getElementById('btnAiOn')) document.getElementById('btnAiOn').addEventListener('dblclick',function(ev){ev.preventDefault();rt7FaceMatch();}); }catch(_){ }
  function rt7WsUrl(){ return (location.protocol==='https:'?'wss://':'ws://')+location.host+'/ws'; }
  function rt7WsPcm16Bytes(f32){ var b=new Uint8Array(f32.length*2); for(var i=0;i<f32.length;i++){ var v=Math.max(-1,Math.min(1,f32[i])); var s=Math.round(v<0?v*0x8000:v*0x7fff); b[i*2]=s&255; b[i*2+1]=(s>>8)&255; } return b; }
  function rt7WsDown16(input,rate){ if(!rate||Math.abs(rate-16000)<1)return input; var ratio=rate/16000, len=Math.floor(input.length/ratio); var out=new Float32Array(Math.max(0,len)); for(var i=0;i<len;i++){ var a=Math.floor(i*ratio), b=Math.min(Math.floor((i+1)*ratio),input.length), sum=0,c=0; for(var j=a;j<b;j++){sum+=input[j];c++;} out[i]=c?sum/c:0; } return out; }
  function rt7WsClean(input){ var out=new Float32Array(input.length); var peak=0; for(var i=0;i<input.length;i++){ var x=input[i]*0.86; if(x>0.98)x=0.98; if(x<-0.98)x=-0.98; out[i]=x; var a=Math.abs(x); if(a>peak)peak=a; } out.peak=peak; return out; }
  function rt7WsSendJson(o){ try{ if(rt7WsIc&&rt7WsIc.readyState===1) rt7WsIc.send(JSON.stringify(o)); }catch(e){} }
  function rt7WsQueue(bytes){ for(var i=0;i<bytes.length;i++) rt7WsTxBytes.push(bytes[i]); while(rt7WsTxBytes.length>=640){ var chunk=rt7WsTxBytes.splice(0,640); if(rt7WsTxActive&&rt7WsIc&&rt7WsIc.readyState===1){ rt7WsSent++; try{ rt7WsIc.send(new Uint8Array(chunk).buffer); }catch(e){ setDebug('ws pcm send failed '+(e.message||e)); } } } }
  function rt7WsStopMic(){ try{ if(rt7WsMicProc){rt7WsMicProc.disconnect();rt7WsMicProc.onaudioprocess=null;} }catch(_){} try{ if(rt7WsMicSource)rt7WsMicSource.disconnect(); }catch(_){} try{ if(rt7WsMicCtx)rt7WsMicCtx.close(); }catch(_){} try{ if(rt7WsMicStream)rt7WsMicStream.getTracks().forEach(function(t){try{t.stop();}catch(_){}}); }catch(_){} rt7WsMicStream=null; rt7WsMicCtx=null; rt7WsMicSource=null; rt7WsMicProc=null; }
  async function rt7WsStartMic(){
    rt7WsStopMic();
    rt7WsMicStream=await navigator.mediaDevices.getUserMedia({audio:{echoCancellation:{ideal:true},noiseSuppression:{ideal:false},autoGainControl:{ideal:false},channelCount:{ideal:1},sampleRate:{ideal:48000}},video:false});
    var AC=window.AudioContext||window.webkitAudioContext; rt7WsMicCtx=new AC();
    rt7WsMicSource=rt7WsMicCtx.createMediaStreamSource(rt7WsMicStream);
    rt7WsMicProc=rt7WsMicCtx.createScriptProcessor(2048,1,1);
    rt7WsMicProc.onaudioprocess=function(e){ if(!rt7WsTxActive)return; var raw=e.inputBuffer.getChannelData(0); var cl=rt7WsClean(raw); if(cl.peak<0.00004)return; var ds=rt7WsDown16(cl,rt7WsMicCtx.sampleRate); if(ds.length)rt7WsQueue(rt7WsPcm16Bytes(ds)); };
    rt7WsMicSource.connect(rt7WsMicProc); rt7WsMicProc.connect(rt7WsMicCtx.destination); if(rt7WsMicCtx.state!=='running') await rt7WsMicCtx.resume();
    setDebug('WS PTT mic ready sr='+Math.round(rt7WsMicCtx.sampleRate));
  }
  function rt7RxEnsureAudio(){ var AC=window.AudioContext||window.webkitAudioContext; if(!rt7RxAudioCtx) rt7RxAudioCtx=new AC({sampleRate:16000}); if(rt7RxAudioCtx.state!=='running') rt7RxAudioCtx.resume().catch(function(){}); }
  function rt7RxPlayPcm(ab){
    try{
      rt7RxEnsureAudio();
      var u8=new Uint8Array(ab); var n=(u8.length/2)|0; if(n<=0)return;
      var f=new Float32Array(n);
      for(var i=0;i<n;i++){ var lo=u8[i*2], hi=u8[i*2+1]; var s=(hi<<8)|lo; if(s&0x8000)s-=0x10000; f[i]=Math.max(-1,Math.min(1,s/32768)); }
      var buf=rt7RxAudioCtx.createBuffer(1,n,16000); buf.copyToChannel(f,0);
      var src=rt7RxAudioCtx.createBufferSource(); src.buffer=buf; src.connect(rt7RxAudioCtx.destination);
      var wall=Date.now(); var dt=rt7RxLastMs?(wall-rt7RxLastMs):0; rt7RxLastMs=wall; if(dt>rt7RxJitterMaxMs)rt7RxJitterMaxMs=dt;
      var now=rt7RxAudioCtx.currentTime;
      // V5.4P: adaptive low-latency jitter buffer for ESP32 -> phone audio.
      // Keep a small cushion when packet timing is unstable, but do not rebuild audio nodes.
      var cushion=(dt>80||rt7RxJitterMaxMs>120)?0.11:0.06;
      if(!rt7RxPlayAt || rt7RxPlayAt<now+cushion) rt7RxPlayAt=now+cushion;
      if(rt7RxPlayAt>now+0.35) rt7RxPlayAt=now+0.18;
      src.start(rt7RxPlayAt); rt7RxPlayAt += n/16000;
      rt7RxPackets++; rt7RxBytes+=u8.length; if(rt7RxPackets<=5||rt7RxPackets%20===0)setDebug('ESP32→手機 PCM packets='+rt7RxPackets+' bytes='+rt7RxBytes+' jitterMax='+rt7RxJitterMaxMs+'ms');
    }catch(e){ setDebug('rx play failed '+(e.message||e)); }
  }
  var rt7WsPausedVideoMode=null;
  function rt7Delay(ms){ return new Promise(function(resolve){ setTimeout(resolve, ms); }); }
  function rt7RememberAndPauseVideoForTalk(){
    rt7WsPausedVideoMode=null;
    if(videoWanted || currentStreamMode==='LAN' || currentStreamMode==='CLOUD' || currentStreamMode==='AUTO'){
      rt7WsPausedVideoMode=currentStreamMode||'AUTO';
      try{ localStorage.setItem('RT7_V50_TALK_RESTORE_MODE', rt7WsPausedVideoMode); }catch(_){}
      stopVideo();
      setAnswer('對講準備中：已先暫停影像，避免內網串流擋住麥克風');
      return true;
    }
    return false;
  }
  function rt7RestoreVideoAfterTalk(){
    var m=rt7WsPausedVideoMode; rt7WsPausedVideoMode=null;
    if(!m || m==='IDLE') return;
    setAnswer('對講結束，恢復影像中...');
    setTimeout(function(){ if(rt7WsIcOn) return; if(m==='LAN') lan(); else if(m==='CLOUD') cloud(); else startAuto(); }, 650);
  }
  function rt7SetTalkIcon_(state){
    try{
      var b=document.getElementById('btnEndTalk');
      var v=document.getElementById('btnVoice');
      if(state==='talk'){ if(b){b.textContent='◼'; b.classList.add('talking');} if(v){v.textContent='◼'; v.classList.add('talking');} }
      else if(state==='listen'){ if(b){b.textContent='◼'; b.classList.add('talking');} if(v){v.textContent='🔊'; v.classList.remove('talking');} }
      else { if(b){b.textContent='◼'; b.classList.remove('talking');} if(v){v.textContent='🎙️'; v.classList.remove('talking');} }
    }catch(e){}
  }
  async function rt7WsEnsureSocket(label){
    if(rt7WsIc && rt7WsIc.readyState===1) return true;
    return new Promise(function(resolve){
      rt7WsIc=new WebSocket(rt7WsUrl()+'?role=phone_pcm&device_id=%231&phase=V50P'); rt7WsIc.binaryType='arraybuffer';
      var done=false; function finish(ok){ if(done)return; done=true; resolve(ok); }
      rt7WsIc.onopen=function(){ setDebug('WS duplex open'); rt7WsSendJson({role:'phone_pcm',type:'intercom_probe',device_id:'#1',label:label||'open',t:Date.now(),phase:'V50P'}); finish(true); };
      rt7WsIc.onmessage=function(ev){ try{ if(typeof ev.data==='string'){ if(ev.data.indexOf('trace')>=0||ev.data.indexOf('relay')>=0) setDebug(ev.data.slice(0,180)); } else if(ev.data){ rt7RxPlayPcm(ev.data); } }catch(e){ setDebug('ws msg err '+(e.message||e)); } };
      rt7WsIc.onerror=function(){ setDebug('WS duplex error'); finish(false); };
      rt7WsIc.onclose=function(){ rt7WsIcOn=false; rt7WsTxActive=false; rt7WsListenActive=false; rt7WsStopMic(); var a=document.getElementById('btnEndTalk'); if(a)a.classList.remove('talking'); var b=document.getElementById('btnVoice'); if(b)b.classList.remove('talking'); };
      setTimeout(function(){ finish(rt7WsIc&&rt7WsIc.readyState===1); }, 2200);
    });
  }
  async function rt7WsPttDown(label){
    if(rt7WsListenTimer){ clearTimeout(rt7WsListenTimer); rt7WsListenTimer=null; }
    var paused=false;
    if(!rt7WsIcOn){ paused=rt7RememberAndPauseVideoForTalk(); if(paused) await rt7Delay(260); }
    rt7WsIcOn=true; rt7WsTxActive=true; rt7WsListenActive=false; rt7WsSent=0; rt7WsTxBytes=[]; rt7RxPackets=0; rt7RxBytes=0; rt7RxPlayAt=0; rt7RxLastMs=0; rt7RxJitterMaxMs=0;
    var e=document.getElementById('btnEndTalk'); if(e)e.classList.add('talking'); var vm=document.getElementById('btnVoice'); if(vm)vm.classList.add('talking');
    rt7SetTalkIcon_('talk'); setAnswer('對講中：手機 → ESP32，放開後接收 ESP32 聲音');
    var ok=await rt7WsEnsureSocket(label||'ptt_down'); if(!ok){ setAnswer('對講連線失敗'); return; }
    rt7WsSendJson({role:'phone_pcm',type:'intercom_begin',device_id:'#1',label:label||'ptt_down',t:Date.now(),phase:'V50P'});
    try{ await rt7WsStartMic(); }catch(err){ setAnswer('手機麥克風啟用失敗：'+(err.message||err)); rt7WsPttStop('mic_failed'); }
  }
  function rt7WsPttUp(label){
    if(!rt7WsIcOn) return;
    rt7WsTxActive=false; rt7WsStopMic();
    rt7WsSendJson({role:'phone_pcm',type:'intercom_end',device_id:'#1',label:label||'ptt_up',sent:rt7WsSent,t:Date.now(),phase:'V50P'});
    rt7WsSendJson({role:'phone_pcm',type:'esp_begin',device_id:'#1',label:label||'ptt_up_listen',t:Date.now(),phase:'V50P'});
    rt7WsListenActive=true; rt7RxEnsureAudio();
    var e=document.getElementById('btnEndTalk'); if(e)e.classList.remove('talking'); var vm=document.getElementById('btnVoice'); if(vm)vm.classList.remove('talking');
    rt7SetTalkIcon_('listen'); setAnswer('接收中：ESP32 → 手機；按下 ◼ 對講 才結束');
    if(rt7WsListenTimer){ clearTimeout(rt7WsListenTimer); rt7WsListenTimer=null; }
    // V50P: 放開中央對講鍵後，保持 ESP32→手機接收，不再自動 10 秒結束。
    // 只有按下下方「◼ 對講」結束鍵，才會停止接收與恢復影像。
  }
  function rt7WsPttStop(label){
    if(rt7WsListenTimer){ clearTimeout(rt7WsListenTimer); rt7WsListenTimer=null; }
    rt7WsTxActive=false; rt7WsListenActive=false;
    rt7WsSendJson({role:'phone_pcm',type:'esp_end',device_id:'#1',label:label||'stop',t:Date.now(),phase:'V50P'});
    rt7WsSendJson({role:'phone_pcm',type:'intercom_end',device_id:'#1',label:label||'stop',sent:rt7WsSent,t:Date.now(),phase:'V50P'});
    setTimeout(function(){ try{ if(rt7WsIc)rt7WsIc.close(); }catch(_){} rt7WsIc=null; rt7WsStopMic(); rt7WsIcOn=false; rt7RestoreVideoAfterTalk(); },120);
    rt7SetTalkIcon_('idle'); setAnswer('對講結束');
  }
  function rt7BindPtt(id,label){
    var el=document.getElementById(id); if(!el)return;
    var down=false;
    function d(ev){ if(ev){ev.preventDefault();ev.stopPropagation();} if(down)return; down=true; rt7WsPttDown(label); }
    function u(ev){ if(ev){ev.preventDefault();ev.stopPropagation();} if(!down)return; down=false; rt7WsPttUp(label); }
    el.addEventListener('pointerdown',d,{passive:false}); el.addEventListener('pointerup',u,{passive:false}); el.addEventListener('pointercancel',u,{passive:false}); el.addEventListener('pointerleave',function(ev){ if(down)u(ev); },{passive:false});
    el.addEventListener('touchstart',d,{passive:false}); el.addEventListener('touchend',u,{passive:false});
    el.addEventListener('click',function(ev){ ev.preventDefault(); ev.stopPropagation(); },true);
  }
  rt7BindPtt('btnVoice','center_ptt');
  // V50P: 下方「◼ 對講」只做結束鍵，不再做 PTT。
  (function(){
    var endBtn=document.getElementById('btnEndTalk');
    if(!endBtn) return;
    function stop(ev){
      if(ev){ ev.preventDefault(); ev.stopPropagation(); }
      if(rt7WsIcOn || rt7WsTxActive || rt7WsListenActive){ rt7WsPttStop('manual_end_button'); }
      else { setAnswer('目前沒有進行中的對講'); rt7SetTalkIcon_('idle'); }
    }
    endBtn.addEventListener('click', stop, true);
    endBtn.addEventListener('touchend', stop, {passive:false});
    endBtn.addEventListener('pointerup', stop, {passive:false});
  })();
  var lastCount=null;
  async function pollDoor(){ try{ var r=await fetch('/api/rt7/doorbell/state?_='+Date.now(),{cache:'no-store'}); var jj=await r.json(); var st=jj.state||jj; if(st&&typeof st.count==='number'){ if(lastCount===null) lastCount=st.count; if(st.count!==lastCount){ lastCount=st.count; showDoorbellInline(); } } }catch(e){} setTimeout(pollDoor,2500); }
  pollDoor();

  // V5.0D: Phone sleep / foreground-background auto recovery, based on original Node-RED design.
  // - Uses Screen Wake Lock when user starts video or taps page.
  // - Records whether the user wants video in localStorage.
  // - On visibilitychange/pageshow/focus/resize, restores the previous LAN/CLOUD stream.
  // - On background, notifies Railway to lower cloud FPS, but does not destroy the user's wanted state.
  var rt7WakeLock=null; var rt7RestoreBusy=false; var rt7RestoreTimer=null; var rt7FaceBusy=false;
  async function rt7RequestWakeLock(reason){
    try{
      if(document.visibilityState && document.visibilityState!=='visible') return false;
      if(!('wakeLock' in navigator) || !navigator.wakeLock || !navigator.wakeLock.request) return false;
      if(rt7WakeLock) return true;
      rt7WakeLock = await navigator.wakeLock.request('screen');
      rt7WakeLock.addEventListener('release', function(){ rt7WakeLock=null; setDebug('wakelock released'); });
      setDebug('wakelock on '+reason);
      return true;
    }catch(e){ setDebug('wakelock unavailable '+reason); return false; }
  }
  async function rt7ResumeAudio(reason){
    try{ if(audioCtx && audioCtx.state!=='running') await audioCtx.resume(); }catch(e){}
  }
  function rt7VideoWanted(){
    try{ return videoWanted || localStorage.getItem('RT7_V50_WANTED_VIDEO')==='1'; }catch(e){ return videoWanted; }
  }
  function rt7SavedMode(){
    try{ return localStorage.getItem('RT7_V50_STREAM_MODE') || currentStreamMode || 'AUTO'; }catch(e){ return currentStreamMode || 'AUTO'; }
  }
  function rt7RestoreVideo(reason){
    if(rt7FaceBusy || rt7FaceRestoreBusy) return;
    if(!rt7VideoWanted()) return;
    if(rt7RestoreBusy) return;
    clearTimeout(rt7RestoreTimer);
    rt7RestoreTimer=setTimeout(function(){
      if(!rt7VideoWanted()) return;
      if(document.visibilityState && document.visibilityState!=='visible') return;
      rt7RestoreBusy=true;
      rt7RequestWakeLock(reason);
      rt7ResumeAudio(reason);
      var m=rt7SavedMode();
      setAnswer('回到前景：恢復影像串流中');
      setDebug('restore video '+reason+' mode='+m);
      try{
        if(m==='LAN' && currentStreamMode==='LAN'){
          if(img) img.src='http://'+ip+'/api/camera/stream?_fg='+Date.now();
        }else if(m==='CLOUD' && currentStreamMode==='CLOUD'){
          if(img) img.src='/api/rt7/camera/stream.mjpg?_fg='+Date.now();
        }else{
          startAuto();
        }
      }catch(e){ startAuto(); }
      setTimeout(function(){ rt7RestoreBusy=false; }, 1200);
    }, reason==='resize' ? 800 : 350);
  }
  function rt7BackgroundIdle(reason){
    if(rt7FaceBusy || rt7FaceRestoreBusy) return;
    if(!rt7VideoWanted()) return;
    setDebug('background idle '+reason);
    try{ navigator.sendBeacon && navigator.sendBeacon('/api/rt7/camera/viewer/ping?state=hidden&_='+Date.now(), ''); }catch(e){}
    try{ fetch('/api/rt7/camera/stream/stop?_bg='+Date.now(), {cache:'no-store', keepalive:true}).catch(function(){}); }catch(e){}
  }
  ['click','touchend','pointerup'].forEach(function(ev){ document.addEventListener(ev, function(){ rt7RequestWakeLock(ev); rt7ResumeAudio(ev); }, {passive:true}); });
  document.addEventListener('visibilitychange', function(){
    if(document.visibilityState==='visible') rt7RestoreVideo('visibilitychange');
    else rt7BackgroundIdle('visibilitychange');
  });
  window.addEventListener('pageshow', function(){ rt7RestoreVideo('pageshow'); });
  window.addEventListener('focus', function(){ rt7RestoreVideo('focus'); });
  window.addEventListener('resize', function(){ if(document.visibilityState==='visible') rt7RestoreVideo('resize'); });
  setInterval(function(){
    if(!rt7VideoWanted()) return;
    var state=(document.visibilityState==='visible')?'visible':'hidden';
    try{ fetch('/api/rt7/camera/viewer/ping?state='+state+'&_='+Date.now(), {cache:'no-store'}).catch(function(){}); }catch(e){}
  }, 15000);

  if(mode==='auto') setTimeout(startAuto, 300); else if(mode==='lan') lan(); else if(mode==='cloud') cloud(); else stopVideo();
})();
</script>
</body></html>`);
});

app.get('/api/rt7/events/latest', (req,res)=>res.redirect(307, '/api/events/latest?limit=' + encodeURIComponent(req.query.limit || '200')));
app.get('/api/rt7/events/clear', (req,res)=>res.redirect(307, '/api/events/clear'));
app.get('/api/rt7/devices/list', (req,res)=>res.json({ ok:true, devices:readDevices(), current_device_id:cloudState.current_device_id }));
app.post('/api/rt7/devices/save', (req,res)=>{
  const devices = saveDevices(req.body?.devices || req.body || []);
  appendEvent({ type:'devices_save', device_count:devices.length, message:'devices saved from Phase10 API' });
  res.json({ ok:true, devices });
});
app.get('/api/rt7/device/state', (req,res)=>res.json({ ok:true, current_device_id:cloudState.current_device_id, device:getCurrentDevice(req), cloudState }));
app.post('/api/rt7/device/set', (req,res)=>{ cloudState.current_device_id = safeString(req.body?.device_id || req.body?.id || req.query.device_id || '#1'); res.json({ ok:true, current_device_id:cloudState.current_device_id, device:getCurrentDevice(req) }); });
app.get('/api/rt7/device/proxy_status', (req,res)=>proxyToEsp(req,res,'/api/status','GET'));

// Independent full intercom proxy-compatible endpoints
app.get('/api/ind_full/ui/state', (req,res)=>proxyToEsp(req,res,'/api/ui/state','GET'));
app.get('/api/ind_full/audio/phone_begin', (req,res)=>proxyToEsp(req,res,'/api/audio/phone_begin','GET'));
app.post('/api/ind_full/audio/phone_pcm_hex', express.text({type:'*/*', limit:'2mb'}), (req,res)=>proxyToEsp(req,res,'/api/audio/phone_pcm_hex','POST'));
app.get('/api/ind_full/audio/phone_end', (req,res)=>proxyToEsp(req,res,'/api/audio/phone_end','GET'));
app.get('/api/ind_full/audio/esp_begin', (req,res)=>proxyToEsp(req,res,'/api/audio/esp_begin','GET'));
app.get('/api/ind_full/audio/esp_pcm_hex', (req,res)=>proxyToEsp(req,res,'/api/audio/esp_pcm_hex','GET'));
app.get('/api/ind_full/audio/esp_end', (req,res)=>proxyToEsp(req,res,'/api/audio/esp_end','GET'));
app.get('/api/ind_full/audio/speaker_tone', (req,res)=>proxyToEsp(req,res,'/api/audio/speaker_tone','GET'));
app.get('/api/ind_full/audio/mic_raw_test', (req,res)=>proxyToEsp(req,res,'/api/audio/mic_raw_test','GET'));

// Plugin / guard state compatible with Phase6C3 Node-RED flow
app.get('/api/rt7/phase6c3_plugin/ping', (req,res)=>res.json({ ok:true, version:SERVER_VERSION, mode:'railway_no_nodered', time:nowIso() }));
app.get('/api/rt7/phase6c3_plugin/plugins/state', (req,res)=>res.json({ ok:true, plugins:cloudState.plugins, ai_enabled:cloudState.ai_enabled, last_snapshot:cloudState.last_snapshot, last_vision:cloudState.last_vision }));
app.get('/api/rt7/phase6c3_plugin/plugins/:plugin/:action', (req,res)=>{ const p=req.params.plugin, a=req.params.action; cloudState.plugins[p] = !/disable|off|0/i.test(a); appendEvent({type:'plugin_set', plugin:p, enabled:cloudState.plugins[p]}); res.json({ok:true, plugins:cloudState.plugins}); });
app.get('/api/rt7/phase6c3_plugin/plugins/reset', (req,res)=>{ cloudState.plugins={ motion:true, face:true, doorbell:true, intercom:true }; res.json({ok:true, plugins:cloudState.plugins}); });
app.get('/api/rt7/phase6c3_plugin/status', (req,res)=>res.json({ ok:true, version:SERVER_VERSION, cloudState, doorbellState }));
app.get('/api/rt7/phase6c3_plugin/camera/status', (req,res)=>res.json({ ok:true, camera:{ latest:!!cloudState.last_snapshot, last_snapshot:cloudState.last_snapshot } }));
app.get('/api/rt7/phase6c3_plugin/camera/state', (req,res)=>res.json({ ok:true, state:{ latest_snapshot:cloudState.last_snapshot, url: cloudState.last_snapshot ? '/api/rt7/camera/latest.jpg' : '' } }));


// V5.4A: ESP32 FACE_GATE auto recognition.
// ESP32 performs motion + FACE_GATE candidate detection, then POSTs the candidate JPEG.
// Railway receives that snapshot and runs the same Railway-local face detect/match flow
// without sending face_snapshot_now back to ESP32 and without restarting the MJPEG viewer.
function rt7IsFaceGateAutoSnapshot_(req) {
  const q = safeString(req && req.query && (req.query.face_gate_auto || req.query.auto_face || req.query.source) || '');
  const h = safeString(req && (req.headers['x-rt7-face-gate-auto'] || req.headers['x-rt7-source'] || req.headers['x-rt7-snapshot-source']) || '');
  return /face_gate_auto|rt7_face_gate_auto|1|true/i.test(q) || /face_gate_auto|rt7_face_gate_auto/i.test(h);
}
function rt7HasPendingFaceSnapshotCommand_() {
  try {
    const a = Array.isArray(cloudState.command_queue) ? cloudState.command_queue : [];
    const b = Array.isArray(pendingCommands) ? pendingCommands : [];
    return a.concat(b).some(c => c && c.status === 'pending' && (c.command === 'face_snapshot_now' || c.action === 'face_snapshot_now' || c.priority === 'face_snapshot'));
  } catch (_) { return false; }
}
function rt7ShouldAutoMatchSnapshot_(req, bytes) {
  // V5.4B: ESP32 FACE_GATE auto snapshot currently POSTs to the same snapshot endpoint
  // without a reliable query/header marker.  If FACE_GATE auto is enabled and the
  // snapshot is not part of a manual face_snapshot_now command, treat it as an
  // ESP32 FACE_GATE candidate.  Cooldown inside rt7StartFaceGateAutoMatch_ prevents
  // repeated recognition while the person remains in front of the camera.
  if (rt7IsFaceGateAutoSnapshot_(req)) return true;
  // V5.4C: FACE_GATE auto recognition is controlled by face_gate_auto_enabled,
  // not by the old AI-enable flag.  ESP32 posts only FACE_GATE candidate
  // snapshots to this endpoint in this mode; Railway must force local match.
  if (!cloudState.face_gate_auto_enabled) return false;
  const q = safeString(req && req.query && (req.query.no_auto_face || req.query.manual || req.query.probe) || '');
  if (/1|true|yes/i.test(q)) return false;
  if (rt7HasPendingFaceSnapshotCommand_()) return false;
  return Number(bytes || 0) >= 3000;
}
function rt7StartFaceGateAutoMatch_(reason) {
  const now = Date.now();
  if (!cloudState.face_gate_auto_enabled) {
    console.log('[RT7_FACE_GATE_AUTO][V54O][SKIP] auto_disabled ai=' + (cloudState.ai_enabled?1:0) + ' auto=' + (cloudState.face_gate_auto_enabled?1:0));
    return false;
  }
  if (cloudState.face_gate_auto_busy) {
    console.log('[RT7_FACE_GATE_AUTO][V54O][SKIP] busy reason=' + safeString(reason));
    return false;
  }
  const cd = Number(cloudState.face_gate_auto_cooldown_ms || 8000);
  if (cloudState.face_gate_auto_last_ms && (now - cloudState.face_gate_auto_last_ms) < cd) {
    console.log('[RT7_FACE_GATE_AUTO][V54O][SKIP] cooldown remain=' + (cd - (now - cloudState.face_gate_auto_last_ms)) + 'ms');
    return false;
  }
  cloudState.face_gate_auto_busy = true;
  cloudState.face_gate_auto_last_ms = now;
  setTimeout(async () => {
    try {
      const latest = rt7GetLatestWithMeta_();
      if (!latest || !latest.b64) throw new Error('NO_LATEST_SNAPSHOT_FOR_AUTO_FACE');
      latest.snap_source = 'face_gate_auto_snapshot';
      latest.snap_forced_realtime = false;
      latest.snap_request_ws_sent = 0;
      latest.snap_wait_ms = 0;
      latest.snap_live_frame_fallback = false;
      latest.snap_stale_warning = false;
      latest.auto_face_gate = true;
      console.log('[RT7_FACE_GATE_AUTO][V54O][MATCH_START] hash=' + latest.snap_hash + ' bytes=' + latest.bytes + ' age=' + latest.snap_age_ms + 'ms reason=' + safeString(reason));
      const r = await rt7FaceMatchLatestCore_(latest, { auto_face_gate:true });
      if (r && typeof r === 'object') {
        r.auto_face_gate = true;
        r.trigger_source = 'esp32_face_gate';
        cloudState.last_face_match = r;
        broadcast('face_match', r);
      }
      console.log('[RT7_FACE_GATE_AUTO][V54O][MATCH_DONE] known=' + (r && r.known_face ? 1 : 0) + ' conf=' + (r && r.confidence || 0) + ' reason=' + (r && r.reason || ''));
    } catch (e) {
      const fail = { ok:false, version:SERVER_VERSION, type:'face_match', auto_face_gate:true, trigger_source:'esp32_face_gate', reason:'AUTO_FACE_MATCH_ERROR', error:String(e && e.message || e), time:nowIso() };
      cloudState.last_face_match = fail;
      broadcast('face_match', fail);
      console.warn('[RT7_FACE_GATE_AUTO][V54O][MATCH_ERR] ' + String(e && e.message || e));
    } finally {
      cloudState.face_gate_auto_busy = false;
    }
  }, 20);
  return true;
}
app.post('/api/rt7/phase6a_fix2/motion/event', (req,res)=>{
  const body = req.body || {};
  let autoStarted = false;
  try {
    const b64 = safeString(body.jpeg_b64 || body.image_b64 || body.b64 || '').replace(/^data:image\/jpeg;base64,/, '');
    if (b64 && body.motion_active !== false) {
      const buf = Buffer.from(b64, 'base64');
      if (buf && buf.length > 800 && buf[0] === 0xFF && buf[1] === 0xD8) {
        ensureDataDir();
        fs.writeFileSync(SNAPSHOT_FILE, buf);
        cloudState.last_snapshot = { ok:true, bytes:buf.length, time:nowIso(), source:'face_gate_auto_snapshot', device_id:safeString(body.device_id || body.ip || '#1'), ip:clientIp(req), url:'/api/rt7/camera/latest.jpg' };
        broadcast('snapshot', cloudState.last_snapshot);
        autoStarted = rt7StartFaceGateAutoMatch_('motion_event');
      }
    }
  } catch (e) { console.warn('[RT7_FACE_GATE_AUTO][V54O][MOTION_EVENT_ERR] ' + String(e && e.message || e)); }
  const ev=appendEvent(Object.assign({ type:'motion', message:'ESP32 motion event', auto_face_gate_started:autoStarted }, body));
  broadcast('motion', ev);
  res.json({ok:true,busy:autoStarted,event:ev,auto_face_gate_started:autoStarted});
});
app.get('/api/rt7/phase6c3_plugin/alarm/confirm', (req,res)=>{ const ev=appendEvent({type:'alarm_confirm', message:'alarm confirmed from cloud UI'}); broadcast('alarm_confirm', ev); res.json({ok:true,event:ev}); });


function getSnapshotMeta_() {
  ensureDataDir();
  if (cloudState.last_snapshot) return cloudState.last_snapshot;
  if (fs.existsSync(SNAPSHOT_FILE)) {
    const st = fs.statSync(SNAPSHOT_FILE);
    cloudState.last_snapshot = {
      ok: true,
      bytes: st.size,
      time: st.mtime.toISOString(),
      source: 'restored_from_file',
      device_id: '#1',
      ip: '',
      url: '/api/rt7/camera/latest.jpg'
    };
    return cloudState.last_snapshot;
  }
  return null;
}

// ESP32 actively uploads snapshots here. Supports raw image/jpeg or JSON {image_b64/jpeg_b64}.
app.post('/api/rt7/camera/snapshot', express.raw({type:['image/jpeg','image/jpg','application/octet-stream'], limit:'6mb'}), (req,res)=>{
  ensureDataDir();
  let buf = Buffer.isBuffer(req.body) ? req.body : null;
  if (!buf || buf.length < 10) return res.status(400).json({ok:false,error:'JPEG_BODY_REQUIRED'});
  fs.writeFileSync(SNAPSHOT_FILE, buf);
  const isAutoFace = rt7ShouldAutoMatchSnapshot_(req, buf.length);
  cloudState.last_snapshot = { ok:true, bytes:buf.length, time:nowIso(), source:isAutoFace?'face_gate_auto_snapshot':'raw_post', device_id:safeString(req.query.device_id || req.headers['x-rt7-device-id'] || '#1'), ip:clientIp(req), url:'/api/rt7/camera/latest.jpg' };
  const autoStarted = isAutoFace ? rt7StartFaceGateAutoMatch_('snapshot_post_auto_detect') : false;
  const ev=appendEvent({ type:isAutoFace?'face_gate_auto_snapshot':'snapshot', bytes:buf.length, message:isAutoFace?'face gate auto snapshot uploaded':'snapshot uploaded', auto_face_gate_started:autoStarted });
  broadcast('snapshot', cloudState.last_snapshot);
  res.json({ ok:true, snapshot:cloudState.last_snapshot, event:ev, auto_face_gate_started:autoStarted });
});
app.post('/api/rt7/camera/snapshot_json', (req,res)=>{
  ensureDataDir();
  const b64 = safeString(req.body?.image_b64 || req.body?.jpeg_b64 || req.body?.b64 || '').replace(/^data:image\/jpeg;base64,/, '');
  if (!b64) return res.status(400).json({ok:false,error:'image_b64 required'});
  const buf = Buffer.from(b64, 'base64');
  fs.writeFileSync(SNAPSHOT_FILE, buf);
  const isAutoFace = rt7ShouldAutoMatchSnapshot_(req, buf.length);
  cloudState.last_snapshot = { ok:true, bytes:buf.length, time:nowIso(), source:isAutoFace?'face_gate_auto_snapshot':'json_b64', device_id:safeString(req.body?.device_id || '#1'), ip:clientIp(req), url:'/api/rt7/camera/latest.jpg' };
  const autoStarted = isAutoFace ? rt7StartFaceGateAutoMatch_('snapshot_json_auto_detect') : false;
  const ev=appendEvent({ type:isAutoFace?'face_gate_auto_snapshot':'snapshot', bytes:buf.length, message:isAutoFace?'face gate auto snapshot uploaded json':'snapshot uploaded json', auto_face_gate_started:autoStarted });
  broadcast('snapshot', cloudState.last_snapshot);
  res.json({ ok:true, snapshot:cloudState.last_snapshot, event:ev, auto_face_gate_started:autoStarted });
});


function streamViewerPrune_() {
  const now = Date.now();
  for (const [id, meta] of streamViewers.entries()) {
    if (!meta || (now - (meta.ts || 0)) > RT7_VIEWER_ACTIVE_TTL_MS) streamViewers.delete(id);
  }
  liveStreamState.viewer_count = streamViewers.size;
  liveStreamState.last_viewer_ping = streamViewers.size ? new Date(Math.max(...Array.from(streamViewers.values()).map(v=>v.ts||0))).toISOString() : null;
  return streamViewers.size;
}
function streamMode_(mode, req) {
  const dev = getCurrentDevice(req);
  const deviceId = normalizeDoorCommandDeviceId_(safeString(req.query.device_id || dev.id || '#1'));
  const fast = mode === 'fast';
  liveStreamState.enabled = true;
  liveStreamState.fps_mode = fast ? 'fast' : 'idle';
  liveStreamState.desired_interval_ms = fast ? ((Date.now() < rt7MjpegCongestUntilMs) ? RT7_STREAM_STABLE_MS : RT7_STREAM_FAST_MS) : RT7_STREAM_IDLE_MS;
  liveStreamState.adaptive_mode = fast ? ((Date.now() < rt7MjpegCongestUntilMs) ? 'fallback_7fps' : 'target_10fps') : 'idle_1fps';
  const cmd = queueCommand({
    command: fast ? 'stream_start' : 'stream_idle',
    action: fast ? 'stream_start' : 'stream_idle',
    device_id: deviceId,
    interval_ms: liveStreamState.desired_interval_ms,
    message: fast ? 'viewer foreground: fast live stream' : 'viewer background: idle live stream'
  });
  return { ok:true, version:SERVER_VERSION, stream:liveStreamState, command:cmd };
}

function acceptWsStreamFrame_(buf, ws) {
  ensureDataDir();
  if (!Buffer.isBuffer(buf)) buf = Buffer.from(buf || []);
  if (!buf || buf.length < 16 || buf[0] !== 0xFF || buf[1] !== 0xD8) return false;
  if (rt7AudioActive_()) return true;
  latestStreamFrame = Buffer.from(buf);
  fs.writeFileSync(STREAM_FRAME_FILE, latestStreamFrame);
  fs.writeFileSync(SNAPSHOT_FILE, latestStreamFrame); // keep Vision QA / latest.jpg aligned with live stream
  const meta = { ok:true, bytes:buf.length, time:nowIso(), source:'ws_frame', device_id:safeString(ws?.rt7DeviceId || '#1'), ip:safeString(ws?._socket?.remoteAddress || ''), url:'/api/rt7/camera/latest.jpg' };
  cloudState.last_snapshot = meta;
  liveStreamState = Object.assign({}, liveStreamState, { ok:true, transport:'ws_frame', seq:(liveStreamState.seq||0)+1, bytes:buf.length, time:meta.time, device_id:meta.device_id, ip:meta.ip, last_url:'/ws', last_frame_ms:Date.now() });
  broadcastBinaryToViewers(latestStreamFrame);
  broadcast('stream_frame', liveStreamState);
  return true;
}

// V4.7 WebSocket Frame Stream: ESP32 sends binary JPEG frames to /ws; browser receives binary JPEG frames.
// HTTP POST /api/rt7/camera/frame remains as a fallback.
function acceptStreamFrame_(req, res) {
  ensureDataDir();
  const buf = Buffer.isBuffer(req.body) ? req.body : null;
  if (!buf || buf.length < 16 || buf[0] !== 0xFF || buf[1] !== 0xD8) return res.status(400).json({ok:false,error:'JPEG_FRAME_REQUIRED'});
  if (rt7AudioActive_()) return res.json({ ok:true, version:SERVER_VERSION, audio_gate:true, skipped:true });
  latestStreamFrame = Buffer.from(buf);
  fs.writeFileSync(STREAM_FRAME_FILE, latestStreamFrame);
  fs.writeFileSync(SNAPSHOT_FILE, latestStreamFrame); // keep Vision QA / latest.jpg aligned with live stream
  const meta = { ok:true, bytes:buf.length, time:nowIso(), source:'live_frame', device_id:safeString(req.query.device_id || req.headers['x-rt7-device-id'] || '#1'), ip:clientIp(req), url:'/api/rt7/camera/latest.jpg' };
  cloudState.last_snapshot = meta;
  liveStreamState = Object.assign({}, liveStreamState, { ok:true, transport:'http_frame_relay', seq:(liveStreamState.seq||0)+1, bytes:buf.length, time:meta.time, device_id:meta.device_id, ip:meta.ip, last_url:'/api/rt7/camera/stream.mjpg', last_frame_ms:Date.now() });
  // V4.7C: IMPORTANT FIX. If ESP32 falls back to HTTP POST frames, still relay
  // the JPEG bytes to WebSocket viewers. Previous V4.7A/B only updated cache and
  // sent JSON metadata, so phone showed "WS connected" but received no binary JPEG.
  broadcastBinaryToViewers(latestStreamFrame);
  broadcast('stream_frame', liveStreamState);
  res.json({ ok:true, version:SERVER_VERSION, frame:{ seq:liveStreamState.seq, bytes:buf.length, time:meta.time, transport:'http_frame_relay' }, snapshot:meta });
}
app.post('/api/rt7/camera/frame', express.raw({type:['image/jpeg','image/jpg','application/octet-stream'], limit:'6mb'}), acceptStreamFrame_);
app.post('/api/rt7/camera/stream/frame', express.raw({type:['image/jpeg','image/jpg','application/octet-stream'], limit:'6mb'}), acceptStreamFrame_);
app.get('/api/rt7/camera/stream/state', (req,res)=>{ streamViewerPrune_(); res.json({ ok:true, version:SERVER_VERSION, stream:liveStreamState, snapshot:getSnapshotMeta_() }); });
app.get('/api/rt7/camera/ws/state', (req,res)=>{
  let viewers=0, uploaders=0;
  for (const ws of wss.clients) { if (ws.readyState === WebSocket.OPEN) { if (ws.rt7Role === 'viewer') viewers++; if (ws.rt7Role === 'esp32_frame_upload') uploaders++; } }
  liveStreamState.ws_viewers=viewers; liveStreamState.ws_uploaders=uploaders;
  res.json({ ok:true, version:SERVER_VERSION, ws:{ path:'/ws', viewers, uploaders }, stream:liveStreamState, snapshot:getSnapshotMeta_() });
});
app.get('/api/rt7/camera/stream/start', (req,res)=>res.json(streamMode_('fast', req)));
app.get('/api/rt7/camera/stream/stop', (req,res)=>res.json(streamMode_('idle', req)));
app.get('/api/rt7/camera/viewer/ping', (req,res)=>{
  const id = safeString(req.query.viewer_id || req.ip || clientIp(req) || 'viewer');
  const state = safeString(req.query.state || 'visible');
  if (state === 'hidden' || state === 'stop') streamViewers.delete(id);
  else streamViewers.set(id, { ts:Date.now(), ip:clientIp(req), ua:req.headers['user-agent']||'', state });
  const n = streamViewerPrune_();
  if (n > 0 && liveStreamState.fps_mode !== 'fast') return res.json(streamMode_('fast', req));
  if (n <= 0 && liveStreamState.fps_mode !== 'idle') return res.json(streamMode_('idle', req));
  res.json({ok:true, version:SERVER_VERSION, stream:liveStreamState, viewers:n});
});
app.get('/api/rt7/camera/stream.mjpg', (req,res)=>{
  res.writeHead(200, {
    'Content-Type':'multipart/x-mixed-replace; boundary=rt7frame',
    'Cache-Control':'no-cache, no-store, must-revalidate, private',
    'Connection':'keep-alive',
    'Pragma':'no-cache',
    'Expires':'0',
    'X-Accel-Buffering':'no',
    'X-RT7-Relay':'stable-5fps-repeat-latest-frame'
  });
  liveStreamState.clients = (liveStreamState.clients || 0) + 1;
  liveStreamState.cloud_mjpeg_clients = liveStreamState.clients;
  liveStreamState.cloud_relay_mode = 'stable_5fps_repeat_latest';
  liveStreamState.cloud_relay_interval_ms = 200;

  let lastFrame = null;
  let lastSeq = -1;
  let sent = 0;
  let busy = false;
  let closed = false;

  // V5.0C: Stable Cloud MJPEG relay.
  // The external phone viewer must not depend on ESP32 frame timing. Railway sends
  // a constant 5 FPS multipart MJPEG stream. If ESP32 briefly misses WS/HTTP upload,
  // repeat the most recent JPEG instead of letting the browser wait and appear frozen.
  const readFallbackFrame = () => {
    try {
      if (latestStreamFrame && Buffer.isBuffer(latestStreamFrame) && latestStreamFrame.length > 16) return latestStreamFrame;
      if (fs.existsSync(STREAM_FRAME_FILE)) return fs.readFileSync(STREAM_FRAME_FILE);
      if (fs.existsSync(SNAPSHOT_FILE)) return fs.readFileSync(SNAPSHOT_FILE);
    } catch (_) {}
    return null;
  };

  const writeOneFrame = () => {
    if (closed || busy || res.destroyed || res.writableEnded) return;
    try {
      const now = Date.now();
      const seq = liveStreamState.seq || 0;
      let frame = readFallbackFrame();
      if (frame && frame.length > 16) {
        lastFrame = Buffer.from(frame);
        lastSeq = seq;
      } else if (lastFrame) {
        frame = lastFrame;
      } else {
        return;
      }

      const repeated = (seq === lastSeq && frame === lastFrame) || (lastFrame && frame.length === lastFrame.length && seq === lastSeq);
      const head = `--rt7frame\r\nContent-Type: image/jpeg\r\nContent-Length: ${frame.length}\r\nX-RT7-Seq: ${seq}\r\nX-RT7-Repeat: ${repeated ? 1 : 0}\r\nX-RT7-Relay-Mode: stable_5fps\r\n\r\n`;
      busy = true;
      const ok = res.write(head) && res.write(frame) && res.write('\r\n');
      sent++;
      liveStreamState.cloud_mjpeg_sent = (liveStreamState.cloud_mjpeg_sent || 0) + 1;
      liveStreamState.cloud_mjpeg_last_sent_ms = now;
      liveStreamState.cloud_mjpeg_last_seq = seq;
      liveStreamState.cloud_mjpeg_last_repeat = repeated ? 1 : 0;
      if (!ok) {
        liveStreamState.cloud_mjpeg_backpressure = (liveStreamState.cloud_mjpeg_backpressure || 0) + 1;
        res.once('drain', ()=>{ busy=false; });
      } else {
        busy = false;
      }
    } catch (e) {
      closed = true;
      clearInterval(timer);
      try { res.end(); } catch(_){ }
    }
  };

  writeOneFrame();
  const timer = setInterval(writeOneFrame, 200); // fixed 5 FPS cloud output
  req.on('close', ()=>{
    closed = true;
    clearInterval(timer);
    liveStreamState.clients = Math.max(0, (liveStreamState.clients || 1)-1);
    liveStreamState.cloud_mjpeg_clients = liveStreamState.clients;
    liveStreamState.cloud_mjpeg_last_client_frames = sent;
  });
});

app.get('/api/rt7/camera/latest.jpg', (req,res)=>{ ensureDataDir(); if (!fs.existsSync(SNAPSHOT_FILE)) return res.status(404).json({ok:false,error:'NO_SNAPSHOT'}); res.type('image/jpeg').send(fs.readFileSync(SNAPSHOT_FILE)); });
app.get('/api/rt7/camera/state', (req,res)=>{ const snap=getSnapshotMeta_(); res.json({ ok:true, version:SERVER_VERSION, snapshot:snap, latest_url: snap ? '/api/rt7/camera/latest.jpg' : '', test_page:'/rt7_snapshot_bridge_test' }); });
app.post('/api/rt7/camera/clear', (req,res)=>{ ensureDataDir(); if (fs.existsSync(SNAPSHOT_FILE)) fs.unlinkSync(SNAPSHOT_FILE); cloudState.last_snapshot=null; const ev=appendEvent({type:'snapshot_clear', message:'Snapshot cleared'}); broadcast('snapshot_clear', ev); res.json({ok:true, event:ev}); });

// Phase8C motion configuration: stored in cloud, ESP32 may poll it later.
let motionConfig = { enabled:false, sensitivity:5, updated_at:null };
app.get('/api/rt7/phase8c/esp_motion/enable', (req,res)=>{ motionConfig.enabled=true; motionConfig.updated_at=nowIso(); res.json({ok:true, motionConfig}); });
app.get('/api/rt7/phase8c/esp_motion/disable', (req,res)=>{ motionConfig.enabled=false; motionConfig.updated_at=nowIso(); res.json({ok:true, motionConfig}); });
app.get('/api/rt7/phase8c/esp_motion/config', (req,res)=>res.json({ok:true, motionConfig}));
app.get('/api/rt7/phase8c/esp_motion/status', (req,res)=>res.json({ok:true, motionConfig, last_motion: readEvents(50).reverse().find(e=>e.type==='motion') || null}));
app.get('/api/rt7/return_fix2/enable', (req,res)=>{ cloudState.ai_enabled=true; res.json({ok:true, ai_enabled:true}); });
app.post('/api/rt7/return_fix2/enable', (req,res)=>{ cloudState.ai_enabled=true; res.json({ok:true, ai_enabled:true}); });
app.get('/api/rt7/return_fix2/disable', (req,res)=>{ cloudState.ai_enabled=false; res.json({ok:true, ai_enabled:false}); });

async function openAiChat(messages, max_tokens=360) {
  const key = safeString(process.env.OPENAI_API_KEY).replace(/^Bearer\s+/i,'').trim();
  if (!key) throw new Error('OPENAI_API_KEY missing');
  const model = safeString(process.env.OPENAI_CHAT_MODEL || 'gpt-4o-mini').trim();
  const r = await fetch('https://api.openai.com/v1/chat/completions', { method:'POST', headers:{ Authorization:'Bearer '+key, 'Content-Type':'application/json' }, body:JSON.stringify({ model, temperature:0.25, max_tokens, messages }) });
  const body = await r.json().catch(()=>({}));
  if (!r.ok) throw new Error(body?.error?.message || ('OpenAI HTTP '+r.status));
  return safeString(body?.choices?.[0]?.message?.content).trim();
}
async function analyzeLatestSnapshot(question) {
  ensureDataDir();
  if (!fs.existsSync(SNAPSHOT_FILE)) return { ok:false, mode:'VISION', error:'NO_CLOUD_SNAPSHOT', answer:'雲端尚未收到 ESP32 上傳的照片。請先讓 ESP32 POST /api/rt7/camera/snapshot。' };
  const b64 = fs.readFileSync(SNAPSHOT_FILE).toString('base64');
  const answer = await openAiChat([{ role:'user', content:[ {type:'text', text: question || '請用繁體中文簡短描述門口畫面，並判斷是否有人臉或可疑狀況。'}, {type:'image_url', image_url:{url:'data:image/jpeg;base64,'+b64} } ] }], 360);
  cloudState.last_vision = { ok:true, question, answer, time:nowIso(), snapshot:cloudState.last_snapshot };
  appendEvent({ type:'vision_qa', question, answer:answer.slice(0,200), message:'vision qa completed' });
  return { ok:true, mode:'VISION', question, answer, snapshot:cloudState.last_snapshot };
}

async function transcribeAudioB64(audio_b64, mime) {
  const key = safeString(process.env.OPENAI_API_KEY).replace(/^Bearer\s+/i,'').trim();
  if (!key) throw new Error('OPENAI_API_KEY missing');
  let b64 = safeString(audio_b64); if (b64.includes(',')) b64 = b64.split(',').pop();
  const buf = Buffer.from(b64, 'base64');
  if (buf.length < 800) throw new Error('AUDIO_TOO_SMALL');
  const blob = new Blob([buf], { type: mime || 'audio/webm' });
  const fd = new FormData(); fd.append('model','whisper-1'); fd.append('language','zh'); fd.append('response_format','json'); fd.append('file', blob, 'rt7_voice.webm');
  const r = await fetch('https://api.openai.com/v1/audio/transcriptions', { method:'POST', headers:{ Authorization:'Bearer '+key }, body:fd });
  const body = await r.json().catch(()=>({}));
  if (!r.ok) throw new Error(body?.error?.message || ('Whisper HTTP '+r.status));
  return safeString(body.text).trim();
}

async function handleVisionQa(req,res) {
  try {
    const q = safeString(req.query.q || req.query.question || req.body?.q || req.body?.question || '請問鏡頭目前看到什麼？');
    const out = await analyzeLatestSnapshot(q);
    broadcast('vision_qa', { ok:out.ok, answer:out.answer, question:q, time:nowIso() });
    res.json(Object.assign({ version:SERVER_VERSION }, out));
  }
  catch(e) { res.status(200).json({ok:false, version:SERVER_VERSION, mode:'VISION', error:String(e.message||e), answer:'雲端 Vision 分析失敗。請確認 Railway 已設定 OPENAI_API_KEY，且已先上傳 Snapshot。'}); }
}
app.get('/api/rt7/phase9a/vision_qa', handleVisionQa);
app.post('/api/rt7/phase9a/vision_qa', handleVisionQa);
app.get('/api/rt7/phase9b/vision_qa', handleVisionQa);
app.post('/api/rt7/phase9b/vision_qa', handleVisionQa);
app.get('/api/rt7/phase9g/vision_qa', handleVisionQa);
app.post('/api/rt7/phase9g/vision_qa', handleVisionQa);
app.get('/api/rt7/phase9i/vision_qa', handleVisionQa);
app.post('/api/rt7/phase9i/vision_qa', handleVisionQa);
app.get('/api/rt7/vision/qa', handleVisionQa);
app.post('/api/rt7/vision/qa', handleVisionQa);
app.get('/api/rt7/phase9d/vision_qa_ping', (req,res)=>res.json({ok:true, version:SERVER_VERSION, openai_key:!!safeString(process.env.OPENAI_API_KEY).trim(), latest_snapshot:getSnapshotMeta_(), last_vision:cloudState.last_vision}));
app.get('/api/rt7/vision/state', (req,res)=>res.json({ok:true, version:SERVER_VERSION, openai_key:!!safeString(process.env.OPENAI_API_KEY).trim(), latest_snapshot:getSnapshotMeta_(), last_vision:cloudState.last_vision}));
app.post('/api/rt7/phase9j/voice_vision', async (req,res)=>{
  const started = Date.now();
  try {
    const mode = safeString(req.body?.mode || 'auto').toLowerCase();
    const text = req.body?.text ? safeString(req.body.text).trim() : await transcribeAudioB64(req.body?.audio_b64 || '', req.body?.mime || 'audio/webm');
    if (!text) return res.json({ok:false, error:'NO_TRANSCRIPT', answer:'沒有辨識到文字。'});
    const visionWords = ['鏡頭','畫面','看到','看見','門口','人臉','有人','誰在','照片','影像','辨識'];
    const isVision = mode === 'vision' || (mode !== 'chat' && visionWords.some(w=>text.includes(w)));
    let result;
    if (isVision) result = await analyzeLatestSnapshot(text);
    else {
      const answer = await openAiChat([{role:'system', content:'你是 RT7 AI 語音助理。請用繁體中文、口語、簡潔回答。'}, {role:'user', content:text}], 420);
      result = { ok:true, mode:'CHAT', text, answer };
    }
    cloudState.last_voice = Object.assign({ time:nowIso(), ms:Date.now()-started }, result);
    res.json(Object.assign({ version:SERVER_VERSION, voice_ms:Date.now()-started }, result, { text }));
  } catch(e) { res.status(200).json({ok:false, version:SERVER_VERSION, error:String(e.message||e), answer:'雲端語音/影像 AI 處理失敗。'}); }
});

// Door open queue: phone/Railway queues command; ESP32 actively polls and ACKs.
let pendingCommands = [];
let doorOpenQueueState = { ok:true, queued:0, acked:0, last:null, last_ack:null };
function queueCommand(cmd) {
  const c = Object.assign({ id:'cmd_'+Date.now()+'_'+Math.floor(Math.random()*1000), time:nowIso(), status:'pending' }, cmd);
  pendingCommands.push(c);
  pendingCommands = pendingCommands.slice(-50);
  doorOpenQueueState.queued += 1;
  doorOpenQueueState.last = c;
  broadcast('command', c);
  appendEvent({ type:'command', command:c.command, id:c.id, device_id:c.device_id, message:c.message||c.command });
  return c;
}
function normalizeDoorCommandDeviceId_(id) {
  const raw = safeString(id || '');
  const low = raw.toLowerCase();
  // UI device list historically uses #1, while ESP32 V4.4 polls with rt7-esp32-s3-cam-01.
  // Normalize the primary camera so Railway Queue and ESP32 polling use the same device_id.
  if (!raw || raw === '#1' || raw === '1' || low.includes('rt7 esp32-s3-cam') || low.includes('esp32-s3-cam')) return 'rt7-esp32-s3-cam-01';
  return raw;
}
function commandMatchesDevice_(cmd, id) {
  const pollId = normalizeDoorCommandDeviceId_(id);
  const cmdId = normalizeDoorCommandDeviceId_(cmd?.device_id || '');
  return !pollId || !cmdId || cmdId === pollId;
}
function enqueueDoorOpen(req, res, endpointName) {
  const dev = getCurrentDevice(req);
  const requestedDeviceId = safeString(req.query.device_id || req.query.device || dev.id || '#1') || '#1';
  const deviceId = normalizeDoorCommandDeviceId_(requestedDeviceId);
  const cmd = queueCommand({
    command:'door_open',
    action:'door_open',
    device_id:deviceId,
    requested_device_id:requestedDeviceId,
    endpoint:endpointName || 'door_open_queue',
    pulse_ms:Number(req.query.pulse_ms || 800),
    message:'雲端開門命令已排入佇列，等待 ESP32 輪詢'
  });
  cloudState.last_door_open = cmd;
  res.json({ ok:true, mode:'cloud_command_queue', command:cmd, requested_device_id:requestedDeviceId, normalized_device_id:deviceId, state:doorOpenQueueState, note:'ESP32 輪詢 /api/rt7/device/commands/next?device_id='+deviceId+' 後執行開門並 ACK' });
}
app.get('/api/rt7/phase9l/door/open', (req,res)=>enqueueDoorOpen(req,res,'phase9l'));
app.post('/api/rt7/phase9l/door/open', (req,res)=>enqueueDoorOpen(req,res,'phase9l_post'));
app.get('/api/rt7/door/open', (req,res)=>enqueueDoorOpen(req,res,'rt7_door_open'));
app.post('/api/rt7/door/open', (req,res)=>enqueueDoorOpen(req,res,'rt7_door_open_post'));
app.get('/api/door/open', (req,res)=>enqueueDoorOpen(req,res,'compat_api_door_open'));
app.get('/api/rt7/door/open/state', (req,res)=>res.json({ ok:true, state:doorOpenQueueState, pending:pendingCommands }));
app.get('/api/rt7/face/command_debug', (req,res)=>res.json({ ok:true, version:SERVER_VERSION, pending:pendingCommands.filter(c=>c.command==='face_snapshot_now'||c.action==='face_snapshot_now'||c.priority==='face_snapshot'), all_pending:pendingCommands.length, state:doorOpenQueueState }));
app.get('/api/rt7/device/commands', (req,res)=>{ const id=normalizeDoorCommandDeviceId_(req.query.device_id||req.query.device||''); const list=id?pendingCommands.filter(c=>commandMatchesDevice_(c,id)):pendingCommands; res.json({ok:true, device_id:id, commands:list, count:list.length, state:doorOpenQueueState}); });
app.get('/api/rt7/device/commands/next', (req,res)=>{
  const id=normalizeDoorCommandDeviceId_(req.query.device_id||req.query.device||'');
  const matches=pendingCommands.filter(c=>commandMatchesDevice_(c,id));
  // V5.2D: face snapshot has priority, otherwise older queued items can hide it.
  const faceCmd=matches.find(c=>c && (c.command==='face_snapshot_now' || c.action==='face_snapshot_now' || c.priority==='face_snapshot'));
  const cmd=faceCmd || matches[0] || null;
  res.json({ok:true, version:SERVER_VERSION, device_id:id, command:cmd, has_command:!!cmd, pending:pendingCommands.length, matching:matches.length, face_priority:!!faceCmd, state:doorOpenQueueState});
});
function ackCommand(req,res){ const id=safeString(req.body?.id||req.query.id); const status=safeString(req.body?.status||req.query.status||'done'); const idx=pendingCommands.findIndex(c=>c.id===id); let cmd=null; if(idx>=0){cmd=pendingCommands[idx]; pendingCommands.splice(idx,1);} doorOpenQueueState.acked+=1; doorOpenQueueState.last_ack={id, status, time:nowIso(), found:!!cmd, command:cmd}; appendEvent({type:'command_ack', id, status, found:!!cmd}); res.json({ok:true, id, status, found:!!cmd, pending:pendingCommands.length, state:doorOpenQueueState}); }
app.get('/api/rt7/device/commands/ack', ackCommand);
app.post('/api/rt7/device/commands/ack', ackCommand);

app.get('/rt7_cloud_phase10_no_nodered', (req,res)=>{
  res.type('html').send(htmlShell('RT7 Phase10 Cloud No Node-RED', `${baseCss}
<style>.phone{max-width:430px;margin:0 auto;background:#fff;min-height:100vh}.videoBox{background:#000;aspect-ratio:4/3;position:relative;display:flex;align-items:center;justify-content:center;color:#cbd5e1;text-align:center;font-weight:900}.videoBox img{width:100%;height:100%;object-fit:cover}.doorAlert{display:none;background:#fff7ed;border:1px solid #fed7aa;color:#9a3412;border-radius:12px;padding:12px;margin:10px;font-size:22px;font-weight:900;text-align:center}.rowbtn{display:grid;grid-template-columns:1fr 1fr;gap:8px}.small{font-size:12px;color:#64748b}.mic{width:118px;height:118px;border-radius:50%;border:3px solid #cbd5e1;background:#eef2f7;font-size:62px}.pill{display:inline-block;border-radius:999px;padding:4px 9px;background:#e2e8f0;font-weight:900}</style>
<div class="phone">
<header class="top"><h1>RT7 PHASE10</h1><p>Railway 雲端影像 / 對講 / AI 門禁（無 Node-RED）</p></header>
<div class="wrap">
<section class="card"><b>目前設備</b><select id="deviceSel"></select><p class="small">區網 IP 在 Railway 通常無法被反向連線；建議 ESP32 主動上傳 snapshot / doorbell / commands polling。</p></section>
<section class="videoBox"><img id="snap" style="display:none"><div id="empty">等待 ESP32 上傳照片<br><span class="small">POST /api/rt7/camera/snapshot</span></div></section>
<div id="doorAlert" class="doorAlert">🔔 有人按門鈴</div>
<section class="card"><div>狀態：<span class="pill" id="status">ready</span></div><div>回答：<b id="answer">雲端待機中</b></div></section>
<section class="card rowbtn"><button class="btn green" onclick="refreshSnap()">更新影像</button><button class="btn" onclick="askVision()">問鏡頭</button><button class="btn red" onclick="openDoor()">開門</button><button class="btn gray" onclick="testDoorbell()">測試門鈴</button><button class="btn" onclick="enableAudio()">啟用提示音</button><button class="btn gray" onclick="loadState(true)">更新狀態</button></section>
<section class="card" style="text-align:center"><button class="mic" onclick="voiceText()">🎙️</button><p class="small">第一版先支援文字測試；手機錄音可 POST /api/rt7/phase9j/voice_vision。</p></section>
<pre class="status" id="log">ready</pre>
</div></div>
<script>
let DEVICES=[], audioCtx=null, audioOK=false, lastCount=null; const $=id=>document.getElementById(id);
function log(o){$('log').textContent='['+new Date().toLocaleTimeString()+'] '+(typeof o==='string'?o:JSON.stringify(o,null,2))+'\n'+$('log').textContent}
async function j(url,opt){const r=await fetch(url+(url.includes('?')?'&':'?')+'_='+Date.now(),Object.assign({cache:'no-store'},opt||{}));const t=await r.text();try{return JSON.parse(t)}catch(e){return{ok:r.ok,raw:t}}}
function beep(f,d,t){if(!audioCtx)return;const o=audioCtx.createOscillator(),g=audioCtx.createGain();o.frequency.value=f;g.gain.value=.18;o.connect(g);g.connect(audioCtx.destination);o.start(audioCtx.currentTime+t);o.stop(audioCtx.currentTime+t+d)}
function ding(){if(audioOK){beep(880,.18,0);beep(660,.22,.26)}}
async function enableAudio(){audioCtx=audioCtx||new(window.AudioContext||window.webkitAudioContext)();await audioCtx.resume();audioOK=true;$('answer').textContent='提示音已啟用';ding()}
async function loadDevices(){const d=await j('/api/rt7/devices/list');DEVICES=d.devices||[];$('deviceSel').innerHTML=(DEVICES.length?DEVICES:[{id:'#1',name:'RT7'}]).map(x=>'<option value="'+(x.id||'')+'">'+(x.id||'')+' / '+(x.name||'')+(x.ip?' / '+x.ip:'')+'</option>').join('')}
async function refreshSnap(){const s=await j('/api/rt7/camera/state');log(s); if(s.latest_url){$('snap').src=s.latest_url+'?_='+Date.now();$('snap').style.display='block';$('empty').style.display='none';$('status').textContent='snapshot';}else{$('answer').textContent='尚無雲端照片'}}
async function loadState(manual){const s=await j('/api/rt7/doorbell/state');log(s); const c=Number(s.state?.count||0), last=s.state?.last||{}; if(lastCount===null)lastCount=c; else if(c>lastCount){$('doorAlert').style.display='block';$('answer').textContent='收到門鈴';ding();setTimeout(()=>$('doorAlert').style.display='none',5000)} lastCount=c; if(manual&&last.message)$('answer').textContent=last.message;}
async function testDoorbell(){log(await j('/api/test/doorbell'));loadState(false)}
async function openDoor(){const r=await j('/api/rt7/phase9l/door/open?device_id='+encodeURIComponent($('deviceSel').value));log(r);$('answer').textContent=r.note||r.message||'開門命令已送出'}
async function askVision(){const q=prompt('要問鏡頭什麼？','門口目前看到什麼？')||''; if(!q)return; $('answer').textContent='Vision 分析中...'; const r=await j('/api/rt7/phase9i/vision_qa?q='+encodeURIComponent(q));log(r);$('answer').textContent=r.answer||r.error||'無回應'}
async function voiceText(){const t=prompt('輸入要測試的語音文字','請問鏡頭看到什麼？')||''; if(!t)return; const r=await j('/api/rt7/phase9j/voice_vision',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({text:t,mode:'auto'})});log(r);$('answer').textContent=r.answer||r.error||'無回應'}
function ws(){try{const w=new WebSocket((location.protocol==='https:'?'wss://':'ws://')+location.host+'/ws');w.onmessage=e=>{try{const m=JSON.parse(e.data); if(['doorbell','snapshot','command'].includes(m.type)){log(m); if(m.type==='doorbell'){loadState(false);ding()} if(m.type==='snapshot')refreshSnap();}}catch(_){}};w.onclose=()=>setTimeout(ws,3000)}catch(e){}}
loadDevices().then(()=>{refreshSnap();loadState(true)});setInterval(()=>loadState(false),2500);ws();
</script>`));
});
app.get('/rt7_independent_full_video_intercom', (req,res)=>res.redirect(307,'/rt7_cloud_phase10_no_nodered'));
app.get('/rt7_face_guard', (req,res)=>res.redirect(307,'/rt7_cloud_phase10_no_nodered'));
app.get('/rt7_admin_home', (req,res)=>res.redirect(307,'/rt7_cloud_admin'));
app.get('/rt7_device_manager', (req,res)=>res.redirect(307,'/rt7_cloud_admin'));
app.get('/rt7_log_viewer', (req,res)=>res.redirect(307,'/rt7_cloud_admin'));


// -----------------------------------------------------------------------------
// V4.1 Maintenance Mapping API
// Purpose: keep Railway Node.js and original Node-RED flow comparable.
// -----------------------------------------------------------------------------
const NODE_RED_MAPPING = [
  { group:'00 Core', status:'done', nodered:'GET /rt7_independent_full_video_intercom', railway:'GET /rt7_cloud_phase10_no_nodered', test:'Open phone page; verify original UI style and buttons' },
  { group:'01 Doorbell', status:'done', nodered:'POST /api/rt7/phase9n/doorbell/event, POST /api/rt7/doorbell/ring, GET /api/rt7/doorbell/state', railway:'same API names retained', test:'ESP32 POST -> phone UI shows event and dingdong plays' },
  { group:'02 Event Log', status:'done', nodered:'GET /api/rt7/events/latest, GET /api/rt7/events/clear, /rt7_event_log', railway:'same API names retained; stored in data/rt7_event_log.jsonl', test:'GET latest, clear, then trigger doorbell' },
  { group:'03 Device Manager', status:'done', nodered:'GET /api/rt7/device/state, POST /api/rt7/device/set, /rt7_device_manager', railway:'same API names retained; stored in data/rt7_devices.json', test:'save device IP/name and reload admin page' },
  { group:'04 Snapshot Bridge', status:'done-v4.2', nodered:'ESP32 /api/camera/snapshot via Node-RED local proxy', railway:'POST /api/rt7/camera/snapshot; POST /api/rt7/camera/snapshot_json; GET /api/rt7/camera/latest.jpg; GET /api/rt7/camera/state; GET /rt7_snapshot_bridge_test', test:'ESP32 actively uploads JPEG/base64; phone page refreshes latest image; clear endpoint works' },
  { group:'04B Original UI Snapshot', status:'done-v4.3', nodered:'Original phone UI camera block / Node-RED image refresh', railway:'GET /rt7_cloud_original_ui_doorbell now displays /api/rt7/camera/latest.jpg and auto-refreshes on snapshot WebSocket event', test:'Open original UI after ESP32 snapshot POST; verify image appears in black video area' },
  { group:'04C Live Stream Bridge', status:'done-v4.7e-ws-upload-native-mjpeg-7fps', nodered:'Original Node-RED MJPEG / live camera view', railway:'ESP32 WebSocket binary JPEG upload to /ws; HTTP POST /api/rt7/camera/frame fallback; GET /api/rt7/camera/stream.mjpg native browser MJPEG output; /rt7_cloud_original_ui_doorbell uses native MJPEG live stream', test:'ESP32 targets about 10 FPS via WebSocket upload; phone UI uses native MJPEG for Android Chrome compatibility; Snapshot remains fallback' },
  { group:'05 Vision QA', status:'partial', nodered:'GET /api/rt7/phase9i/vision_qa', railway:'GET /api/rt7/phase9i/vision_qa uses latest uploaded snapshot + OpenAI if OPENAI_API_KEY exists', test:'Upload snapshot, ask question, verify answer' },
  { group:'06 Voice Vision Router', status:'partial', nodered:'POST /api/rt7/phase9j/voice_vision', railway:'POST /api/rt7/phase9j/voice_vision text-mode scaffold; audio upload reserved', test:'POST {text:"請問鏡頭看到什麼"}' },
  { group:'07 Door Open Queue', status:'done-v4.4', nodered:'GET /api/rt7/phase9l/door/open direct local ESP32 request', railway:'GET /api/rt7/phase9l/door/open queues command; ESP32 polls /api/rt7/device/commands', test:'GET door/open then GET device/commands' },
  { group:'08 Phase6C3 Plugin', status:'stub', nodered:'phase6c3_plugin ping/plugins/motion/face endpoints', railway:'compatible endpoints kept; advanced face cache/enroll needs next incremental port', test:'GET ping/plugins/state; do not enable full face match yet' },
  { group:'09 Intercom Audio', status:'stub', nodered:'/api/ind_full/audio/* local proxy to ESP32 audio endpoints', railway:'/api/ind_full/audio/* returns compatibility JSON / queue scaffold', test:'Call begin/end endpoints; later add WebSocket PCM bridge one step at a time' }
];
app.get('/api/rt7/mapping', (req,res)=>res.json({ ok:true, version:SERVER_VERSION, mapping:NODE_RED_MAPPING }));
app.get('/api/rt7/mapping/status', (req,res)=>{
  const count = NODE_RED_MAPPING.reduce((a,x)=>{ a[x.status]=(a[x.status]||0)+1; return a; },{});
  res.json({ ok:true, version:SERVER_VERSION, count, next_recommended:'V4.4 Door Open Queue only after V4.3 original UI snapshot passes' });
});
app.get('/rt7_mapping', (req,res)=>{
  const rows = NODE_RED_MAPPING.map(x=>`<tr><td>${x.group}</td><td><b>${x.status}</b></td><td><code>${x.nodered}</code></td><td><code>${x.railway}</code></td><td>${x.test}</td></tr>`).join('');
  res.type('html').send(htmlShell('RT7 Node-RED / Railway Mapping', `${baseCss}<div class="wrap"><h1>RT7 Node-RED / Railway API 對照表</h1><p>版本：${SERVER_VERSION}</p><p><a class="btn" href="/rt7_cloud_phase10_no_nodered">回手機頁</a> <a class="btn gray" href="/api/rt7/mapping">JSON</a></p><table border="1" cellspacing="0" cellpadding="8" style="width:100%;border-collapse:collapse;background:#fff"><thead><tr><th>功能</th><th>狀態</th><th>Node-RED 原始路由</th><th>Railway 對應 API</th><th>測試</th></tr></thead><tbody>${rows}</tbody></table></div>`));
});



// -----------------------------------------------------------------------------
// V4.8 Stream Compare Test: LAN direct stream vs Railway cloud stream.
// Goal: keep product target no Node-RED / no Tailscale, but measure whether LAN
// direct ESP32 MJPEG is smooth before comparing with cloud relay.
// -----------------------------------------------------------------------------
app.get('/rt7_stream_compare_test', (req, res) => {
  res.type('html').send(`<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no"><title>RT7 V4.8 Stream Compare</title>
<style>
body{font-family:system-ui,-apple-system,"Noto Sans TC","Microsoft JhengHei",Arial,sans-serif;margin:0;background:#f3f6f8;color:#17262a} .wrap{max-width:980px;margin:0 auto;padding:14px} .top{background:#0d2a30;color:#fff;padding:18px;text-align:center;font-weight:900} .card{background:#fff;border:1px solid #d7dee5;border-radius:14px;padding:14px;margin:12px 0;box-shadow:0 2px 10px rgba(0,0,0,.05)} button{border:0;border-radius:12px;padding:12px 14px;margin:4px;color:#fff;font-weight:900;font-size:16px} .blue{background:#1583d8}.green{background:#16a34a}.red{background:#dc2626}.gray{background:#475569}.orange{background:#d97706} select,input{font-size:16px;padding:10px;border:1px solid #9aa8b4;border-radius:10px;width:100%;box-sizing:border-box;margin:5px 0}.grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}.video{background:#000;min-height:260px;display:flex;align-items:center;justify-content:center;border-radius:12px;overflow:hidden}.video img{width:100%;height:100%;min-height:260px;object-fit:contain}.label{font-weight:900;margin:8px 0;color:#7a2a19}.small{font-size:13px;color:#64748b;line-height:1.6} pre{white-space:pre-wrap;background:#0b1220;color:#cbd5e1;border-radius:12px;padding:10px;max-height:260px;overflow:auto}.ok{color:#16a34a;font-weight:900}.warn{color:#d97706;font-weight:900}@media(max-width:720px){.grid{grid-template-columns:1fr}.video{min-height:220px}.video img{min-height:220px}}
</style></head><body><div class="top">RT7 V4.8 內網 / 外網影像串流比較測試</div><div class="wrap">
<div class="card"><b>測試目的</b><div class="small">不使用 Node-RED、不使用 Tailscale。先測手機與 ESP32 同 Wi-Fi 時，直接讀 ESP32 <code>/api/camera/stream</code> 是否順暢；再比較 Railway 雲端轉發 <code>/api/rt7/camera/stream.mjpg</code>。若內網順、外網慢，代表瓶頸在雲端轉發路徑，不是 ESP32 攝影機本身。</div></div>
<div class="card"><label>選擇/輸入 ESP32 IP</label><select id="deviceSel"></select><input id="espIp" placeholder="例如 192.168.0.179"><div><button class="blue" onclick="loadDevices()">重新讀取設備</button><button class="green" onclick="saveIp()">套用 IP</button><button class="gray" onclick="loadState()">讀取狀態</button></div></div>
<div class="card"><div><button class="orange" onclick="startLan()">1. 內網直連 ESP32 串流</button><button class="green" onclick="startCloud()">2. Railway 雲端串流</button><button class="blue" onclick="startBoth()">同時比較</button><button class="red" onclick="stopAll()">停止</button></div><div class="small">手機若不在同一 Wi-Fi，內網直連會失敗，這是正常。一般使用者外網仍走 Railway。</div></div>
<div class="grid"><div class="card"><div class="label">內網直連 ESP32 /api/camera/stream</div><div class="video"><img id="lanImg"><span id="lanEmpty" style="color:#fff">尚未開始</span></div><div id="lanInfo" class="small"></div></div><div class="card"><div class="label">外網 / Railway /api/rt7/camera/stream.mjpg</div><div class="video"><img id="cloudImg"><span id="cloudEmpty" style="color:#fff">尚未開始</span></div><div id="cloudInfo" class="small"></div></div></div>
<div class="card"><b>判讀方式</b><div class="small">A. 內網直連順、Railway 慢：ESP32 攝影機正常，雲端 relay 是瓶頸。<br>B. 內網也慢：需回頭調 ESP32 camera frame size / jpeg quality / Wi-Fi。<br>C. 內網不能開但 Railway 可開：手機不在同 Wi-Fi 或瀏覽器擋 HTTP 私網影像。</div></div>
<div class="card"><b>狀態</b><pre id="log">ready</pre></div>
</div><script>
function $(id){return document.getElementById(id)}
function log(x){$('log').textContent=(typeof x==='string'?x:JSON.stringify(x,null,2))+'\n\n'+$('log').textContent.slice(0,4000)}
async function j(u){const r=await fetch(u+(u.includes('?')?'&':'?')+'_='+Date.now(),{cache:'no-store'});const t=await r.text();try{return JSON.parse(t)}catch(e){return{ok:r.ok,raw:t}}}
let espIp='192.168.0.179';
async function loadDevices(){const d=await j('/api/devices');const devs=d.devices||[];$('deviceSel').innerHTML=devs.map(x=>'<option value="'+(x.ip||'')+'">'+(x.id||'')+' / '+(x.name||'')+' / '+(x.ip||'')+'</option>').join(''); if(devs[0]&&devs[0].ip){espIp=devs[0].ip;$('espIp').value=espIp;} log(d)}
function saveIp(){espIp=($('espIp').value||$('deviceSel').value||espIp).trim().replace(/^https?:\/\//,'').replace(/\/.*/,'');$('espIp').value=espIp;log('ESP32 IP = '+espIp)}
$('deviceSel').addEventListener('change',()=>{if($('deviceSel').value){$('espIp').value=$('deviceSel').value;saveIp();}})
function startLan(){saveIp();$('lanEmpty').style.display='none';$('lanImg').onerror=()=>{$('lanInfo').innerHTML='<span class="warn">內網直連失敗：請確認手機與 ESP32 同 Wi-Fi，或瀏覽器是否擋 HTTP 私網影像。</span>';};$('lanImg').onload=()=>{$('lanInfo').innerHTML='<span class="ok">內網直連已啟動。</span> 這一路徑不經 Railway。';};$('lanImg').src='http://'+espIp+'/api/camera/stream?_lan='+Date.now();$('lanInfo').textContent='連線中： http://'+espIp+'/api/camera/stream';}
async function startCloud(){await j('/api/rt7/camera/stream/start');$('cloudEmpty').style.display='none';$('cloudImg').onerror=()=>{$('cloudInfo').innerHTML='<span class="warn">Railway MJPEG 載入失敗</span>';};$('cloudImg').onload=()=>{$('cloudInfo').innerHTML='<span class="ok">Railway 雲端串流已啟動。</span>';};$('cloudImg').src='/api/rt7/camera/stream.mjpg?_cloud='+Date.now();$('cloudInfo').textContent='連線中：/api/rt7/camera/stream.mjpg';}
function startBoth(){startLan();startCloud();}
async function stopAll(){$('lanImg').removeAttribute('src');$('lanImg').src='';$('cloudImg').removeAttribute('src');$('cloudImg').src='';$('lanEmpty').style.display='block';$('cloudEmpty').style.display='block';await j('/api/rt7/camera/stream/stop');log('stopped')}
async function loadState(){const s=await j('/api/rt7/camera/stream/state');log(s)}
loadDevices().then(loadState);setInterval(loadState,5000);
</script></body></html>`);
});



// -----------------------------------------------------------------------------
// V4.8F Auto LAN/Cloud Stream Test page
// Production idea: no Node-RED, no Tailscale.  Browser first tries LAN direct
// ESP32 MJPEG, then falls back to Railway cloud MJPEG if LAN is unavailable.
// -----------------------------------------------------------------------------
app.get('/rt7_auto_stream_test', (req, res) => {
  res.type('html').send(`<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no"><title>RT7 V4.8F3 Auto LAN/Cloud Stream</title>
<style>body{font-family:system-ui,-apple-system,"Noto Sans TC","Microsoft JhengHei",Arial,sans-serif;margin:0;background:#f5f7fb;color:#17262a}.wrap{max-width:720px;margin:0 auto;padding:14px}.top{background:#0d2a30;color:#fff;padding:18px;text-align:center;font-weight:900}.card{background:#fff;border:1px solid #d7dee5;border-radius:14px;padding:14px;margin:12px 0}.video{background:#000;aspect-ratio:4/3;border-radius:14px;overflow:hidden;display:flex;align-items:center;justify-content:center;color:#fff}.video img{width:100%;height:100%;object-fit:contain;pointer-events:none}.btn{width:100%;border:0;border-radius:12px;padding:14px;margin:6px 0;color:#fff;font-weight:900;font-size:18px;background:#0b84d8}.red{background:#dc2626}.green{background:#16a34a}.orange{background:#d97706}input{width:100%;box-sizing:border-box;padding:12px;border:1px solid #9aa8b4;border-radius:10px;font-size:18px}.badge{display:inline-block;border-radius:999px;padding:5px 10px;color:#fff;background:#64748b;font-weight:900}.lan{background:#16a34a}.cloud{background:#d97706}.small{font-size:13px;color:#64748b;line-height:1.55}pre{white-space:pre-wrap;background:#0b1220;color:#d8f2ff;border-radius:12px;padding:10px;max-height:260px;overflow:auto}</style></head><body><div class="top">RT7 V4.8F3 自動內網/雲端串流</div><div class="wrap">
<div class="card"><b>ESP32 IP</b><input id="ip" value="192.168.0.179"><div class="small">在家同 Wi-Fi 會自動直連 ESP32；外網或失敗時自動切 Railway 雲端。</div></div>
<div class="card"><button class="btn green" id="startBtn">開始影像（自動判斷）</button><button class="btn red" id="stopBtn">停止影像</button><p>目前模式：<span id="mode" class="badge">AUTO</span></p></div>
<div class="video"><img id="img"><span id="empty">尚未開始</span></div>
<div class="card"><b>說明</b><div class="small">LAN = 手機直接讀 ESP32 <code>/api/camera/stream</code>，流暢。CLOUD = Railway <code>/api/rt7/camera/stream.mjpg</code>，遠端可用但 FPS 較低。</div></div>
<pre id="log">ready</pre></div><script>
const $=id=>document.getElementById(id); let wanted=false;
function log(s){$('log').textContent='['+new Date().toLocaleTimeString()+'] '+s+'\n'+$('log').textContent.slice(0,3000)}
async function j(u){const r=await fetch(u+(u.includes('?')?'&':'?')+'_='+Date.now(),{cache:'no-store'});const t=await r.text();try{return JSON.parse(t)}catch(e){return{ok:r.ok,raw:t}}}
function setMode(m){$('mode').textContent=m;$('mode').className='badge '+(m==='LAN'?'lan':m==='CLOUD'?'cloud':'')}
function lanUrl(){return 'http://'+$('ip').value.trim().replace(/^https?:\/\//,'').replace(/\/.*$/,'')+'/api/camera/stream'}
function probe(url,ms){return new Promise(resolve=>{const im=new Image();let done=false;const fin=ok=>{if(done)return;done=true;try{im.src=''}catch(e){}resolve(ok)};im.onload=()=>fin(true);im.onerror=()=>fin(false);setTimeout(()=>fin(false),ms||2600);im.src=url+'?_probe='+Date.now();});}
async function cloud(){await j('/api/rt7/camera/stream/start');$('empty').style.display='none';$('img').onerror=()=>log('Cloud MJPEG error');$('img').src='/api/rt7/camera/stream.mjpg?_cloud='+Date.now();setMode('CLOUD');log('CLOUD mode: Railway remote stream');}
async function lan(url){$('empty').style.display='none';$('img').onerror=()=>{log('LAN lost -> CLOUD fallback');cloud();};$('img').src=url+'?_lan='+Date.now();setMode('LAN');log('LAN mode: direct ESP32 stream '+url);}
async function start(){wanted=true;setMode('AUTO');log('probe LAN...');const u=lanUrl();if(await probe(u,2600)) await lan(u); else await cloud();}
async function stop(){wanted=false;$('img').removeAttribute('src');$('img').src='';$('empty').style.display='block';setMode('AUTO');await j('/api/rt7/camera/stream/stop');log('stopped')}
$('startBtn').addEventListener('click',start);$('stopBtn').addEventListener('click',stop);
document.addEventListener('visibilitychange',()=>{if(document.visibilityState==='visible'&&wanted)start();});
</script></body></html>`);
});
app.get('/api/rt7/stream/compare/state', (req, res) => {
  streamViewerPrune_();
  const dev = getCurrentDevice(req);
  res.json({ ok:true, version:SERVER_VERSION, lan:{ url: dev.base_url ? dev.base_url + '/api/camera/stream' : '', note:'手機與 ESP32 同 Wi-Fi 時測試；不經 Node-RED/Tailscale/Railway relay' }, cloud:{ url:'/api/rt7/camera/stream.mjpg', stream:liveStreamState, snapshot:getSnapshotMeta_() } });
});

app.get('/api/rt7/intercom/ws/state', (req,res)=>res.json({ ok:true, version:SERVER_VERSION, ws:rt7IntercomWsState_() }));
app.get('/api/rt7/intercom/ws/probe', (req,res)=>{ const label=safeString(req.query.label||'probe'); const n=rt7SendToEspIntercom_(JSON.stringify({type:'intercom_probe',role:'intercom_probe_http',device_id:'#1',label,t:Date.now(),version:SERVER_VERSION})); res.json({ok:true,version:SERVER_VERSION,esp:n,state:rt7IntercomWsState_()}); });


app.get('/api/rt7/face/live_frame_state', (req,res)=>{
  const latest = rt7GetLatestWithMeta_();
  res.json({ ok:true, version:SERVER_VERSION, latest, stream:liveStreamState, intercom_ws:rt7IntercomWsState_() });
});

app.get('/api/rt7/face/snapshot_trigger_test', (req,res)=>{
  const requestId = 'manual_face_snap_' + Date.now();
  const cmd = queueCommand({ command:'face_snapshot_now', action:'face_snapshot_now', request_id:requestId, device_id:'rt7-esp32-s3-cam-01', message:'manual face snapshot trigger test' });
  const wsSent = rt7SendWsJsonToEsp_({ type:'face_snapshot_now', command:'face_snapshot_now', request_id:requestId, phase:'V54O', manual:true, time:nowIso() });
  res.json({ ok:true, version:SERVER_VERSION, request_id:requestId, ws_sent:wsSent, command:cmd, state:rt7IntercomWsState_(), latest_snapshot:getSnapshotMeta_() });
});


wss.on('connection', (ws, req) => {
  ws.rt7Role = 'control';
  ws.rt7DeviceId = '';
  try {
    const u = new URL(req.url || '/ws', 'http://localhost');
    const qRole = safeString(u.searchParams.get('role') || '');
    const qDev = safeString(u.searchParams.get('device_id') || u.searchParams.get('device') || '');
    const qPcmRole = safeString(u.searchParams.get('pcm_role') || '');
    if (qRole) ws.rt7Role = qRole;
    if (qDev) ws.rt7DeviceId = qDev;
    if (qPcmRole) { ws.rt7PcmRole = qPcmRole; ws.rt7PcmClient = rt7IsEspPcmRole_(qPcmRole); }
    if (rt7IsEspPcmRole_(qRole)) { ws.rt7PcmClient = true; if (!ws.rt7PcmRole) ws.rt7PcmRole = 'esp32_pcm'; }
  } catch (_) {}
  try { ws.send(JSON.stringify({ ok: true, type: 'hello', version: SERVER_VERSION, time: nowIso(), ws_frame:true, intercom_ws:rt7IntercomWsState_() })); } catch (_) {}
  ws.on('message', (data, isBinary) => {
    try {
      if (isBinary) {
        const buf = Buffer.from(data);
        // V5.0K: ESP32 mic PCM upstream. JPEG uploads are normally > 2 KB;
        // ESP PCM frames are 640 bytes, so relay small ESP binary frames to phone clients.
        if (rt7IsEspPcmClient_(ws) && buf.length <= 2048) {
          rt7AudioHold_(5000);
          ws.rt7EspPcmPackets = (ws.rt7EspPcmPackets || 0) + 1;
          ws.rt7EspPcmBytes = (ws.rt7EspPcmBytes || 0) + buf.length;
          rt7WsTrace.espPcmPackets++;
          rt7WsTrace.espPcmBytes += buf.length;
          rt7WsTrace.lastEspPcmTime = nowIso();
          const pn = rt7SendToPhoneIntercom_(buf, { binary:true });
          if (pn > 0) {
            rt7WsTrace.phoneRxPackets++;
            rt7WsTrace.phoneRxBytes += buf.length;
            rt7WsTrace.lastPhoneRxTime = nowIso();
          }
          if (ws.rt7EspPcmPackets <= 5 || ws.rt7EspPcmPackets % 50 === 0) {
            try { ws.send(JSON.stringify({ ok:true, type:'esp_pcm_relay_trace_v50n', esp_packets:ws.rt7EspPcmPackets, esp_bytes:ws.rt7EspPcmBytes, phone_clients:pn, state:rt7IntercomWsState_() })); } catch (_) {}
          }
          return;
        }
        const looksLikePhonePcm = rt7IsPhonePcmRole_(ws.rt7Role) || (!rt7IsEspPcmRole_(ws.rt7Role) && buf.length <= 2048);
        if (looksLikePhonePcm) {
          rt7AudioHold_(5000);
          if (!rt7IsPhonePcmRole_(ws.rt7Role)) ws.rt7Role = 'phone_pcm_auto';
          ws.rt7IntercomPackets = (ws.rt7IntercomPackets || 0) + 1;
          ws.rt7IntercomBytes = (ws.rt7IntercomBytes || 0) + buf.length;
          rt7WsTrace.phonePcmPackets++;
          rt7WsTrace.phonePcmBytes += buf.length;
          rt7WsTrace.lastPhonePcmTime = nowIso();
          const n = rt7SendToEspIntercom_(buf, { binary:true });
          if (n > 0) {
            rt7WsTrace.relayPcmPackets++;
            rt7WsTrace.relayPcmBytes += buf.length;
            rt7WsTrace.lastRelayTime = nowIso();
          }
          if (ws.rt7IntercomPackets <= 5 || ws.rt7IntercomPackets % 50 === 0) {
            try { ws.send(JSON.stringify({ ok:true, type:'ws_relay_trace_v50n', phone_packets:ws.rt7IntercomPackets, phone_bytes:ws.rt7IntercomBytes, relay_clients:n, phone_pcm_rx:rt7WsTrace.phonePcmPackets, relay_to_esp32:rt7WsTrace.relayPcmPackets, esp32_clients:rt7IntercomWsState_().esp, state:rt7IntercomWsState_() })); } catch (_) {}
          }
          return;
        }
        acceptWsStreamFrame_(buf, ws);
        return;
      }
      const txt = data.toString('utf8');
      let msg = null;
      try { msg = JSON.parse(txt); } catch (_) {}
      if (msg && msg.role) {
        ws.rt7Role = safeString(msg.role);
        ws.rt7DeviceId = safeString(msg.device_id || msg.device || msg.id || ws.rt7DeviceId || '#1');
        if (msg.pcm_role) { ws.rt7PcmRole = safeString(msg.pcm_role); ws.rt7PcmClient = rt7IsEspPcmRole_(ws.rt7PcmRole); }
        if (msg.pcm_client === true || msg.type === 'esp32_pcm_register') { ws.rt7PcmClient = true; if (!ws.rt7PcmRole) ws.rt7PcmRole = 'esp32_pcm'; }
        if (ws.rt7Role === 'viewer') streamViewers.set(safeString(msg.viewer_id || req.socket.remoteAddress || Math.random()), { ts:Date.now(), ip:req.socket.remoteAddress, state:'visible', ws:true });
        ws.send(JSON.stringify({ ok:true, type:'role_ack', phase:'V50P', role:ws.rt7Role, pcm_role:ws.rt7PcmRole||'', pcm_client:!!ws.rt7PcmClient, version:SERVER_VERSION, time:nowIso(), intercom_ws:rt7IntercomWsState_() }));
      }
      if (msg && rt7IsPhonePcmRole_(ws.rt7Role) && (msg.type === 'intercom_begin' || msg.type === 'intercom_end' || msg.type === 'intercom_ping' || msg.type === 'intercom_probe' || msg.type === 'esp_begin' || msg.type === 'esp_end' || msg.type === 'intercom_listen')) {
        if (msg.type === 'intercom_begin') rt7AudioHold_(8000);
        if (msg.type === 'intercom_end') rt7AudioHold_(3500);
        const n = rt7SendToEspIntercom_(JSON.stringify(Object.assign({ relay_time:Date.now() }, msg)));
        try { ws.send(JSON.stringify({ ok:true, type:'intercom_control_relay', control:msg.type, esp:n, state:rt7IntercomWsState_() })); } catch (_) {}
      }
    } catch (e) {
      try { ws.send(JSON.stringify({ ok:false, type:'ws_error', error:String(e.message||e) })); } catch (_) {}
    }
  });
  ws.on('close', () => { ws.rt7Closed = true; });
});

ensureDataDir();
const port = process.env.PORT || 3000;
server.listen(port, () => console.log(`${SERVER_VERSION} listening on ${port}`));

