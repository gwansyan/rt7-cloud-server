const express = require('express');
const fs = require('fs');
const path = require('path');
const http = require('http');
const WebSocket = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: '/ws' });

app.use(express.json({ limit: '8mb' }));
app.use(express.urlencoded({ extended: true, limit: '8mb' }));

const DATA_DIR = process.env.RT7_DATA_DIR || path.join(__dirname, 'data');
const EVENT_LOG = path.join(DATA_DIR, 'rt7_event_log.jsonl');
const DEVICES_FILE = path.join(DATA_DIR, 'rt7_devices.json');

const SERVER_VERSION = 'RT7_CLOUD_SERVER_V5_1P_INTERCOM_PHONE_MIC_ORIGINAL_BATCH_FIX';

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
  last_vision: null
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

// ---------- Original RT7 mobile-style cloud doorbell UI ----------
app.get('/rt7_cloud_original_ui_doorbell', (req, res) => {
  const q = req.query || {};
  const mode = safeString(q.mode || 'idle').toLowerCase();
  const ip = safeString(q.ip || '192.168.0.179').replace(/[^0-9.]/g, '') || '192.168.0.179';
  const aiOn = q.ai === '1' || cloudState.ai_enabled === true;
  const doorLast = doorbellState.last || null;
  const doorText = doorLast && doorLast.time ? ('最後：' + new Date(doorLast.time).toLocaleTimeString('zh-TW')) : '等待事件';
  let modeLabel = mode === 'lan' ? 'LAN' : (mode === 'cloud' ? 'CLOUD' : (mode === 'auto' ? 'AUTO' : 'AUTO'));
  let answer = mode === 'idle' ? '雲端門鈴待機中' : '自動判斷影像來源中';
  let hint = mode === 'idle' ? '等待影像串流' : '自動判斷：內網直連 / Railway 雲端';
  res.type('html').send(`<!doctype html><html lang="zh-Hant"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<title>RT7 Cloud Original UI V5.1A</title>
<style>
:root{--dark:#0b252b;--dark2:#0d2c32;--red:#ef2b24;--blue:#17a8e5;--green:#22a951;--text:#17262a;--line:#e5e7eb;--orange:#9a3b18}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent} html,body{margin:0;padding:0;background:#fff;color:var(--text);font-family:system-ui,-apple-system,"Noto Sans TC","Microsoft JhengHei",Arial,sans-serif} body{max-width:520px;margin:0 auto;min-height:100vh;padding-bottom:28px}
a,button,input,select{pointer-events:auto!important;touch-action:manipulation!important}.noTouch,.video img,.emptyVideo,.badge{pointer-events:none!important}
.top{height:66px;background:linear-gradient(90deg,var(--dark),var(--dark2));color:#fff;display:flex;align-items:center;justify-content:space-between;padding:0 16px;font-weight:900}.hamb{font-size:34px}.title{text-align:center;line-height:1.15;font-size:17px;letter-spacing:.4px}.spacer{width:34px}
.deviceBar{padding:8px 12px;background:#fff;border-bottom:1px solid var(--line)}.deviceText{height:42px;border:1px solid #334155;border-radius:8px;font-weight:900;padding:0 10px;background:#fff;font-size:17px;display:flex;align-items:center;justify-content:space-between;color:#111827}.deviceText select{border:0;background:#fff;font:inherit;font-weight:900;width:100%;outline:0}
.video{position:relative;background:#000;aspect-ratio:4/3;overflow:hidden}.video img{width:100%;height:100%;object-fit:cover;background:#000;display:block;border:0}.emptyVideo{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;text-align:center;color:#cbd5e1;font-weight:900;font-size:18px;line-height:1.45;padding:12px}.badge{position:absolute;top:12px;border-radius:7px;padding:7px 12px;color:white;font-weight:900;box-shadow:0 2px 8px rgba(0,0,0,.22)}.idle{left:14px;background:#71839d}.idle.aiOn{background:#16a34a}.live{right:14px;background:var(--red)}
.videoBtns{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:6px;background:#fff;padding:6px 8px;border-bottom:1px solid var(--line);align-items:center}.vbtn{display:flex;align-items:center;justify-content:center;border:0;border-radius:8px;color:#fff;font-weight:900;padding:8px 3px;font-size:13px;line-height:1;min-width:0;width:100%;height:38px;text-decoration:none;white-space:nowrap;overflow:hidden}.vblue{background:var(--blue)}.vred{background:var(--red)}.vdark{background:#102a31}.vorange{background:#f59e0b}
.statusLine{min-height:46px;display:grid;grid-template-columns:1fr 1fr;gap:8px;border-bottom:1px solid var(--line);align-items:center;padding:8px 12px;background:#fff;font-size:15px;font-weight:800}.dot{display:inline-block;width:11px;height:11px;border-radius:50%;background:var(--green);margin-right:8px}.answer{color:#5b1f14}.door{color:#8a2f15;text-align:right}.door.bellNow{color:#9a3412;font-weight:900}.doorAlert{display:none!important}
.micZone{text-align:center;padding:18px 0 8px}.bigMic{touch-action:none;-webkit-user-select:none;user-select:none;width:128px;height:128px;border-radius:50%;border:3px solid #cbd5e1;background:#eef2f7;display:inline-flex;align-items:center;justify-content:center;font-size:72px;box-shadow:0 4px 18px rgba(20,40,60,.08);text-decoration:none;color:#24333a}
.actions{display:flex;justify-content:center;gap:10px;padding:10px 8px 4px}.act{width:66px;text-align:center;font-size:12px;font-weight:900;color:#24333a}.circle{width:58px;height:58px;border:3px solid var(--red);border-radius:50%;background:#fff;display:flex;align-items:center;justify-content:center;font-size:28px;margin:0 auto 4px;box-shadow:0 2px 10px rgba(0,0,0,.1);text-decoration:none;color:#24333a}.circle.aiActive{border-color:#22c55e;background:#ecfdf5}.bigMic.talking{border-color:#ef4444;background:#fff1f2;box-shadow:0 0 0 5px rgba(239,68,68,.16)}.circle.talking{border-color:#ef4444;background:#fff1f2;box-shadow:0 0 0 4px rgba(239,68,68,.18)}.reg{display:flex;align-items:center;gap:10px;padding:8px 20px}.reg label{font-size:14px;font-weight:900}.reg input{flex:1;height:36px;border:1px solid #cbd5e1;border-radius:7px;padding:0 10px;font-size:16px}.small{font-size:12px;color:#64748b}.debug{display:none!important}
@media(max-height:740px){.top{height:56px}.videoBtns{gap:4px;padding:5px 6px}.vbtn{height:34px;font-size:12px;padding:7px 2px}.title{font-size:15px}.video{aspect-ratio:16/9}.bigMic{width:104px;height:104px;font-size:58px}.circle{width:50px;height:50px;font-size:24px}.act{font-size:11px}.statusLine{font-size:13px;min-height:38px}.reg{padding-top:4px}}
</style></head><body>
<header class="top"><div class="hamb">☰</div><div class="title">RT7 PHASE10<br>AI MODE ROUTER</div><div class="spacer"></div></header>
<div class="deviceBar"><div class="deviceText"><select id="deviceSel"><option value="${ip}">#1 / RT7 ESP32-S3-CAM / ${ip}</option></select></div></div>
<section class="video"><div id="emptyVideo" class="emptyVideo">${hint}<br><span class="small">網內使用 ESP32 直連；網外使用 Railway 雲端</span></div><img id="stream" alt=""><div id="aiBadge" class="badge idle ${aiOn?'aiOn':''}">${aiOn?'AI_ENABLE':'IDLE'}</div><div id="streamModeBadge" class="badge live">${modeLabel}</div></section>
<section class="videoBtns"><button id="btnAiOn" class="vbtn vblue" type="button">啟用AI</button><button id="btnAiOff" class="vbtn vred" type="button">關閉AI</button><button id="btnAudio" class="vbtn vorange" type="button">啟用提示音</button><button id="btnStart" class="vbtn vdark" type="button">開始影像</button><button id="btnStop" class="vbtn vdark" type="button">停止影像</button></section>
<section class="statusLine"><div class="answer"><span class="dot"></span>回答：<span id="answerText">${answer}</span></div><div class="door">門鈴：<span id="doorText">${doorText}</span></div><div id="doorAlert" class="doorAlert">🔔 有人按門鈴</div></section>
<section class="micZone"><button id="btnVoice" class="bigMic" type="button" aria-label="按住對講">🎙️</button><div class="small" style="font-weight:900;color:#64748b;margin-top:4px">短按麥克風播放對講測試音</div></section>
<section class="actions"><div class="act"><button id="btnOpenDoor" class="circle" type="button">🚪</button>開門</div><div class="act"><button class="circle" type="button">👥</button>名單</div><div class="act"><button id="btnEndTalk" class="circle" type="button">◼</button>對講</div><div class="act"><button class="circle" type="button">＋</button>註冊</div><div class="act"><button id="btnAiVoice" class="circle ${aiOn?'aiActive':''}" type="button">🎙️</button>AI語音助理</div></section>
<div class="reg"><label>註冊名稱</label><input id="regName" value="gwansyan"></div>
<script>
(function(){
  var ip=${JSON.stringify(ip)}; var mode=${JSON.stringify(mode)}; var ai=${aiOn?'true':'false'}; var img=document.getElementById('stream'); var empty=document.getElementById('emptyVideo'); var badge=document.getElementById('streamModeBadge'); var answer=document.getElementById('answerText'); var debug=null; var audioCtx=null; var audioOK=false; var audioTried=false;
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
  function bind(id,fn){ var el=document.getElementById(id); if(el) el.addEventListener('click', function(ev){ ev.preventDefault(); ev.stopPropagation(); fn(); }, false); }
  bind('btnStart', startAuto); bind('btnStop', stopVideo); bind('btnAudio', enableDoorbellAudio);
  bind('btnAiOn', function(){ setAiUi(true,'AI 已啟用'); setDebug('ai on'); });
  bind('btnAiOff', function(){ setAiUi(false,'AI 已關閉'); setDebug('ai off'); });
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
  // V5.1P: Intercom Phone Mic PCM - original RT7 batching + no giant serial spam.
  // Fix: V5.1N still sent 640B/40ms and all-zero chunks, causing tick/tock audio.
  // This version uses original RT7 cleaner/batching: 1280B per ~40ms, skips digital-zero frames, and avoids request flooding.
  var rt7IntercomBeaconKeep=[];
  var rt7IntercomBeginOn=false;
  var rt7MicStream=null, rt7MicCtx=null, rt7MicSource=null, rt7MicProc=null;
  var rt7MicTxBytes=[], rt7MicSendBusy=false, rt7MicSendQueue=[];
  var rt7MicLastPostMs=0, rt7MicHpX=0, rt7MicHpY=0;
  var rt7MicFrameCount=0, rt7MicZeroFrames=0, rt7MicRestarted=false, rt7MicBeginMs=0, rt7MicLastDiagMs=0;
  // Keep 640-byte GET chunks because 8081 image beacon is the proven LAN path.
  // Audio cleaning follows original RT7 settings more closely.
  var RT7_MIC_GAIN=0.76, RT7_HP_A=0.995, RT7_MIC_TARGET_BYTES=1280, RT7_MIC_MAX_BYTES=1280, RT7_MIC_MIN_POST_MS=38;
  function sendIntercomBeacon(ic,label,extra){
    try{
      var beacon=new Image();
      rt7IntercomBeaconKeep.push(beacon);
      if(rt7IntercomBeaconKeep.length>64) rt7IntercomBeaconKeep.splice(0, rt7IntercomBeaconKeep.length-64);
      beacon.onload=function(){ setDebug('intercom '+ic+' beacon loaded '+label); };
      beacon.onerror=function(){ setDebug('intercom '+ic+' beacon sent/error ok '+label); };
      var url='http://'+ip+':8081/api/door/open_fast?ic='+encodeURIComponent(ic)+'&_ic_label='+encodeURIComponent(label||'')+'&_door='+Date.now();
      if(extra) url += extra;
      beacon.src=url;
      return true;
    }catch(e){ setAnswer('對講 '+ic+' 失敗：'+e.message); setDebug('intercom '+ic+' failed '+e.message); return false; }
  }
  function rt7BytesToHex(bytes){ var hex=''; for(var i=0;i<bytes.length;i++) hex+=bytes[i].toString(16).padStart(2,'0'); return hex; }
  function rt7FloatToPcm16Bytes(f32){
    var bytes=new Uint8Array(f32.length*2);
    for(var i=0;i<f32.length;i++){
      var v=Math.max(-1,Math.min(1,f32[i]));
      var s=Math.round(v<0?v*0x8000:v*0x7fff);
      bytes[i*2]=s&255; bytes[i*2+1]=(s>>8)&255;
    }
    return bytes;
  }
  function rt7DownsampleTo16k(input,inRate){
    if(!inRate || Math.abs(inRate-16000)<1) return input;
    var ratio=inRate/16000, len=Math.floor(input.length/ratio);
    if(len<=0) return new Float32Array(0);
    var out=new Float32Array(len);
    for(var i=0;i<len;i++){
      var a=Math.floor(i*ratio), b=Math.min(Math.floor((i+1)*ratio),input.length), sum=0,c=0;
      for(var j=a;j<b;j++){sum+=input[j];c++;}
      out[i]=c?sum/c:0;
    }
    return out;
  }
  function rt7CleanMicFrame(input){
    var out=new Float32Array(input.length);
    var fadeN=Math.min(32,input.length);
    for(var i=0;i<input.length;i++){
      var x=input[i];
      var y=x-rt7MicHpX+RT7_HP_A*rt7MicHpY;
      rt7MicHpX=x; rt7MicHpY=y;
      y*=RT7_MIC_GAIN;
      // Original RT7 style: mild limiter, not hard clipping.
      if(y>0.98)y=0.98+(y-0.98)*0.25;
      if(y<-0.98)y=-0.98+(y+0.98)*0.25;
      if(i<fadeN)y*=(0.80+0.20*i/fadeN);
      if(input.length-i<fadeN)y*=(0.80+0.20*(input.length-i)/fadeN);
      out[i]=Math.max(-1,Math.min(1,y));
    }
    return out;
  }
  function rt7MicStats(raw){
    var sum=0, peak=0;
    for(var i=0;i<raw.length;i++){ var a=Math.abs(raw[i]); sum+=raw[i]*raw[i]; if(a>peak)peak=a; }
    return {rms:Math.sqrt(sum/Math.max(1,raw.length)), peak:peak};
  }
  function rt7MicQueueBytes(bytes){
    for(var i=0;i<bytes.length;i++) rt7MicTxBytes.push(bytes[i]);
    var now=performance.now();
    if(rt7MicTxBytes.length>=RT7_MIC_TARGET_BYTES && (now-rt7MicLastPostMs)>=RT7_MIC_MIN_POST_MS){
      var n=Math.min(RT7_MIC_MAX_BYTES,rt7MicTxBytes.length); n-=n%2;
      var chunk=rt7MicTxBytes.splice(0,n);
      rt7MicSendQueue.push(rt7BytesToHex(chunk));
      rt7MicLastPostMs=now;
      if(rt7MicSendQueue.length>4) rt7MicSendQueue.splice(0,rt7MicSendQueue.length-4);
      rt7MicFlushQueue();
    }
  }
  function rt7MicFlushTail(){
    while(rt7MicTxBytes.length>0){
      if(rt7MicTxBytes.length%2)rt7MicTxBytes.push(0);
      var n=Math.min(RT7_MIC_MAX_BYTES,rt7MicTxBytes.length); n-=n%2;
      var chunk=rt7MicTxBytes.splice(0,n);
      rt7MicSendQueue.push(rt7BytesToHex(chunk));
      if(rt7MicSendQueue.length>4)rt7MicSendQueue.splice(0,rt7MicSendQueue.length-4);
    }
    rt7MicFlushQueue();
  }
  function rt7MicFlushQueue(){
    if(rt7MicSendBusy)return;
    rt7MicSendBusy=true;
    (function pump(){
      if(!rt7MicSendQueue.length){ rt7MicSendBusy=false; return; }
      var hex=rt7MicSendQueue.shift();
      sendIntercomBeacon('pcm','mic_pcm','&hex='+hex);
      setTimeout(pump, 10);
    })();
  }
  function rt7StopPhoneMic(){
    try{ if(rt7MicProc){ rt7MicProc.disconnect(); rt7MicProc.onaudioprocess=null; } }catch(_){}
    try{ if(rt7MicSource) rt7MicSource.disconnect(); }catch(_){}
    try{ if(rt7MicCtx) rt7MicCtx.close(); }catch(_){}
    try{ if(rt7MicStream) rt7MicStream.getTracks().forEach(function(t){try{t.stop();}catch(_){}}); }catch(_){}
    rt7MicStream=null; rt7MicCtx=null; rt7MicSource=null; rt7MicProc=null;
  }
  async function rt7OpenMicStream(fallback){
    var constraints = fallback
      ? {audio:{echoCancellation:false,noiseSuppression:false,autoGainControl:true,channelCount:1},video:false}
      : {audio:{echoCancellation:{ideal:true},noiseSuppression:{ideal:false},autoGainControl:{ideal:false},channelCount:{ideal:1},sampleRate:{ideal:48000}},video:false};
    return await navigator.mediaDevices.getUserMedia(constraints);
  }
  async function rt7EnsurePhoneMic(fallback){
    rt7StopPhoneMic(); // important: avoid stale/silent Android Chrome stream
    rt7MicStream=await rt7OpenMicStream(!!fallback);
    var AC=window.AudioContext||window.webkitAudioContext;
    // Original RT7 used normal AudioContext; fallback tries 16kHz context to avoid all-zero resampling bugs.
    rt7MicCtx=fallback ? new AC({sampleRate:16000}) : new AC();
    rt7MicSource=rt7MicCtx.createMediaStreamSource(rt7MicStream);
    rt7MicProc=rt7MicCtx.createScriptProcessor(2048,1,1);
    rt7MicProc.onaudioprocess=function(e){
      if(!rt7IntercomBeginOn)return;
      var raw=e.inputBuffer.getChannelData(0);
      rt7MicFrameCount++;
      var st=rt7MicStats(raw);
      var now=performance.now();
      if(st.peak<0.00015) rt7MicZeroFrames++; else rt7MicZeroFrames=0;
      if(now-rt7MicLastDiagMs>900){
        rt7MicLastDiagMs=now;
        setDebug('mic rms='+st.rms.toFixed(5)+' peak='+st.peak.toFixed(5)+' sr='+Math.round(rt7MicCtx.sampleRate)+' zero='+rt7MicZeroFrames);
      }
      if(!rt7MicRestarted && (now-rt7MicBeginMs)>900 && rt7MicZeroFrames>12){
        rt7MicRestarted=true;
        setDebug('mic digital-zero, restart fallback constraints');
        rt7EnsurePhoneMic(true).catch(function(err){ setDebug('mic fallback failed '+(err.message||err)); });
        return;
      }
      // V5.1P: Do not feed true digital-zero chunks to ESP32 speaker.
      // V5.1N logs showed long runs of 0000 PCM; these create underflow/click artifacts.
      if(st.peak < 0.00005) return;
      var cleaned=rt7CleanMicFrame(raw);
      var ds=rt7DownsampleTo16k(cleaned, rt7MicCtx.sampleRate);
      if(ds.length) rt7MicQueueBytes(rt7FloatToPcm16Bytes(ds));
    };
    rt7MicSource.connect(rt7MicProc);
    // Keep processor alive; ScriptProcessor output is unused.
    rt7MicProc.connect(rt7MicCtx.destination);
    if(rt7MicCtx.state!=='running') await rt7MicCtx.resume();
    var tracks=rt7MicStream.getAudioTracks();
    var label=tracks&&tracks[0]?tracks[0].label:'mic';
    setDebug('phone mic ready original-clean sr='+Math.round(rt7MicCtx.sampleRate)+' fallback='+(!!fallback)+' track='+label);
    return true;
  }
  async function intercomBeginEndToggle(label){
    if(!rt7IntercomBeginOn){
      rt7IntercomBeginOn=true;
      rt7MicTxBytes=[]; rt7MicSendQueue=[]; rt7MicLastPostMs=0; rt7MicHpX=0; rt7MicHpY=0;
      rt7MicFrameCount=0; rt7MicZeroFrames=0; rt7MicRestarted=false; rt7MicBeginMs=performance.now(); rt7MicLastDiagMs=0;
      setAnswer('對講開始：請說話');
      setDebug('INTERCOM BEGIN + PHONE_MIC_ORIGINAL_CLEAN '+label);
      sendIntercomBeacon('begin', label||'begin');
      setTimeout(function(){ sendIntercomBeacon('ping','mic_pre_ping'); }, 80);
      var b=document.getElementById('btnVoice'); if(b)b.classList.add('talking');
      var e=document.getElementById('btnEndTalk'); if(e)e.classList.add('talking');
      try{ await rt7EnsurePhoneMic(false); }
      catch(e2){
        try{ setDebug('mic normal failed, try fallback '+(e2.message||e2)); await rt7EnsurePhoneMic(true); }
        catch(e3){ setAnswer('手機麥克風啟用失敗：'+(e3.message||e3)); setDebug('mic start failed '+(e3.message||e3)); rt7IntercomBeginOn=false; sendIntercomBeacon('end','mic_start_failed'); if(b)b.classList.remove('talking'); if(e)e.classList.remove('talking'); }
      }
    }else{
      rt7IntercomBeginOn=false;
      rt7MicFlushTail();
      setAnswer('對講結束');
      setDebug('INTERCOM END '+label);
      sendIntercomBeacon('end', label||'end');
      var b2=document.getElementById('btnVoice'); if(b2)b2.classList.remove('talking');
      var e2=document.getElementById('btnEndTalk'); if(e2)e2.classList.remove('talking');
      setTimeout(rt7StopPhoneMic, 250);
    }
  }
  bind('btnVoice', function(){ intercomBeginEndToggle('bigMic_click'); });
  bind('btnEndTalk', function(){ if(rt7IntercomBeginOn) intercomBeginEndToggle('lowerTalk_end'); else { setAnswer('對講尚未開始'); setDebug('intercom end ignored'); } });
  function speakAnswer(txt){ if(window.speechSynthesis && (txt||'').length){ try{ speechSynthesis.cancel(); var u=new SpeechSynthesisUtterance(txt); u.lang='zh-TW'; speechSynthesis.speak(u); }catch(e){} } }
  function setAiUi(on, msg){
    ai=!!on;
    var b=document.getElementById('aiBadge');
    var v=document.getElementById('btnAiVoice');
    if(b){ b.textContent=ai?'AI_ENABLE':'IDLE'; if(ai)b.classList.add('aiOn'); else b.classList.remove('aiOn'); }
    if(v){ if(ai)v.classList.add('aiActive'); else v.classList.remove('aiActive'); }
    if(msg) setAnswer(msg);
  }
  async function routeVoiceQuestion(text){
    text=(text||'').trim();
    if(!text){ setAiUi(false,'沒有收到語音內容，請再按一次 AI語音助理後說話'); setDebug('voice empty'); return; }
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
      setAiUi(false);
    }
  }
  function startVoiceAsk(){ setAiUi(true,'請開始說話'); var SR=window.SpeechRecognition||window.webkitSpeechRecognition; if(!SR){ var t=prompt('請輸入要問 AI語音助理的內容：','')||''; routeVoiceQuestion(t); return; } try{ var rec=new SR(); rec.lang='zh-TW'; rec.continuous=false; rec.interimResults=false; rec.maxAlternatives=1; setAnswer('請開始說話'); setDebug('speech recognition start'); rec.onresult=function(ev){ var text=''; try{text=ev.results[0][0].transcript||'';}catch(e){} routeVoiceQuestion(text); }; rec.onerror=function(ev){ setAiUi(false,'語音辨識失敗：'+(ev.error||'unknown')+'。請再按一次 AI語音助理。'); setDebug('speech error '+(ev.error||'')); }; rec.onend=function(){ setDebug('speech recognition end'); }; rec.start(); }catch(e){ var t2=prompt('語音辨識無法啟動，請輸入問題：','')||''; routeVoiceQuestion(t2); } }
  // V5.1 Restore Phone Intercom: phone mic -> ESP32 speaker, ESP32 mic -> phone playback.
  var phoneMicStream=null, phoneMicCtx=null, phoneMicSource=null, phoneMicProc=null;
  var phoneAudioCtx=null, phoneAudioNextTime=0, talking=false, intercomOn=false, sendBusy=false, sendQueue=[], txBytes=[];
  var espPolling=false, espTimer=null, hpLastX=0, hpLastY=0, lastPostMs=0;
  var MIC_GAIN=0.76, HP_A=0.995, POST_MIN_MS=60, TARGET_BYTES=1024, MAX_BYTES=1024;
  var lanBeaconSeq=0, lanBeaconBusy=0, lanBeaconMax=3; var lanBeaconKeep=[];
  function intercomLanBase(){ var dip=(dev&&dev.ip)?dev.ip:(ip||'192.168.0.179'); return 'http://'+dip+':8081'; }
  function isLanDirectMode(){
    var bt=''; try{ bt=(document.getElementById('streamBadge')||{}).textContent||''; }catch(e){}
    return true; // V5.1G: force LAN 8081 for intercom PTT because door-open 8081 path is proven reachable
  }
  function intercomPath(path){
    // V5.1B: LAN mode uses ESP32 port 8081 directly. Railway cannot proxy to private 192.168.x.x,
    // and port 80 is occupied by /api/camera/stream while LAN video is running.
    if(isLanDirectMode()) return intercomLanBase()+path.replace('/api/ind_full','/api');
    return path;
  }
  function lanBeaconUrl(path, extra){
    var ip=(dev&&dev.ip)?dev.ip:(window.rt7EspIp||'192.168.0.179');
    var q=(extra||'');
    if(q && q.charAt(0)==='&') q='?'+q.substring(1);
    else if(q && q.charAt(0)!=='?') q='?'+q;
    var sep=q ? '&' : '?';
    return 'http://'+ip+':8081'+path+q+sep+'_='+(Date.now())+'&seq='+(++lanBeaconSeq);
  }
  function sendLanBeacon(path, label, extra){
    // V5.1G: keep Image objects alive; Android Chrome may cancel local images if the object is GC'ed during long-press.
    try{
      var img=new Image();
      lanBeaconBusy++;
      lanBeaconKeep.push(img);
      if(lanBeaconKeep.length>24) lanBeaconKeep.splice(0, lanBeaconKeep.length-24);
      img.onload=img.onerror=function(){ lanBeaconBusy=Math.max(0,lanBeaconBusy-1); setTimeout(function(){ var i=lanBeaconKeep.indexOf(img); if(i>=0) lanBeaconKeep.splice(i,1); }, 1800); };
      var url=lanBeaconUrl(path, extra||'');
      setDebug('LAN beacon '+(label||'')+' -> '+url.replace(/hex=[^&]+/,'hex=...'));
      img.src=url;
      return {ok:true,source:'lan_beacon',label:label||''};
    }catch(e){ setDebug((label||'lan beacon')+' failed '+e.message); return {ok:false,error:e.message}; }
  }
  async function apiGetAudio(u,label){
    try{
      if(isLanDirectMode()){
        var p=u.replace('/api/ind_full','/api');
        sendLanBeacon(p,label||'audio_get','');
        return {ok:true,source:'lan_beacon'};
      }
      var url=intercomPath(u); var r=await fetch(url+(url.indexOf('?')>=0?'&':'?')+'_='+Date.now(),{cache:'no-store',mode:'same-origin'}); var t=await r.text(); try{return JSON.parse(t)}catch(e){return{ok:r.ok,raw:t}}
    }catch(e){ setDebug((label||'audio')+' failed '+e.message); return {ok:false,error:e.message}; }
  }
  async function apiPostAudio(u,body,label){
    try{
      if(isLanDirectMode()){
        // PCM is sent as short GET chunks to 8081 so it reaches ESP32 while MJPEG occupies port 80.
        // Body is hex-only, so it is safe in the query string. Keep chunks <= about 2KB URL.
        var p='/api/door/open_fast';
        if(lanBeaconBusy>lanBeaconMax){ return {ok:true,drop:true,source:'lan_beacon'}; }
        sendLanBeacon(p,label||'phone_pcm','&ic=pcm&hex='+body);
        return {ok:true,source:'lan_beacon'};
      }
      var url=intercomPath(u); var r=await fetch(url+(url.indexOf('?')>=0?'&':'?')+'_='+Date.now(),{method:'POST',headers:{'Content-Type':'text/plain'},body:body,cache:'no-store',mode:'same-origin'}); var t=await r.text(); try{return JSON.parse(t)}catch(e){return{ok:r.ok,raw:t}}
    }catch(e){ setDebug((label||'audio post')+' failed '+e.message); return {ok:false,error:e.message}; }
  }
  async function resumeAudioForIntercom(reason){ try{ if(phoneAudioCtx&&phoneAudioCtx.state!=='running') await phoneAudioCtx.resume(); }catch(e){} try{ if(phoneMicCtx&&phoneMicCtx.state!=='running') await phoneMicCtx.resume(); }catch(e){} }
  function cleanMicFrame(input){ var out=new Float32Array(input.length); var fadeN=Math.min(32,input.length); for(var i=0;i<input.length;i++){ var x=input[i]; var y=x-hpLastX+HP_A*hpLastY; hpLastX=x; hpLastY=y; y*=MIC_GAIN; if(y>0.98)y=0.98; if(y<-0.98)y=-0.98; if(i<fadeN)y*=i/fadeN; if(input.length-i<fadeN)y*=(input.length-i)/fadeN; out[i]=y; } return out; }
  function downsampleTo16k(input,inRate){ if(inRate===16000)return input; var ratio=inRate/16000, len=Math.floor(input.length/ratio), out=new Float32Array(len); for(var i=0;i<len;i++){ var a=Math.floor(i*ratio), b=Math.min(Math.floor((i+1)*ratio),input.length), sum=0,c=0; for(var j=a;j<b;j++){sum+=input[j];c++;} out[i]=c?sum/c:0; } return out; }
  function floatToPcm16Bytes(f32){ var bytes=new Uint8Array(f32.length*2); for(var i=0;i<f32.length;i++){ var s=Math.max(-1,Math.min(1,f32[i])); var v=Math.round(s<0?s*0x8000:s*0x7fff); bytes[i*2]=v&255; bytes[i*2+1]=(v>>8)&255; } return bytes; }
  function bytesToHex(bytes){ var hex=''; for(var i=0;i<bytes.length;i++)hex+=bytes[i].toString(16).padStart(2,'0'); return hex; }
  function enqueueTx(bytes){ for(var i=0;i<bytes.length;i++)txBytes.push(bytes[i]); var now=performance.now(); if(txBytes.length>=TARGET_BYTES&&(now-lastPostMs)>=POST_MIN_MS){ var n=Math.min(MAX_BYTES,txBytes.length); n-=n%2; var chunk=txBytes.splice(0,n); sendQueue.push(bytesToHex(chunk)); lastPostMs=now; if(sendQueue.length>2)sendQueue.splice(0,sendQueue.length-2); flushTxQueue(); } }
  async function flushTxQueue(){ if(sendBusy)return; sendBusy=true; while(sendQueue.length){ await apiPostAudio('/api/ind_full/audio/phone_pcm_hex', sendQueue.shift(), 'phone_pcm'); } sendBusy=false; }
  function flushTxTail(){ while(txBytes.length>0){ if(txBytes.length%2)txBytes.push(0); var n=Math.min(MAX_BYTES,txBytes.length); n-=n%2; var chunk=txBytes.splice(0,n); sendQueue.push(bytesToHex(chunk)); if(sendQueue.length>2)sendQueue.splice(0,sendQueue.length-2); } flushTxQueue(); }
  function fastIntercomBeginBurst(tag){
    // Same endpoint as working door open, but with ic parameter so ESP32 does not open the door.
    sendLanBeacon('/api/door/open_fast','ic_ping_'+tag,'ic=ping');
    sendLanBeacon('/api/door/open_fast','ic_begin_'+tag,'ic=begin');
    setTimeout(function(){ sendLanBeacon('/api/door/open_fast','ic_begin2_'+tag,'ic=begin'); }, 180);
  }
  function fastIntercomButtonTest(tag){
    // V5.1I: use the exact proven 8081 door beacon route, but add ic=test so ESP32 logs test and never opens the door.
    setAnswer('對講測試訊號已送出');
    setDebug('INTERCOM TEST beacon '+tag);
    sendLanBeacon('/api/door/open_fast','ic_test_'+tag,'ic=test');
    setTimeout(function(){ sendLanBeacon('/api/door/open_fast','ic_ping_after_test_'+tag,'ic=ping'); }, 160);
  }

  async function ensurePhoneMic(){ if(phoneMicStream&&phoneMicCtx)return true; phoneMicStream=await navigator.mediaDevices.getUserMedia({audio:{echoCancellation:{ideal:true},noiseSuppression:{ideal:false},autoGainControl:{ideal:false},channelCount:{ideal:1},sampleRate:{ideal:48000}},video:false}); phoneMicCtx=new (window.AudioContext||window.webkitAudioContext)(); phoneMicSource=phoneMicCtx.createMediaStreamSource(phoneMicStream); phoneMicProc=phoneMicCtx.createScriptProcessor(2048,1,1); phoneMicProc.onaudioprocess=function(e){ if(!talking)return; var raw=e.inputBuffer.getChannelData(0); var cleaned=cleanMicFrame(raw); var ds=downsampleTo16k(cleaned, phoneMicCtx.sampleRate); enqueueTx(floatToPcm16Bytes(ds)); }; phoneMicSource.connect(phoneMicProc); phoneMicProc.connect(phoneMicCtx.destination); await resumeAudioForIntercom('mic'); setDebug('intercom mic ready sr='+phoneMicCtx.sampleRate); return true; }
  async function ensurePhonePlay(){ if(!phoneAudioCtx){ phoneAudioCtx=new (window.AudioContext||window.webkitAudioContext)({sampleRate:16000}); phoneAudioNextTime=phoneAudioCtx.currentTime+0.08; } await resumeAudioForIntercom('play'); return true; }
  function playPcm16Hex(hex){ if(!phoneAudioCtx||!hex)return; var n=Math.floor(hex.length/4); if(n<=0)return; var buf=phoneAudioCtx.createBuffer(1,n,16000), ch=buf.getChannelData(0); for(var i=0;i<n;i++){ var lo=parseInt(hex.substr(i*4,2),16), hi=parseInt(hex.substr(i*4+2,2),16); var v=(hi<<8)|lo; if(v>=32768)v-=65536; ch[i]=v/32768; } var src=phoneAudioCtx.createBufferSource(); src.buffer=buf; src.connect(phoneAudioCtx.destination); var now=phoneAudioCtx.currentTime; if(phoneAudioNextTime<now+0.02) phoneAudioNextTime=now+0.06; src.start(phoneAudioNextTime); phoneAudioNextTime+=buf.duration; }
  async function pollEspAudio(){ if(!espPolling||talking)return; var r=await apiGetAudio('/api/ind_full/audio/esp_pcm_hex?ms=60','esp_pcm'); if(r&&r.ok&&r.hex) playPcm16Hex(r.hex); }
  async function startEspRx(){ await ensurePhonePlay(); espPolling=true; intercomOn=true; await apiGetAudio('/api/ind_full/audio/esp_begin','esp_begin'); if(espTimer)clearInterval(espTimer); espTimer=setInterval(pollEspAudio,120); setDebug('esp rx polling on'); }
  async function stopEspRx(){ espPolling=false; if(espTimer)clearInterval(espTimer); espTimer=null; await apiGetAudio('/api/ind_full/audio/esp_end','esp_end'); setDebug('esp rx polling off'); }
  async function intercomDown(ev){
    if(ev){ev.preventDefault();ev.stopPropagation();}
    if(talking)return;
    talking=true; txBytes=[]; sendQueue=[]; hpLastX=0; hpLastY=0;
    var b=document.getElementById('btnVoice');
    if(b){b.classList.add('talking'); try{b.setPointerCapture&&ev&&ev.pointerId!=null&&b.setPointerCapture(ev.pointerId);}catch(_){}}
    var ebtn=document.getElementById('btnEndTalk'); if(ebtn)ebtn.classList.add('talking');
    setAnswer('對講中：請說話');
    setDebug('PHONE_TX start '+(currentStreamMode==='LAN'?'LAN 8081 immediate begin':'cloudProxy'));
    // V5.1D: send BEGIN immediately on pointerdown. Do not wait for getUserMedia/Speech/UI work.
    // This verifies the 8081 path in Serial and makes PTT feel instant.
    try{ fastIntercomBeginBurst('down'); }catch(_){ }
    try{
      await ensurePhoneMic();
      await startEspRx();
    }catch(e){
      talking=false; if(b)b.classList.remove('talking'); if(ebtn)ebtn.classList.remove('talking');
      setAnswer('手機麥克風啟用失敗：'+e.message); setDebug('intercom start failed');
    }
  }
  async function intercomUp(ev){ if(ev){ev.preventDefault();ev.stopPropagation();} if(!talking)return; talking=false; flushTxTail(); var b=document.getElementById('btnVoice'); if(b)b.classList.remove('talking'); var ebtn=document.getElementById('btnEndTalk'); if(ebtn)ebtn.classList.remove('talking'); if(isLanDirectMode()){ sendLanBeacon('/api/door/open_fast','phone_end_fast','ic=end'); } else { await apiGetAudio('/api/ind_full/audio/phone_end','phone_end'); } setAnswer('對講已結束'); setDebug('PHONE_TX stop'); }
  function bindIntercomPtt(){
    // V5.1A: 中間大麥克風才是對講 PTT；下方「對講結束」保留為停止/復位。
    var b=document.getElementById('btnVoice');
    if(b){
      // V5.1G: send a tiny begin burst in capture phase before mic permission / long-press logic.
      b.addEventListener('pointerdown',function(ev){ try{ fastIntercomBeginBurst('pdcap'); }catch(e){} },true);
      b.addEventListener('touchstart',function(ev){ try{ fastIntercomBeginBurst('tscap'); }catch(e){} },{passive:true,capture:true});
      b.addEventListener('click',function(ev){ try{ ev.preventDefault(); ev.stopPropagation(); fastIntercomButtonTest('bigMic_click'); }catch(e){} },true);
      b.addEventListener('pointerdown',intercomDown,false);
      b.addEventListener('pointerup',intercomUp,false);
      b.addEventListener('pointercancel',intercomUp,false);
      b.addEventListener('pointerleave',intercomUp,false);
      b.addEventListener('touchstart',intercomDown,{passive:false});
      b.addEventListener('touchend',intercomUp,{passive:false});
      b.addEventListener('mousedown',intercomDown,false);
      b.addEventListener('mouseup',intercomUp,false);
      b.addEventListener('contextmenu',function(ev){ev.preventDefault();},false);
    }
    var end=document.getElementById('btnEndTalk');
    if(end){
      end.addEventListener('click',function(ev){ev.preventDefault();ev.stopPropagation(); if(talking) intercomUp(ev); else { fastIntercomButtonTest('lowerTalk_click'); }},false);
    }
  }
  window.addEventListener('mouseup',function(){ if(talking)intercomUp(); }); window.addEventListener('pageshow',function(){ resumeAudioForIntercom('pageshow'); }); document.addEventListener('visibilitychange',function(){ if(document.visibilityState==='visible') resumeAudioForIntercom('visible'); });
  bind('btnAiVoice', startVoiceAsk); // V5.1A: btnVoice is intercom PTT, do not bind to AI voice
  // V5.1J: complex PTT disabled for route test; direct click bind above is used.
  // bindIntercomPtt();
  var lastCount=null;
  async function pollDoor(){ try{ var r=await fetch('/api/rt7/doorbell/state?_='+Date.now(),{cache:'no-store'}); var jj=await r.json(); var st=jj.state||jj; if(st&&typeof st.count==='number'){ if(lastCount===null) lastCount=st.count; if(st.count!==lastCount){ lastCount=st.count; showDoorbellInline(); } } }catch(e){} setTimeout(pollDoor,2500); }
  pollDoor();

  // V5.0D: Phone sleep / foreground-background auto recovery, based on original Node-RED design.
  // - Uses Screen Wake Lock when user starts video or taps page.
  // - Records whether the user wants video in localStorage.
  // - On visibilitychange/pageshow/focus/resize, restores the previous LAN/CLOUD stream.
  // - On background, notifies Railway to lower cloud FPS, but does not destroy the user's wanted state.
  var rt7WakeLock=null; var rt7RestoreBusy=false; var rt7RestoreTimer=null;
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
app.post('/api/rt7/phase6a_fix2/motion/event', (req,res)=>{ const ev=appendEvent(Object.assign({ type:'motion', message:'ESP32 motion event' }, req.body || {})); broadcast('motion', ev); res.json({ok:true,event:ev}); });
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
  cloudState.last_snapshot = { ok:true, bytes:buf.length, time:nowIso(), source:'raw_post', device_id:safeString(req.query.device_id || req.headers['x-rt7-device-id'] || '#1'), ip:clientIp(req), url:'/api/rt7/camera/latest.jpg' };
  const ev=appendEvent({ type:'snapshot', bytes:buf.length, message:'snapshot uploaded' });
  broadcast('snapshot', cloudState.last_snapshot);
  res.json({ ok:true, snapshot:cloudState.last_snapshot, event:ev });
});
app.post('/api/rt7/camera/snapshot_json', (req,res)=>{
  ensureDataDir();
  const b64 = safeString(req.body?.image_b64 || req.body?.jpeg_b64 || req.body?.b64 || '').replace(/^data:image\/jpeg;base64,/, '');
  if (!b64) return res.status(400).json({ok:false,error:'image_b64 required'});
  const buf = Buffer.from(b64, 'base64');
  fs.writeFileSync(SNAPSHOT_FILE, buf);
  cloudState.last_snapshot = { ok:true, bytes:buf.length, time:nowIso(), source:'json_b64', device_id:safeString(req.body?.device_id || '#1'), ip:clientIp(req), url:'/api/rt7/camera/latest.jpg' };
  const ev=appendEvent({ type:'snapshot', bytes:buf.length, message:'snapshot uploaded json' });
  broadcast('snapshot', cloudState.last_snapshot);
  res.json({ ok:true, snapshot:cloudState.last_snapshot, event:ev });
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
app.get('/api/rt7/device/commands', (req,res)=>{ const id=normalizeDoorCommandDeviceId_(req.query.device_id||req.query.device||''); const list=id?pendingCommands.filter(c=>commandMatchesDevice_(c,id)):pendingCommands; res.json({ok:true, device_id:id, commands:list, count:list.length, state:doorOpenQueueState}); });
app.get('/api/rt7/device/commands/next', (req,res)=>{ const id=normalizeDoorCommandDeviceId_(req.query.device_id||req.query.device||''); const cmd=pendingCommands.find(c=>commandMatchesDevice_(c,id)) || null; res.json({ok:true, device_id:id, command:cmd, has_command:!!cmd, pending:pendingCommands.length, state:doorOpenQueueState}); });
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
  { group:'09 Intercom Audio', status:'restored-v5.1A', nodered:'/api/ind_full/audio/* local proxy to ESP32 audio endpoints', railway:'/api/ind_full/audio/* returns compatibility JSON / queue scaffold', test:'Call begin/end endpoints; later add WebSocket PCM bridge one step at a time' }
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

wss.on('connection', (ws, req) => {
  ws.rt7Role = 'control';
  ws.rt7DeviceId = '';
  try { ws.send(JSON.stringify({ ok: true, type: 'hello', version: SERVER_VERSION, time: nowIso(), ws_frame:true })); } catch (_) {}
  ws.on('message', (data, isBinary) => {
    try {
      if (isBinary || Buffer.isBuffer(data)) {
        acceptWsStreamFrame_(Buffer.from(data), ws);
        return;
      }
      const txt = data.toString('utf8');
      let msg = null;
      try { msg = JSON.parse(txt); } catch (_) {}
      if (msg && msg.role) {
        ws.rt7Role = safeString(msg.role);
        ws.rt7DeviceId = safeString(msg.device_id || msg.device || msg.id || ws.rt7DeviceId || '#1');
        if (ws.rt7Role === 'viewer') streamViewers.set(safeString(msg.viewer_id || req.socket.remoteAddress || Math.random()), { ts:Date.now(), ip:req.socket.remoteAddress, state:'visible', ws:true });
        ws.send(JSON.stringify({ ok:true, type:'role_ack', role:ws.rt7Role, version:SERVER_VERSION, time:nowIso() }));
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

