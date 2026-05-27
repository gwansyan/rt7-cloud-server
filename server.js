const express = require('express');
const fs = require('fs');
const path = require('path');
const http = require('http');
const WebSocket = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: '/ws' });

app.use(express.json({ limit: '4mb' }));
app.use(express.urlencoded({ extended: true, limit: '4mb' }));

const DATA_DIR = process.env.RT7_DATA_DIR || path.join(__dirname, 'data');
const EVENT_LOG = path.join(DATA_DIR, 'rt7_event_log.jsonl');
const DEVICES_FILE = path.join(DATA_DIR, 'rt7_devices.json');

const SERVER_VERSION = 'RT7_CLOUD_SERVER_V4_3D_FORCE_AUDIO_UNLOCK';

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

function registerOrUpdateDevice(dev) {
  const devices = readDevices();
  const idx = devices.findIndex(d => (d.id && d.id === dev.id) || (dev.ip && d.ip === dev.ip));
  if (idx >= 0) devices[idx] = Object.assign({}, devices[idx], dev);
  else devices.push(dev);
  saveDevices(devices);
  return dev;
}

function htmlShell(title, body, extraHead = '') {
  return `<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no"><title>${title}</title>${extraHead}</head><body>${body}</body></html>`;
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
  res.type('html').send(`<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no"><title>RT7 Cloud Original UI Doorbell</title>
<style>
:root{--dark:#0b252b;--dark2:#0d2c32;--red:#ef2b24;--blue:#17a8e5;--green:#22a951;--text:#17262a;--line:#e5e7eb;--orange:#9a3b18}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}html,body{margin:0;padding:0;background:#fff;color:var(--text);font-family:system-ui,-apple-system,"Noto Sans TC","Microsoft JhengHei",Arial,sans-serif}body{max-width:520px;margin:0 auto;min-height:100vh;padding-bottom:32px}.top{height:66px;background:linear-gradient(90deg,var(--dark),var(--dark2));color:#fff;display:flex;align-items:center;justify-content:space-between;padding:0 16px;font-weight:900}.hamb{font-size:34px;line-height:1}.title{text-align:center;line-height:1.15;font-size:17px;letter-spacing:.4px}.deviceBar{padding:8px 12px;background:#fff;border-bottom:1px solid var(--line)}select{width:100%;height:34px;border:1px solid #334155;border-radius:4px;font-weight:800;padding:0 8px;background:#fff}.video{position:relative;background:#000;aspect-ratio:4/3;overflow:hidden}.video img{width:100%;height:100%;object-fit:cover;background:#000;display:block}.emptyVideo{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;color:#cbd5e1;font-weight:900;font-size:18px}.badge{position:absolute;top:12px;border-radius:7px;padding:7px 12px;color:white;font-weight:900;box-shadow:0 2px 8px rgba(0,0,0,.22)}.idle{left:14px;background:#71839d}.live{right:14px;background:var(--red)}.videoBtns{position:absolute;left:12px;right:12px;bottom:12px;display:flex;justify-content:space-between;gap:8px}.videoBtns .leftBtns,.videoBtns .rightBtns{display:flex;gap:8px}.vbtn{border:1px solid rgba(255,255,255,.55);border-radius:9px;color:#fff;font-weight:900;padding:9px 12px;font-size:14px;min-width:72px}.vblue{background:var(--blue)}.vred{background:var(--red)}.vdark{background:#102a31}.statusLine{min-height:46px;display:grid;grid-template-columns:1fr 1fr;gap:8px;border-bottom:1px solid var(--line);align-items:center;padding:8px 12px;background:#fff;font-size:15px;font-weight:800}.statusLine .dot{display:inline-block;width:11px;height:11px;border-radius:50%;background:var(--green);margin-right:8px}.answer{color:#5b1f14}.door{color:#8a2f15;text-align:right}.doorAlert{grid-column:1/3;background:#fff7ed;border:1px solid #fed7aa;color:#9a3412;border-radius:12px;padding:12px;font-size:22px;font-weight:900;text-align:center;display:none}.micZone{text-align:center;padding:18px 0 8px}.bigMic{width:128px;height:128px;border-radius:50%;border:3px solid #cbd5e1;background:#eef2f7;display:inline-flex;align-items:center;justify-content:center;font-size:72px;box-shadow:0 4px 18px rgba(20,40,60,.08)}.actions{display:flex;justify-content:center;gap:10px;padding:10px 8px 4px}.act{width:66px;text-align:center;font-size:12px;font-weight:900;color:#24333a}.circle{width:58px;height:58px;border:3px solid var(--red);border-radius:50%;background:#fff;display:flex;align-items:center;justify-content:center;font-size:28px;margin:0 auto 4px;box-shadow:0 2px 10px rgba(0,0,0,.1)}.reg{display:flex;align-items:center;gap:10px;padding:8px 20px}.reg label{font-size:14px;font-weight:900}.reg input{flex:1;height:32px;border:1px solid #cbd5e1;border-radius:7px;padding:0 10px}.panel{margin:12px;border:1px solid var(--line);border-radius:14px;padding:12px;background:#fff}.panel .grid{display:grid;grid-template-columns:1fr 1fr;gap:10px}.btn{border:0;border-radius:10px;color:#fff;font-size:17px;font-weight:900;padding:12px}.green{background:#18a34a}.blue{background:#0b84d8}.gray{background:#475569}.red{background:#dc2626}.hint{font-weight:900;color:#9a3b18;line-height:1.5;margin-top:8px}.json{background:#08101f;color:#d8f2ff;border-radius:12px;padding:12px;white-space:pre-wrap;font-family:ui-monospace,Consolas,monospace;font-size:12px;max-height:260px;overflow:auto;margin:12px}.small{font-size:12px;color:#64748b}.audioOverlay{position:fixed;inset:0;background:rgba(7,20,28,.86);z-index:9999;display:flex;align-items:center;justify-content:center;padding:22px}.audioCard{width:min(420px,92vw);background:#fff;border-radius:22px;padding:24px 18px;text-align:center;box-shadow:0 16px 50px rgba(0,0,0,.35);border:2px solid #dbeafe}.audioIcon{font-size:62px;line-height:1;margin-bottom:8px}.audioTitle{font-size:24px;font-weight:1000;color:#0f2b33;margin:6px 0}.audioText{font-size:15px;font-weight:800;color:#475569;line-height:1.55;margin:10px 0 18px}.audioStart{width:100%;border:0;border-radius:16px;background:var(--red);color:#fff;font-size:22px;font-weight:1000;padding:16px 14px;box-shadow:0 6px 18px rgba(239,43,36,.28)}.audioSmall{font-size:12px;color:#64748b;margin-top:10px;font-weight:700}@media(max-height:740px){.top{height:56px}.title{font-size:15px}.video{aspect-ratio:16/10}.bigMic{width:104px;height:104px;font-size:58px}.circle{width:50px;height:50px;font-size:24px}.act{font-size:11px}.statusLine{font-size:13px;min-height:38px}.reg{padding-top:4px}.panel{margin-top:6px}}
</style></head><body>
<header class="top"><div class="hamb">☰</div><div class="title">RT7 PHASE10<br>AI MODE ROUTER</div><div style="width:34px"></div></header>
<div id="audioOverlay" class="audioOverlay"><div class="audioCard"><div class="audioIcon">🔔</div><div class="audioTitle">啟用門鈴提示音</div><div class="audioText">手機瀏覽器需要先點一下，才能在有人按門鈴時自動播放 dingdong 提示音。</div><button class="audioStart" onclick="forceUnlockAudio()">點一下啟用聲音</button><div class="audioSmall">啟用後會播放一次測試提示音</div></div></div>
<div class="deviceBar"><select id="deviceSel"><option value="">讀取設備中...</option></select></div>
<section class="video"><div id="emptyVideo" class="emptyVideo">等待雲端 Snapshot<br><span class="small">ESP32 POST /api/rt7/camera/snapshot</span></div><img id="stream" alt=""><div class="badge idle">IDLE</div><div class="badge live">LIVE</div><div class="videoBtns"><div class="leftBtns"><button class="vbtn vblue" onclick="enableAi()">啟用 AI</button><button class="vbtn vred" onclick="disableAi()">關閉 AI</button></div><div class="rightBtns"><button class="vbtn vdark" onclick="refreshSnapshot(true)">更新照片</button><button class="vbtn vdark" onclick="stopVideo()">清除顯示</button></div></div></section>
<section class="statusLine"><div class="answer"><span class="dot"></span>回答：<span id="answerText">雲端門鈴待機中</span></div><div class="door">門鈴：<span id="doorText">等待事件</span></div><div id="doorAlert" class="doorAlert">🔔 有人按門鈴</div></section>
<section class="micZone"><button id="unlockBtn" class="bigMic" onclick="unlockAudio()" title="啟用提示音">🎙️</button></section>
<section class="actions"><div class="act"><div class="circle" onclick="manualDoor()">🚪</div>開門</div><div class="act"><div class="circle">👥</div>名單</div><div class="act"><div class="circle">◼</div>對講結束</div><div class="act"><div class="circle" onclick="registerName()">＋</div>註冊</div><div class="act"><div class="circle" onclick="unlockAudio()">🎙️</div>AI語音助理</div></section>
<div class="reg"><label>註冊名稱</label><input id="regName" value="gwansyan"></div>
<div id="json" style="display:none">ready</div>
<script>
const API_BASE = location.origin;
let DEVICES=[]; let lastCount=null; let audioOK=false; let audioCtx=null; let currentDevice=null; let lastSnapshotTime='';
let audioUnlockedOnce=false;
function $(id){return document.getElementById(id)}
function setJson(o){$('json').textContent=typeof o==='string'?o:JSON.stringify(o,null,2)}
function setDoor(msg){$('doorText').textContent=msg||''}
function setAnswer(msg){$('answerText').textContent=msg||''}
function selectedDevice(){return DEVICES.find(d=>d.id===$('deviceSel').value)||DEVICES[0]||{id:'#1',name:'RT7 ESP32-S3-CAM',ip:''}}
async function j(url,opt){const r=await fetch(url+(url.includes('?')?'&':'?')+'_='+Date.now(),Object.assign({cache:'no-store'},opt||{}));const t=await r.text();try{return JSON.parse(t)}catch(e){return {ok:r.ok,raw:t}}}
async function loadDevices(){try{let d=await j('/api/devices');DEVICES=d.devices||[];if(!DEVICES.length)DEVICES=[{id:'#1',name:'RT7 ESP32-S3-CAM',ip:''}];$('deviceSel').innerHTML=DEVICES.map(function(x){return '<option value="'+String(x.id||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/"/g,'&quot;')+'">'+(x.id||'')+' / '+(x.name||'')+(x.ip?' / '+x.ip:'')+'</option>';}).join('');currentDevice=selectedDevice();}catch(e){$('deviceSel').innerHTML='<option>#1 / RT7 ESP32-S3-CAM</option>';}}
function beep(freq,dur,delay){if(!audioCtx)return;const o=audioCtx.createOscillator();const g=audioCtx.createGain();o.type='sine';o.frequency.value=freq;g.gain.setValueAtTime(0.0001,audioCtx.currentTime+delay);g.gain.exponentialRampToValueAtTime(0.38,audioCtx.currentTime+delay+0.02);g.gain.exponentialRampToValueAtTime(0.0001,audioCtx.currentTime+delay+dur);o.connect(g);g.connect(audioCtx.destination);o.start(audioCtx.currentTime+delay);o.stop(audioCtx.currentTime+delay+dur+0.05)}
function setAudioUi(ok){
  if($('unlockBtn')){$('unlockBtn').style.borderColor=ok?'#22c55e':'#cbd5e1';$('unlockBtn').style.background=ok?'#ecfdf5':'#eef2f7'}
  if($('audioOverlay')) $('audioOverlay').style.display=ok?'none':'flex';
}
async function unlockAudio(silent=false){
  try{
    audioCtx=audioCtx||new (window.AudioContext||window.webkitAudioContext)();
    const osc=audioCtx.createOscillator(); const gain=audioCtx.createGain();
    gain.gain.value=0.0001; osc.connect(gain); gain.connect(audioCtx.destination); osc.start(); osc.stop(audioCtx.currentTime+0.01);
    await audioCtx.resume();
    audioOK=(audioCtx.state==='running'); audioUnlockedOnce=audioOK;
    if(audioOK){try{localStorage.setItem('rt7_audio_unlocked','1')}catch(_){}; setAudioUi(true); if(!silent){setAnswer('提示音已啟用'); setTimeout(playDingDong,80);} return true;}
    if(!silent)setAnswer('請再點一次「啟用聲音」'); setAudioUi(false); return false;
  }catch(e){ if(!silent)setAnswer('提示音啟用失敗：'+e.message); setAudioUi(false); return false; }
}
async function forceUnlockAudio(){ await unlockAudio(false); }
async function ensureAudio(){
  if(audioOK&&audioCtx&&audioCtx.state!=='closed'){
    if(audioCtx.state==='suspended') await audioCtx.resume();
    if(audioCtx.state==='running') return true;
  }
  setAudioUi(false);
  return false;
}
async function playDingDong(){const ok=await ensureAudio();if(!ok){setAnswer('請先點「啟用門鈴提示音」');return;}beep(988,0.16,0);beep(784,0.22,0.24)}
function showDoorbell(last, shouldSound){$('doorAlert').style.display='block';setDoor('有人按門鈴 #' + (last.count||''));setAnswer('收到雲端門鈴訊息'); if(shouldSound) playDingDong(); if(navigator.vibrate) navigator.vibrate([120,80,120]); setTimeout(()=>{$('doorAlert').style.display='none'},5000)}
async function loadState(manual=false){try{const s=await j('/api/rt7/doorbell/state');setJson(s);const st=s.state||{};const c=Number(st.count||0);const last=st.last||{};if(last.time){setDoor('最後：'+new Date(last.time).toLocaleTimeString());} if(lastCount===null){lastCount=c;} else if(c>lastCount){showDoorbell(last,true);lastCount=c;} if(manual&&last.message){showDoorbell(last,false);} lastCount=c;}catch(e){setAnswer('讀取失敗 '+e.message)}}
async function testDoorbell(){const d=selectedDevice();const s=await j('/api/test/doorbell?ip='+encodeURIComponent(d.ip||'web')+'&device='+encodeURIComponent(d.id||'#1'));setJson(s);await loadState(true)}
function registerName(){const name=($('regName')&&$('regName').value)||'gwansyan';setAnswer('註冊名稱：'+name+'（雲端版先保留原始 UI）');}
function resetLocal(){lastCount=null;$('doorAlert').style.display='none';setDoor('本機顯示已重設');setAnswer('雲端門鈴待機中')}
function enableAi(){setAnswer('AI 已啟用（雲端 UI）')} function disableAi(){setAnswer('AI 已關閉（雲端 UI）')} function manualDoor(){setDoor('開門：此版尚未連接雲端開門 API')}
async function refreshSnapshot(manual=false){
  try{
    const s=await j('/api/rt7/camera/state');
    setJson(s);
    if(s.latest_url && s.snapshot){
      const t=s.snapshot.time||'';
      if(t!==lastSnapshotTime || manual){
        lastSnapshotTime=t;
        $('emptyVideo').style.display='none';
        $('stream').src=s.latest_url+'?_='+Date.now();
        setAnswer('最新 Snapshot：'+(t?new Date(t).toLocaleTimeString():'已更新'));
      }
    }else if(manual){
      $('stream').removeAttribute('src'); $('stream').src=''; $('emptyVideo').style.display='flex';
      setAnswer('尚無雲端 Snapshot，請先由 ESP32 上傳照片');
    }
  }catch(e){ if(manual) setAnswer('讀取 Snapshot 失敗：'+e.message); }
}
function startVideo(){refreshSnapshot(true)}
function stopVideo(){$('stream').removeAttribute('src');$('stream').src='';$('emptyVideo').style.display='flex';setAnswer('影像顯示已清除，雲端照片仍保留')}
function wsConnect(){try{const ws=new WebSocket((location.protocol==='https:'?'wss://':'ws://')+location.host+'/ws');ws.onmessage=e=>{try{const m=JSON.parse(e.data);if(m.type==='doorbell'&&m.payload){loadState(false);showDoorbell(m.payload,true);refreshSnapshot(false)} if(m.type==='snapshot'){refreshSnapshot(true)}}catch(_){}};ws.onclose=()=>setTimeout(wsConnect,3000)}catch(e){}}
$('deviceSel').addEventListener('change',()=>{currentDevice=selectedDevice();setAnswer('已切換 '+(currentDevice.name||currentDevice.id));});
setAudioUi(false); loadDevices().then(()=>{loadState(true);refreshSnapshot(true)}); setInterval(()=>{loadState(false);refreshSnapshot(false)},2200); wsConnect();
</script></body></html>`);
});


// ============================================================================
// V4 NO-NODERED PHASE10 BRIDGE
// Goal: migrate the original Node-RED RT7 image/intercom/access-control routes
// into Railway Express. Railway cannot directly reach a private LAN ESP32 unless
// the ESP32 is exposed by VPN/Tailscale/tunnel, so routes support two modes:
//   1) Cloud-native ingest: ESP32 POSTs snapshots/events to Railway.
//   2) Optional proxy: if a device has a public/tunnel URL or reachable IP, Railway proxies.
// ============================================================================
const SNAPSHOT_FILE = path.join(DATA_DIR, 'rt7_latest_snapshot.jpg');
let cloudState = {
  ai_enabled: true,
  plugins: { motion: true, face: true, doorbell: true, intercom: true },
  last_snapshot: null,
  last_vision: null,
  last_voice: null,
  last_proxy: null,
  last_door_open: null,
  current_device_id: '#1'
};

function getCurrentDevice(req) {
  const devices = readDevices();
  const qid = safeString(req.query.device_id || req.query.device || '').trim();
  const qip = safeString(req.query.ip || '').trim();
  if (qip) return { id: qid || 'query', name: 'query device', ip: qip, base_url: qip.startsWith('http') ? qip : ('http://' + qip) };
  let dev = devices.find(d => safeString(d.id) === qid) || devices.find(d => safeString(d.id) === cloudState.current_device_id) || devices[0] || defaultDevices()[0];
  const base = safeString(dev.base_url || dev.url || dev.ip).trim();
  return Object.assign({}, dev, { base_url: base ? (base.startsWith('http') ? base : ('http://' + base)) : '' });
}

function isReachableDeviceUrl(baseUrl) {
  if (!baseUrl) return false;
  if (/^https?:\/\/(127\.0\.0\.1|localhost)/i.test(baseUrl)) return false;
  // Private RFC1918 addresses are not reachable from Railway unless routed by a tunnel.
  // We still allow them when RT7_ALLOW_PRIVATE_PROXY=1 for local testing.
  if (process.env.RT7_ALLOW_PRIVATE_PROXY === '1') return true;
  if (/^https?:\/\/(10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.)/i.test(baseUrl)) return false;
  return true;
}

async function proxyToEsp(req, res, espPath, method) {
  const dev = getCurrentDevice(req);
  if (!isReachableDeviceUrl(dev.base_url)) {
    const payload = { ok:false, mode:'cloud_bridge', error:'ESP32_NOT_REACHABLE_FROM_RAILWAY', message:'Railway 無法直接連到區網 ESP32 IP。請改用 ESP32 主動 POST 雲端端點，或設定 Tailscale/公開 HTTPS URL 後存到設備管理。', device:dev, path:espPath };
    cloudState.last_proxy = Object.assign({ time: nowIso() }, payload);
    appendEvent({ type:'proxy_skip', path:espPath, device_id:dev.id, ip:dev.ip || dev.base_url, message:payload.message });
    return res.status(200).json(payload);
  }
  try {
    const qs = new URLSearchParams(req.query || {}); qs.delete('_'); qs.delete('ip'); qs.delete('device_id'); qs.delete('device');
    const url = dev.base_url.replace(/\/$/,'') + espPath + (qs.toString() ? ('?' + qs.toString()) : '');
    const options = { method: method || req.method, headers: {} };
    if (req.method === 'POST') {
      if (typeof req.body === 'string' || Buffer.isBuffer(req.body)) options.body = req.body;
      else options.body = JSON.stringify(req.body || {}), options.headers['Content-Type'] = 'application/json';
    }
    const r = await fetch(url, options);
    const ct = r.headers.get('content-type') || 'text/plain';
    const buf = Buffer.from(await r.arrayBuffer());
    cloudState.last_proxy = { ok:r.ok, status:r.status, url, time:nowIso() };
    res.status(r.status).type(ct).send(buf);
  } catch (e) {
    const payload = { ok:false, error:'PROXY_FAILED', message:String(e.message || e), path:espPath, device:dev };
    cloudState.last_proxy = Object.assign({ time: nowIso() }, payload);
    appendEvent({ type:'proxy_error', path:espPath, message:payload.message, device_id:dev.id });
    res.status(200).json(payload);
  }
}

// Node-RED compatible event aliases
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
  try { const q = safeString(req.query.q || req.query.question || '請問鏡頭目前看到什麼？'); res.json(await analyzeLatestSnapshot(q)); }
  catch(e) { res.status(200).json({ok:false, mode:'VISION', error:String(e.message||e), answer:'雲端 Vision 分析失敗。'}); }
}
app.get('/api/rt7/phase9a/vision_qa', handleVisionQa);
app.get('/api/rt7/phase9b/vision_qa', handleVisionQa);
app.get('/api/rt7/phase9g/vision_qa', handleVisionQa);
app.get('/api/rt7/phase9i/vision_qa', handleVisionQa);
app.get('/api/rt7/phase9d/vision_qa_ping', (req,res)=>res.json({ok:true, version:SERVER_VERSION, latest_snapshot:cloudState.last_snapshot}));
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

// Door open: compatible endpoint. If ESP32 is not reachable, records a cloud command for ESP32 polling.
let pendingCommands = [];
function queueCommand(cmd) { const c=Object.assign({id:'cmd_'+Date.now(), time:nowIso()}, cmd); pendingCommands.push(c); pendingCommands=pendingCommands.slice(-50); broadcast('command', c); appendEvent({type:'command', command:c.command, message:c.message||c.command}); return c; }
app.get('/api/rt7/phase9l/door/open', async (req,res)=>{
  const dev=getCurrentDevice(req);
  if (isReachableDeviceUrl(dev.base_url)) return proxyToEsp(req,res,'/api/door/open','GET');
  const cmd=queueCommand({ command:'door_open', device_id:dev.id, message:'雲端開門命令已排入佇列，等待 ESP32 輪詢' });
  cloudState.last_door_open = cmd;
  res.json({ ok:true, mode:'cloud_command_queue', command:cmd, note:'ESP32 可輪詢 /api/rt7/device/commands?device_id=#1 取得命令' });
});
app.get('/api/rt7/device/commands', (req,res)=>{ const id=safeString(req.query.device_id||req.query.device||''); const list=id?pendingCommands.filter(c=>!c.device_id||c.device_id===id):pendingCommands; res.json({ok:true, commands:list}); });
app.post('/api/rt7/device/commands/ack', (req,res)=>{ const id=safeString(req.body?.id||req.query.id); pendingCommands=pendingCommands.filter(c=>c.id!==id); res.json({ok:true, pending:pendingCommands.length}); });

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
  { group:'05 Vision QA', status:'partial', nodered:'GET /api/rt7/phase9i/vision_qa', railway:'GET /api/rt7/phase9i/vision_qa uses latest uploaded snapshot + OpenAI if OPENAI_API_KEY exists', test:'Upload snapshot, ask question, verify answer' },
  { group:'06 Voice Vision Router', status:'partial', nodered:'POST /api/rt7/phase9j/voice_vision', railway:'POST /api/rt7/phase9j/voice_vision text-mode scaffold; audio upload reserved', test:'POST {text:"請問鏡頭看到什麼"}' },
  { group:'07 Door Open Queue', status:'partial', nodered:'GET /api/rt7/phase9l/door/open direct local ESP32 request', railway:'GET /api/rt7/phase9l/door/open queues command; ESP32 polls /api/rt7/device/commands', test:'GET door/open then GET device/commands' },
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


wss.on('connection', (ws) => {
  ws.send(JSON.stringify({ ok: true, type: 'hello', version: SERVER_VERSION, time: nowIso() }));
});

ensureDataDir();
const port = process.env.PORT || 3000;
server.listen(port, () => console.log(`${SERVER_VERSION} listening on ${port}`));
