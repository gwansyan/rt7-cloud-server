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

const SERVER_VERSION = 'RT7_CLOUD_SERVER_V3';

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
  res.type('html').send(htmlShell('RT7 Cloud Server V3', `${baseCss}
<header class="top"><h1>RT7 CLOUD SERVER V3</h1><p>Doorbell + Event Logger + Device Registry + WebSocket</p></header>
<main class="wrap">
<section class="card"><h2 class="ok">Server OK</h2><p>Railway Node.js Server is running.</p>
<a class="btn" href="/rt7_cloud_doorbell_player">雲端門鈴播放器</a>
<a class="btn" href="/rt7_cloud_admin">雲端管理頁</a>
<a class="btn" href="/api/rt7/doorbell/state">門鈴狀態 JSON</a>
<a class="btn" href="/api/events/latest">事件紀錄 JSON</a>
</section>
<section class="card"><h3>部署策略</h3><p>V3 採「一個功能一個功能」搬移，保留 Node-RED 對照文件，方便日後維護。</p></section>
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
app.post('/api/rt7/doorbell/ring', (req, res) => handleDoorbell(req, res, 'legacy_ring'));
app.post('/api/doorbell', (req, res) => handleDoorbell(req, res, 'cloud_v3'));
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

wss.on('connection', (ws) => {
  ws.send(JSON.stringify({ ok: true, type: 'hello', version: SERVER_VERSION, time: nowIso() }));
});

ensureDataDir();
const port = process.env.PORT || 3000;
server.listen(port, () => console.log(`${SERVER_VERSION} listening on ${port}`));
