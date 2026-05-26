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

const SERVER_VERSION = 'RT7_CLOUD_SERVER_V3_ORIGINAL_UI_DOORBELL';

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
<a class="btn" href="/rt7_cloud_original_ui_doorbell">原始 UI 雲端門鈴</a>
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



// ---------- Original RT7 mobile-style cloud doorbell UI ----------
app.get('/rt7_cloud_original_ui_doorbell', (req, res) => {
  res.type('html').send(`<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no"><title>RT7 Cloud Original UI Doorbell</title>
<style>
:root{--dark:#0b252b;--dark2:#0d2c32;--red:#ef2b24;--blue:#17a8e5;--green:#22a951;--text:#17262a;--line:#e5e7eb;--orange:#9a3b18}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}html,body{margin:0;padding:0;background:#fff;color:var(--text);font-family:system-ui,-apple-system,"Noto Sans TC","Microsoft JhengHei",Arial,sans-serif}body{max-width:520px;margin:0 auto;min-height:100vh;padding-bottom:32px}.top{height:66px;background:linear-gradient(90deg,var(--dark),var(--dark2));color:#fff;display:flex;align-items:center;justify-content:space-between;padding:0 16px;font-weight:900}.hamb{font-size:34px;line-height:1}.title{text-align:center;line-height:1.15;font-size:17px;letter-spacing:.4px}.deviceBar{padding:8px 12px;background:#fff;border-bottom:1px solid var(--line)}select{width:100%;height:34px;border:1px solid #334155;border-radius:4px;font-weight:800;padding:0 8px;background:#fff}.video{position:relative;background:#000;aspect-ratio:4/3;overflow:hidden}.video img{width:100%;height:100%;object-fit:cover;background:#000;display:block}.emptyVideo{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;color:#cbd5e1;font-weight:900;font-size:18px}.badge{position:absolute;top:12px;border-radius:7px;padding:7px 12px;color:white;font-weight:900;box-shadow:0 2px 8px rgba(0,0,0,.22)}.idle{left:14px;background:#71839d}.live{right:14px;background:var(--red)}.videoBtns{position:absolute;left:12px;right:12px;bottom:12px;display:flex;justify-content:space-between;gap:8px}.videoBtns .leftBtns,.videoBtns .rightBtns{display:flex;gap:8px}.vbtn{border:1px solid rgba(255,255,255,.55);border-radius:9px;color:#fff;font-weight:900;padding:9px 12px;font-size:14px;min-width:72px}.vblue{background:var(--blue)}.vred{background:var(--red)}.vdark{background:#102a31}.statusLine{min-height:46px;display:grid;grid-template-columns:1fr 1fr;gap:8px;border-bottom:1px solid var(--line);align-items:center;padding:8px 12px;background:#fff;font-size:15px;font-weight:800}.statusLine .dot{display:inline-block;width:11px;height:11px;border-radius:50%;background:var(--green);margin-right:8px}.answer{color:#5b1f14}.door{color:#8a2f15;text-align:right}.doorAlert{grid-column:1/3;background:#fff7ed;border:1px solid #fed7aa;color:#9a3412;border-radius:12px;padding:12px;font-size:22px;font-weight:900;text-align:center;display:none}.micZone{text-align:center;padding:18px 0 8px}.bigMic{width:128px;height:128px;border-radius:50%;border:3px solid #cbd5e1;background:#eef2f7;display:inline-flex;align-items:center;justify-content:center;font-size:72px;box-shadow:0 4px 18px rgba(20,40,60,.08)}.actions{display:flex;justify-content:center;gap:10px;padding:10px 8px 4px}.act{width:66px;text-align:center;font-size:12px;font-weight:900;color:#24333a}.circle{width:58px;height:58px;border:3px solid var(--red);border-radius:50%;background:#fff;display:flex;align-items:center;justify-content:center;font-size:28px;margin:0 auto 4px;box-shadow:0 2px 10px rgba(0,0,0,.1)}.reg{display:flex;align-items:center;gap:10px;padding:8px 20px}.reg label{font-size:14px;font-weight:900}.reg input{flex:1;height:32px;border:1px solid #cbd5e1;border-radius:7px;padding:0 10px}.panel{margin:12px;border:1px solid var(--line);border-radius:14px;padding:12px;background:#fff}.panel .grid{display:grid;grid-template-columns:1fr 1fr;gap:10px}.btn{border:0;border-radius:10px;color:#fff;font-size:17px;font-weight:900;padding:12px}.green{background:#18a34a}.blue{background:#0b84d8}.gray{background:#475569}.red{background:#dc2626}.hint{font-weight:900;color:#9a3b18;line-height:1.5;margin-top:8px}.json{background:#08101f;color:#d8f2ff;border-radius:12px;padding:12px;white-space:pre-wrap;font-family:ui-monospace,Consolas,monospace;font-size:12px;max-height:260px;overflow:auto;margin:12px}.small{font-size:12px;color:#64748b}@media(max-height:740px){.top{height:56px}.title{font-size:15px}.video{aspect-ratio:16/10}.bigMic{width:104px;height:104px;font-size:58px}.circle{width:50px;height:50px;font-size:24px}.act{font-size:11px}.statusLine{font-size:13px;min-height:38px}.reg{padding-top:4px}.panel{margin-top:6px}}
</style></head><body>
<header class="top"><div class="hamb">☰</div><div class="title">RT7 PHASE10<br>AI MODE ROUTER</div><div style="width:34px"></div></header>
<div class="deviceBar"><select id="deviceSel"><option value="">讀取設備中...</option></select></div>
<section class="video"><div id="emptyVideo" class="emptyVideo">雲端門鈴模式<br><span class="small">影像串流下一版雲端化</span></div><img id="stream" alt=""><div class="badge idle">IDLE</div><div class="badge live">LIVE</div><div class="videoBtns"><div class="leftBtns"><button class="vbtn vblue" onclick="enableAi()">啟用 AI</button><button class="vbtn vred" onclick="disableAi()">關閉 AI</button></div><div class="rightBtns"><button class="vbtn vdark" onclick="startVideo()">開始影像</button><button class="vbtn vdark" onclick="stopVideo()">停止影像</button></div></div></section>
<section class="statusLine"><div class="answer"><span class="dot"></span>回答：<span id="answerText">雲端門鈴待機中</span></div><div class="door">門鈴：<span id="doorText">等待事件</span></div><div id="doorAlert" class="doorAlert">🔔 有人按門鈴</div></section>
<section class="micZone"><button id="unlockBtn" class="bigMic" onclick="unlockAudio()" title="啟用提示音">🎙️</button></section>
<section class="actions"><div class="act"><div class="circle" onclick="manualDoor()">🚪</div>開門</div><div class="act"><div class="circle">👥</div>名單</div><div class="act"><div class="circle">◼</div>對講結束</div><div class="act"><div class="circle" onclick="testDoorbell()">＋</div>測試</div><div class="act"><div class="circle" onclick="unlockAudio()">🎙️</div>AI語音助理</div></section>
<div class="reg"><label>註冊名稱</label><input id="regName" value="gwansyan"></div>
<section class="panel"><div class="grid"><button class="btn green" onclick="unlockAudio()">啟用提示音</button><button class="btn blue" onclick="playDingDong()">測試提示音</button><button class="btn gray" onclick="loadState(true)">立即讀取</button><button class="btn red" onclick="resetLocal()">本機重設顯示</button></div><div class="hint">手機瀏覽器通常需要先按一次「啟用提示音」或麥克風圖示，之後門鈴事件才可自動播放。</div></section>
<pre id="json" class="json">ready</pre>
<script>
const API_BASE = location.origin;
let DEVICES=[]; let lastCount=null; let audioOK=false; let audioCtx=null; let currentDevice=null;
function $(id){return document.getElementById(id)}
function setJson(o){$('json').textContent=typeof o==='string'?o:JSON.stringify(o,null,2)}
function setDoor(msg){$('doorText').textContent=msg||''}
function setAnswer(msg){$('answerText').textContent=msg||''}
function selectedDevice(){return DEVICES.find(d=>d.id===$('deviceSel').value)||DEVICES[0]||{id:'#1',name:'RT7 ESP32-S3-CAM',ip:''}}
async function j(url,opt){const r=await fetch(url+(url.includes('?')?'&':'?')+'_='+Date.now(),Object.assign({cache:'no-store'},opt||{}));const t=await r.text();try{return JSON.parse(t)}catch(e){return {ok:r.ok,raw:t}}}
async function loadDevices(){try{let d=await j('/api/devices');DEVICES=d.devices||[];if(!DEVICES.length)DEVICES=[{id:'#1',name:'RT7 ESP32-S3-CAM',ip:''}];$('deviceSel').innerHTML=DEVICES.map(x=>`<option value="${x.id}">${x.id} / ${x.name||''}${x.ip?' / '+x.ip:''}</option>`).join('');currentDevice=selectedDevice();}catch(e){$('deviceSel').innerHTML='<option>#1 / RT7 ESP32-S3-CAM</option>';}}
function beep(freq,dur,delay){if(!audioCtx)return;const o=audioCtx.createOscillator();const g=audioCtx.createGain();o.type='sine';o.frequency.value=freq;g.gain.setValueAtTime(0.0001,audioCtx.currentTime+delay);g.gain.exponentialRampToValueAtTime(0.25,audioCtx.currentTime+delay+0.02);g.gain.exponentialRampToValueAtTime(0.0001,audioCtx.currentTime+delay+dur);o.connect(g);g.connect(audioCtx.destination);o.start(audioCtx.currentTime+delay);o.stop(audioCtx.currentTime+delay+dur+0.03)}
async function unlockAudio(){try{audioCtx=audioCtx||new (window.AudioContext||window.webkitAudioContext)();await audioCtx.resume();audioOK=true;setAnswer('提示音已啟用');$('unlockBtn').style.borderColor='#22c55e';playDingDong();}catch(e){setAnswer('提示音啟用失敗：'+e.message)}}
function playDingDong(){if(!audioCtx){setAnswer('請先啟用提示音');return;}beep(880,0.18,0);beep(660,0.22,0.26)}
function showDoorbell(last, shouldSound){$('doorAlert').style.display='block';setDoor('有人按門鈴 #' + (last.count||''));setAnswer('收到雲端門鈴訊息'); if(shouldSound&&audioOK) playDingDong(); setTimeout(()=>{$('doorAlert').style.display='none'},5000)}
async function loadState(manual=false){try{const s=await j('/api/rt7/doorbell/state');setJson(s);const st=s.state||{};const c=Number(st.count||0);const last=st.last||{};if(last.time){setDoor('最後：'+new Date(last.time).toLocaleTimeString());} if(lastCount===null){lastCount=c;} else if(c>lastCount){showDoorbell(last,true);lastCount=c;} if(manual&&last.message){showDoorbell(last,false);} lastCount=c;}catch(e){setAnswer('讀取失敗 '+e.message)}}
async function testDoorbell(){const d=selectedDevice();const s=await j('/api/test/doorbell?ip='+encodeURIComponent(d.ip||'web')+'&device='+encodeURIComponent(d.id||'#1'));setJson(s);await loadState(true)}
function resetLocal(){lastCount=null;$('doorAlert').style.display='none';setDoor('本機顯示已重設');setAnswer('雲端門鈴待機中')}
function enableAi(){setAnswer('AI 已啟用（雲端 UI）')} function disableAi(){setAnswer('AI 已關閉（雲端 UI）')} function manualDoor(){setDoor('開門：此版尚未連接雲端開門 API')}
function startVideo(){const d=selectedDevice();if(!d.ip){setAnswer('此設備沒有 IP，影像串流下一版雲端化');return;}$('emptyVideo').style.display='none';$('stream').src='http://'+d.ip+'/api/camera/stream?_='+Date.now();setAnswer('嘗試連線區網影像：'+d.ip)}
function stopVideo(){$('stream').removeAttribute('src');$('stream').src='';$('emptyVideo').style.display='flex';setAnswer('影像已停止')}
function wsConnect(){try{const ws=new WebSocket((location.protocol==='https:'?'wss://':'ws://')+location.host+'/ws');ws.onmessage=e=>{try{const m=JSON.parse(e.data);if(m.type==='doorbell'&&m.payload){loadState(false);showDoorbell(m.payload,true)}}catch(_){}};ws.onclose=()=>setTimeout(wsConnect,3000)}catch(e){}}
$('deviceSel').addEventListener('change',()=>{currentDevice=selectedDevice();setAnswer('已切換 '+(currentDevice.name||currentDevice.id));});
loadDevices().then(()=>loadState(true)); setInterval(()=>loadState(false),2200); wsConnect();
</script></body></html>`);
});

wss.on('connection', (ws) => {
  ws.send(JSON.stringify({ ok: true, type: 'hello', version: SERVER_VERSION, time: nowIso() }));
});

ensureDataDir();
const port = process.env.PORT || 3000;
server.listen(port, () => console.log(`${SERVER_VERSION} listening on ${port}`));
