// RT7_V6_0A_ICATCH_PROTOCOL_BRIDGE
// iCATCH / SoCatch DVR LAN Bridge for RT7 Multi-Camera AI Gate
//
// 目的：在 DVR 同一內網的 Windows 電腦執行，取得 iCATCH DVR CH01~CH04 影像，主動上傳 Railway。
// 已知 DVR：iCATCH RMH-0428EU-K A3，SoCatch App 可連線，瀏覽器開 192.168.0.123:80 會 ERR_EMPTY_RESPONSE。
//
// 支援模式：
// 1) ICATCH_RTSP_TEMPLATE / DVR_RTSP_TEMPLATE：使用你填入的正確 RTSP URL。
// 2) 自動探測：嘗試 iCATCH/SoCatch/ONVIF/常見 RTSP/HTTP Snapshot。
// 3) 手動資料夾模式：若 DVR 只能由原廠程式匯出 JPG，可設定 RT7_BRIDGE_SOURCE_DIR，Bridge 會讀取 CH01.jpg~CH04.jpg 上傳。
//
// 必填：RAILWAY_URL=https://你的-railway.up.railway.app
// 常用：DVR_HOST=192.168.0.123 DVR_USER=admin DVR_PASS= DVR_CHANNELS=1,2,3,4

const { spawn, execFile } = require('child_process');
const http = require('http');
const https = require('https');
const net = require('net');
const fs = require('fs');
const path = require('path');

const RAILWAY_URL = (process.env.RAILWAY_URL || '').replace(/\/+$/, '');
const TOKEN = process.env.RT7_DVR_BRIDGE_TOKEN || 'rt7-dvr-bridge';
const DVR_HOST = process.env.DVR_HOST || '192.168.0.123';
const DVR_USER = process.env.DVR_USER || 'admin';
const DVR_PASS = process.env.DVR_PASS || '';
const DVR_RTSP_PORT = process.env.DVR_RTSP_PORT || '554';
const DVR_HTTP_PORT = process.env.DVR_HTTP_PORT || process.env.DVR_PORT || '80';
const CHANNELS = String(process.env.DVR_CHANNELS || '1,2,3,4').split(',').map(s => parseInt(s.trim(), 10)).filter(Boolean);
const INTERVAL_MS = Math.max(1000, parseInt(process.env.INTERVAL_MS || '3000', 10) || 3000);
const RTSP_TIMEOUT_MS = Math.max(3000, parseInt(process.env.RTSP_TIMEOUT_MS || '12000', 10) || 12000);
const SOURCE_DIR = (process.env.RT7_BRIDGE_SOURCE_DIR || '').trim();
const DEBUG = String(process.env.DEBUG || '').trim() === '1';
const PROBE_ONLY = String(process.env.PROBE_ONLY || '').trim() === '1';

function enc(v) { return encodeURIComponent(String(v || '')); }
function mask(url) { return String(url).replace(/:([^:@/]*?)@/, ':****@').replace(/password=[^&]*/gi, 'password=****').replace(/pwd=[^&]*/gi, 'pwd=****'); }
function isJpeg(buf) { return buf && buf.length > 4 && buf[0] === 0xff && buf[1] === 0xd8; }
function isProbablyImage(buf) { return isJpeg(buf) || (buf && buf.length > 8 && buf.slice(1,4).toString() === 'PNG'); }
function padCh(ch) { return 'CH' + String(ch).padStart(2, '0'); }

function fill(tpl, ch) {
  const ch0 = String(Math.max(0, Number(ch) - 1));
  return tpl
    .replace(/\{host\}/g, DVR_HOST)
    .replace(/\{rtsp_port\}/g, DVR_RTSP_PORT)
    .replace(/\{http_port\}/g, DVR_HTTP_PORT)
    .replace(/\{port\}/g, DVR_RTSP_PORT)
    .replace(/\{user\}/g, enc(DVR_USER))
    .replace(/\{pass\}/g, enc(DVR_PASS))
    .replace(/\{password\}/g, enc(DVR_PASS))
    .replace(/\{ch\}/g, String(ch))
    .replace(/\{ch0\}/g, ch0)
    .replace(/\{channel\}/g, String(ch));
}

// iCATCH/SoCatch candidates. 這些是探測用；若全部失敗，代表此機型需從 SoCatch/ONVIF 抓真實 URL，或使用手動資料夾模式。
const ICATCH_RTSP_TEMPLATES = [
  'rtsp://{user}:{pass}@{host}:{rtsp_port}/user={user}&password={pass}&channel={ch}&stream=0.sdp',
  'rtsp://{user}:{pass}@{host}:{rtsp_port}/user={user}&password={pass}&channel={ch}&stream=1.sdp',
  'rtsp://{host}:{rtsp_port}/user={user}&password={pass}&channel={ch}&stream=0.sdp',
  'rtsp://{host}:{rtsp_port}/user={user}&password={pass}&channel={ch}&stream=1.sdp',
  'rtsp://{user}:{pass}@{host}:{rtsp_port}/live/ch{ch}',
  'rtsp://{user}:{pass}@{host}:{rtsp_port}/live/ch{ch0}',
  'rtsp://{user}:{pass}@{host}:{rtsp_port}/ch{ch}/main',
  'rtsp://{user}:{pass}@{host}:{rtsp_port}/ch{ch}/sub',
  'rtsp://{user}:{pass}@{host}:{rtsp_port}/video{ch}',
  'rtsp://{user}:{pass}@{host}:{rtsp_port}/av{ch}_0',
  'rtsp://{user}:{pass}@{host}:{rtsp_port}/av{ch}_1',
  'rtsp://{user}:{pass}@{host}:{rtsp_port}/{ch}',
  'rtsp://{host}:{rtsp_port}/chID={ch}&streamType=main',
  'rtsp://{user}:{pass}@{host}:{rtsp_port}/chID={ch}&streamType=main',
  // Generic compatibility
  'rtsp://{user}:{pass}@{host}:{rtsp_port}/cam/realmonitor?channel={ch}&subtype=0',
  'rtsp://{user}:{pass}@{host}:{rtsp_port}/Streaming/Channels/{ch}01',
  'rtsp://{user}:{pass}@{host}:{rtsp_port}/ISAPI/Streaming/channels/{ch}01'
];
const ICATCH_HTTP_TEMPLATES = [
  'http://{host}:{http_port}/cgi-bin/snapshot.cgi?channel={ch}',
  'http://{host}:{http_port}/cgi-bin/snapshot.cgi?chn={ch0}',
  'http://{host}:{http_port}/snapshot.cgi?channel={ch}',
  'http://{host}:{http_port}/snapshot.cgi?chn={ch0}',
  'http://{user}:{pass}@{host}:{http_port}/cgi-bin/snapshot.cgi?channel={ch}',
  'http://{user}:{pass}@{host}:{http_port}/cgi-bin/snapshot.cgi?chn={ch0}',
  'http://{host}:{http_port}/cgi-bin/hi3510/snap.cgi?chn={ch0}&u={user}&p={pass}',
  'http://{host}:{http_port}/webcapture.jpg?command=snap&channel={ch}',
  'http://{host}:{http_port}/image.jpg?channel={ch}',
  'http://{host}:{http_port}/jpg/image.jpg?channel={ch}'
];

const customRtsp = process.env.ICATCH_RTSP_TEMPLATE || process.env.DVR_RTSP_TEMPLATE || '';
const customHttp = process.env.ICATCH_SNAPSHOT_TEMPLATE || process.env.DVR_SNAPSHOT_TEMPLATE || '';
const RTSP_TEMPLATES = customRtsp ? [customRtsp] : ICATCH_RTSP_TEMPLATES;
const HTTP_TEMPLATES = customHttp ? [customHttp] : ICATCH_HTTP_TEMPLATES;

function requestBuffer(urlText, timeoutMs=5000) {
  return new Promise(resolve => {
    let u;
    try { u = new URL(urlText); } catch (e) { return resolve({ ok:false, error:'BAD_URL ' + e.message, url:urlText }); }
    const lib = u.protocol === 'https:' ? https : http;
    const headers = { 'User-Agent':'RT7-iCATCH-Bridge/6.0A', 'Accept':'image/jpeg,image/*,*/*' };
    if (DVR_USER && !urlText.includes('@')) headers.Authorization = 'Basic ' + Buffer.from(DVR_USER + ':' + DVR_PASS).toString('base64');
    const req = lib.request(u, { method:'GET', headers, timeout:timeoutMs }, res => {
      const chunks = [];
      res.on('data', d => chunks.push(d));
      res.on('end', () => {
        const buffer = Buffer.concat(chunks);
        resolve({ ok:res.statusCode >= 200 && res.statusCode < 300 && isProbablyImage(buffer), status:res.statusCode, buffer, contentType:res.headers['content-type'] || '', url:urlText });
      });
    });
    req.on('timeout', () => { try { req.destroy(new Error('HTTP timeout')); } catch (_) {} });
    req.on('error', e => resolve({ ok:false, error:String(e.message || e), url:urlText }));
    req.end();
  });
}

function tcpProbe(port, payload) {
  return new Promise(resolve => {
    const sock = new net.Socket();
    const chunks = [];
    const timer = setTimeout(() => { try { sock.destroy(); } catch (_) {} resolve({ ok:false, port, error:'timeout' }); }, 2500);
    sock.connect(Number(port), DVR_HOST, () => {
      if (payload) sock.write(payload);
      else setTimeout(() => { try { sock.end(); } catch (_) {} }, 500);
    });
    sock.on('data', d => chunks.push(d));
    sock.on('close', () => { clearTimeout(timer); const b = Buffer.concat(chunks); resolve({ ok:true, port, bytes:b.length, hex:b.slice(0,32).toString('hex'), text:b.slice(0,80).toString('latin1').replace(/[^\x20-\x7E]/g,'.') }); });
    sock.on('error', e => { clearTimeout(timer); resolve({ ok:false, port, error:String(e.message || e) }); });
  });
}

function checkFfmpeg() {
  return new Promise(resolve => {
    execFile('ffmpeg', ['-version'], { windowsHide:true, timeout:3000 }, (err, stdout) => {
      if (err) return resolve({ ok:false, error:String(err.message || err) });
      resolve({ ok:true, version:String(stdout || '').split('\n')[0] });
    });
  });
}
function ffmpegSnapshot(rtspUrl) {
  return new Promise(resolve => {
    const args = ['-hide_banner','-loglevel',DEBUG?'info':'error','-rtsp_transport','tcp','-i',rtspUrl,'-frames:v','1','-q:v','3','-f','image2pipe','-vcodec','mjpeg','-'];
    const p = spawn('ffmpeg', args, { windowsHide:true });
    const chunks = [];
    let err = '';
    const timer = setTimeout(() => { try { p.kill('SIGKILL'); } catch (_) {} }, RTSP_TIMEOUT_MS);
    p.stdout.on('data', d => chunks.push(d));
    p.stderr.on('data', d => { err += d.toString(); });
    p.on('close', code => { clearTimeout(timer); const buf=Buffer.concat(chunks); resolve({ ok:isJpeg(buf), code, buffer:buf, error:err.trim().slice(0,900), rtspUrl }); });
    p.on('error', e => { clearTimeout(timer); resolve({ ok:false, error:String(e.message || e), rtspUrl }); });
  });
}
function upload(ch, jpeg, source='icatch') {
  return new Promise(resolve => {
    if (!RAILWAY_URL) return resolve({ ok:false, error:'RAILWAY_URL_EMPTY' });
    const id = padCh(ch);
    const url = new URL(RAILWAY_URL + '/api/rt7/dvr/bridge/upload/' + encodeURIComponent(id) + '?source=' + encodeURIComponent(source));
    const lib = url.protocol === 'https:' ? https : http;
    const req = lib.request(url, { method:'POST', headers:{ 'Content-Type':'image/jpeg', 'Content-Length':jpeg.length, 'X-RT7-Bridge-Token':TOKEN, 'User-Agent':'RT7-iCATCH-Bridge/6.0A' }, timeout:10000 }, res => {
      const chunks=[]; res.on('data',d=>chunks.push(d)); res.on('end',()=>resolve({ ok:res.statusCode>=200&&res.statusCode<300, status:res.statusCode, body:Buffer.concat(chunks).toString('utf8') }));
    });
    req.on('timeout',()=>{ try{req.destroy(new Error('upload timeout'));}catch(_){} resolve({ok:false,error:'upload timeout'}); });
    req.on('error', e=>resolve({ ok:false, error:String(e.message || e) }));
    req.end(jpeg);
  });
}

function readManualFrame(ch) {
  if (!SOURCE_DIR) return null;
  const names = [`CH${String(ch).padStart(2,'0')}.jpg`, `CH${String(ch).padStart(2,'0')}.jpeg`, `ch${ch}.jpg`, `${ch}.jpg`];
  for (const n of names) {
    const f = path.join(SOURCE_DIR, n);
    try { if (fs.existsSync(f)) { const b=fs.readFileSync(f); if (isJpeg(b)) return { ok:true, buffer:b, file:f }; } } catch (_) {}
  }
  return null;
}

const working = new Map();
async function tryHttp(ch) {
  for (const tpl of HTTP_TEMPLATES) {
    const url = fill(tpl, ch);
    const r = await requestBuffer(url, 4500);
    if (r.ok) return { ok:true, buffer:r.buffer, url, kind:'http' };
    if (DEBUG) console.log(`[${padCh(ch)}] HTTP fail`, r.status || r.error, mask(url));
  }
  return { ok:false };
}
async function tryRtsp(ch) {
  const templates = working.has(ch) ? [working.get(ch), ...RTSP_TEMPLATES.filter(t => t !== working.get(ch))] : RTSP_TEMPLATES;
  let last='';
  for (const tpl of templates) {
    const url = fill(tpl, ch);
    const r = await ffmpegSnapshot(url);
    if (r.ok) { working.set(ch, tpl); return { ok:true, buffer:r.buffer, url, kind:'rtsp' }; }
    last = r.error || ('ffmpeg code ' + r.code);
    console.log(`[${padCh(ch)}] RTSP fail ${last.slice(0,120)} ${mask(url)}`);
  }
  return { ok:false, error:last };
}
async function oneChannel(ch) {
  const manual = readManualFrame(ch);
  if (manual) {
    const up = await upload(ch, manual.buffer, 'manual-folder');
    console.log(`[${padCh(ch)}] manual=${manual.file} jpeg=${manual.buffer.length} upload=${up.ok?'OK':'FAIL'} ${up.status||''} ${up.error||''}`);
    return;
  }
  const h = await tryHttp(ch);
  if (h.ok) {
    const up = await upload(ch, h.buffer, 'icatch-http');
    console.log(`[${padCh(ch)}] HTTP jpeg=${h.buffer.length} upload=${up.ok?'OK':'FAIL'} ${up.status||''} ${mask(h.url)}`);
    return;
  }
  const r = await tryRtsp(ch);
  if (r.ok) {
    const up = await upload(ch, r.buffer, 'icatch-rtsp');
    console.log(`[${padCh(ch)}] RTSP jpeg=${r.buffer.length} upload=${up.ok?'OK':'FAIL'} ${up.status||''} ${mask(r.url)}`);
    return;
  }
  console.log(`[${padCh(ch)}] iCATCH auto templates failed. 建議：1) 用 ONVIF Device Manager 查 Media URL，2) 設定 ICATCH_RTSP_TEMPLATE，3) 若 SoCatch 只能專用協定，先用 RT7_BRIDGE_SOURCE_DIR 手動資料夾模式。`);
}
async function diagnostic() {
  console.log('--- iCATCH diagnostic ---');
  for (const p of [80, 554, 5000, 5001, 6000, 34567, 37777, 8899, 8080, 8000]) {
    const r = await tcpProbe(p, p === 80 ? Buffer.from('GET / HTTP/1.0\r\n\r\n') : null);
    console.log(`[TCP ${p}]`, r.ok ? `open bytes=${r.bytes} ${r.text || r.hex || ''}` : r.error);
  }
  console.log('--- end diagnostic ---');
}
async function loop() {
  for (const ch of CHANNELS) await oneChannel(ch);
  setTimeout(loop, INTERVAL_MS);
}

(async () => {
  console.log('RT7_V6_0A_ICATCH_PROTOCOL_BRIDGE starting...');
  console.log('Railway:', RAILWAY_URL || '(missing RAILWAY_URL)');
  console.log('DVR:', `iCATCH ${DVR_USER}@${DVR_HOST} http=${DVR_HTTP_PORT} rtsp=${DVR_RTSP_PORT} channels=${CHANNELS.join(',')}`);
  console.log('Token:', TOKEN ? '(set)' : '(empty)');
  if (SOURCE_DIR) console.log('Manual source dir:', SOURCE_DIR);
  const ff = await checkFfmpeg();
  console.log('FFmpeg:', ff.ok ? ff.version : ('not found: ' + ff.error));
  await diagnostic();
  if (!RAILWAY_URL) console.log('[ERROR] RAILWAY_URL_EMPTY，請設定 Railway 網址。');
  if (PROBE_ONLY) { console.log('PROBE_ONLY=1 finished.'); return; }
  console.log('Press Ctrl+C to stop.');
  loop();
})();
