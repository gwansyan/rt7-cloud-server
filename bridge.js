// RT7_V6_2M_ICATCH_STRICT_VIDEOLOSS_REJECT_FIX
// iCATCH / SoCatch DVR net_video.cgi LAN Bridge
//
// 根據 PCAPdroid 已確認真正影像 API：
//   GET /cgi-bin/net_video.cgi?hq=0&iframe=15&pframe=15&audio=0
//   Authorization: Basic admin:vbnmmnbv
//   Magic: 39e739de-8d69-aadb-78b9-946a2905858d
//   Response: multipart/x-mixed-replace + application/octet-stream
//
// 本 Bridge 做法：
// 1) 用同內網 Windows 電腦連 iCATCH DVR 的 net_video.cgi。
// 2) 交給 ffmpeg 嘗試解 H.264/octet-stream。
// 3) 擷取 JPEG frame 上傳 Railway /api/rt7/dvr/bridge/upload/CHxx。
//
// 若 ffmpeg 無法解析，代表 iCATCH octet-stream 有私有封包頭，下一版需加入 depacketizer。

const { spawn, execFile } = require('child_process');
const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const os = require('os');
let jpegjs = null;
try { jpegjs = require('jpeg-js'); } catch (_) { jpegjs = null; }

const VERSION = 'RT7_V6_2M_ICATCH_STRICT_VIDEOLOSS_REJECT_FIX';
const RAILWAY_URL = (process.env.RAILWAY_URL || '').replace(/\/+$/, '');
const TOKEN = process.env.RT7_DVR_BRIDGE_TOKEN || 'rt7-dvr-bridge';
const DVR_HOST = process.env.DVR_HOST || '192.168.0.123';
const DVR_USER = process.env.DVR_USER || 'admin';
const DVR_PASS = process.env.DVR_PASS || 'vbnmmnbv';
const DVR_HTTP_PORT = process.env.DVR_HTTP_PORT || process.env.DVR_PORT || '80';
// V6.2B: PCAPdroid 確認 iCATCH net_video.cgi 沒有 channel 參數。
// 同一個 net_video.cgi 其實是目前 SoCatch/主畫面的單一影像來源；
// 若同時把同一 URL 當成 CH01~CH04 擷取，會造成 CH2/CH4 顯示 CH1 的畫面。
// 因此預設只上傳 CH01。若未來抓到真正的 channel 參數，再設定 DVR_CHANNELS=1,2,3,4 與 ICATCH_NET_VIDEO_TEMPLATE。
const CHANNELS = String(process.env.DVR_CHANNELS || '1').split(',').map(s => parseInt(s.trim(), 10)).filter(Boolean);
const INTERVAL_MS = Math.max(200, parseInt(process.env.INTERVAL_MS || '1000', 10) || 1000);
const TRUE_STREAM_MODE = String(process.env.TRUE_STREAM_MODE || '1').trim() !== '0';
// V6.2H: 3FPS low-delay balance.
// 保留安全解碼避免 V6.2F 藍綠屏；降低到 3FPS，Bridge 僅保留最新 frame，避免排隊造成延遲。
const STREAM_FPS = Math.max(1, Math.min(8, parseInt(process.env.STREAM_FPS || '3', 10) || 3));
const UPLOAD_MIN_INTERVAL_MS = Math.max(100, parseInt(process.env.UPLOAD_MIN_INTERVAL_MS || String(Math.floor(1000 / STREAM_FPS)), 10) || Math.floor(1000 / STREAM_FPS));
const FFMPEG_TIMEOUT_MS = Math.max(2500, parseInt(process.env.FFMPEG_TIMEOUT_MS || '10000', 10) || 10000);
const DEBUG = String(process.env.DEBUG || '').trim() === '1';
const PROBE_ONLY = String(process.env.PROBE_ONLY || '').trim() === '1';
const TEST_ONLY = String(process.env.TEST_ONLY || '').trim() === '1';
const TEST_OUT = process.env.TEST_OUT || path.join(__dirname, 'icatch_ch1_test.jpg');

const RAW_ONLY = String(process.env.RAW_ONLY || '').trim() === '1';
const RAW_OUT = process.env.RAW_OUT || path.join(__dirname, 'icatch_ch1_raw.bin');
const RAW_BYTES = Math.max(65536, parseInt(process.env.RAW_BYTES || '524288', 10) || 524288);
const MAGIC = process.env.ICATCH_MAGIC || process.env.DVR_MAGIC || '39e739de-8d69-aadb-78b9-946a2905858d';
// V6.2L: fallback VIDEO LOSS filter. Blue VIDEO LOSS frames from this DVR are much smaller
// than real CH01 frames in your tests (~7KB vs 11~26KB). This works even if jpeg-js is not installed.
const MIN_REAL_JPEG_BYTES = Math.max(0, parseInt(process.env.MIN_REAL_JPEG_BYTES || '9000', 10) || 0);

// V6.2I Direct LAN View: 在 Bridge 電腦本機也提供最新 JPEG / MJPEG，
// 手機與 Bridge 電腦同一 LAN 時可直接連，避開 Railway 中轉降低延遲。
const LOCAL_HTTP_PORT = Math.max(0, parseInt(process.env.LOCAL_HTTP_PORT || '8787', 10) || 0);
const LOCAL_HTTP_BIND = process.env.LOCAL_HTTP_BIND || '0.0.0.0';
const LOCAL_PUBLIC_HOST = process.env.LOCAL_PUBLIC_HOST || autoLanHost(); // auto 192.168.x.x:8787
function autoLanHost() {
  try {
    const nets = os.networkInterfaces();
    const addrs = [];
    for (const arr of Object.values(nets)) for (const n of (arr || [])) {
      if (n && n.family === 'IPv4' && !n.internal) addrs.push(n.address);
    }
    const preferred = addrs.find(a => /^192\.168\.0\./.test(a)) ||
                      addrs.find(a => /^192\.168\./.test(a)) ||
                      addrs.find(a => /^10\./.test(a)) ||
                      addrs.find(a => /^172\.(1[6-9]|2\d|3[0-1])\./.test(a)) ||
                      addrs[0];
    return preferred ? (preferred + ':' + LOCAL_HTTP_PORT) : '';
  } catch (_) { return ''; }
}
const LOCAL_LATEST = {}; // { CH01: { jpeg, ts, seq, bytes } }
function rememberLocalFrame(ch, jpeg) {
  const id = padCh(ch);
  if (!Buffer.isBuffer(jpeg) || jpeg.length < 4) return;
  const old = LOCAL_LATEST[id] || { seq: 0 };
  LOCAL_LATEST[id] = { jpeg, ts: Date.now(), seq: (old.seq || 0) + 1, bytes: jpeg.length };
}
function localFrameStatus() {
  return Object.keys(LOCAL_LATEST).sort().map(id => ({
    id, online: Date.now() - LOCAL_LATEST[id].ts < 10000,
    age_ms: Date.now() - LOCAL_LATEST[id].ts,
    seq: LOCAL_LATEST[id].seq,
    bytes: LOCAL_LATEST[id].bytes
  }));
}
function startLocalLanServer() {
  if (!LOCAL_HTTP_PORT) return;
  const boundary = 'rt7lanboundary';
  const srv = http.createServer((req, res) => {
    const u = new URL(req.url, 'http://127.0.0.1');
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Headers', '*');
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    if (u.pathname === '/' || u.pathname === '/direct') {
      res.setHeader('Content-Type', 'text/html; charset=utf-8');
      const host = LOCAL_PUBLIC_HOST || req.headers.host || ('127.0.0.1:' + LOCAL_HTTP_PORT);
      return res.end(`<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>RT7 Direct LAN Bridge</title><style>body{margin:0;background:#071f25;color:white;font-family:system-ui,-apple-system,'Noto Sans TC',sans-serif}.wrap{max-width:760px;margin:0 auto;padding:14px}.card{background:white;color:#10212b;border-radius:18px;padding:14px;margin:12px 0}.view{background:#000;border-radius:14px;overflow:hidden}.view img{width:100%;display:block}.btn{display:inline-block;background:#0ea5e9;color:#fff;text-decoration:none;border-radius:10px;padding:10px 12px;font-weight:900;margin:4px}</style></head><body><div class="wrap"><h1>RT7 V6.2M Direct LAN View</h1><div class="card"><b>LAN Bridge：</b>http://${host}<br><b>CH01：</b>/stream/CH01.mjpg<br>此頁由 Bridge 電腦直接提供，不經 Railway，延遲最低。V6.2M 已強化藍底 VIDEO LOSS 過濾。</div><div class="card"><div class="view"><img id="img" src="/latest/CH01.jpg?ts=${Date.now()}"></div><p id="meta">Direct stable poll starting...</p><p><a class="btn" href="/latest/CH01.jpg?ts=${Date.now()}">看單張</a><a class="btn" href="/direct-mjpeg">MJPEG備用</a><a class="btn" href="/status">狀態 JSON</a></p></div><script>let seq=0;async function tick(){try{const r=await fetch('/status?ts='+Date.now(),{cache:'no-store'});const j=await r.json();const c=(j.cameras||[]).find(x=>x.id==='CH01');if(c&&c.seq!==seq){seq=c.seq;const im=new Image();im.onload=()=>{document.getElementById('img').src=im.src;document.getElementById('meta').textContent='ONLINE seq='+seq+' age_ms='+c.age_ms+' bytes='+c.bytes;};im.src='/latest/CH01.jpg?seq='+seq+'&ts='+Date.now();}}catch(e){document.getElementById('meta').textContent='poll error '+e;}setTimeout(tick,250);}tick();</script></div></body></html>`);
    }
    if (u.pathname === '/direct-mjpeg') {
      res.setHeader('Content-Type', 'text/html; charset=utf-8');
      return res.end(`<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><title>RT7 Direct MJPEG</title><style>body{margin:0;background:#071f25;color:white;font-family:system-ui}.wrap{padding:12px;max-width:760px;margin:auto}.view{background:#000;border-radius:14px;overflow:hidden}.view img{width:100%;display:block}.btn{display:inline-block;background:#0ea5e9;color:#fff;text-decoration:none;border-radius:10px;padding:10px;margin-top:10px}</style></head><body><div class="wrap"><h2>RT7 Direct MJPEG 備用</h2><div class="view"><img src="/stream/CH01.mjpg?ts=${Date.now()}"></div><a class="btn" href="/direct">回穩定輪詢</a></div></body></html>`);
    }
    if (u.pathname === '/status') {
      res.setHeader('Content-Type', 'application/json; charset=utf-8');
      return res.end(JSON.stringify({ ok: true, version: VERSION, port: LOCAL_HTTP_PORT, bind: LOCAL_HTTP_BIND, public_host: LOCAL_PUBLIC_HOST, cameras: localFrameStatus() }, null, 2));
    }
    const mLatest = u.pathname.match(/^\/latest\/(CH\d+)\.jpg$/i);
    if (mLatest) {
      const id = mLatest[1].toUpperCase();
      const f = LOCAL_LATEST[id];
      if (!f || !f.jpeg) { res.statusCode = 404; return res.end('NO_LOCAL_FRAME'); }
      res.setHeader('Content-Type', 'image/jpeg');
      res.setHeader('X-RT7-Seq', String(f.seq));
      res.setHeader('X-RT7-Age-Ms', String(Date.now() - f.ts));
      return res.end(f.jpeg);
    }
    const mStream = u.pathname.match(/^\/stream\/(CH\d+)\.mjpg$/i);
    if (mStream) {
      const id = mStream[1].toUpperCase();
      res.writeHead(200, {
        'Content-Type': 'multipart/x-mixed-replace; boundary=' + boundary,
        'Cache-Control': 'no-store, no-cache, must-revalidate, proxy-revalidate',
        'Pragma': 'no-cache',
        'Connection': 'close',
        'Access-Control-Allow-Origin': '*'
      });
      let lastSeq = -1;
      const timer = setInterval(() => {
        const f = LOCAL_LATEST[id];
        if (!f || !f.jpeg || f.seq === lastSeq) return;
        lastSeq = f.seq;
        try {
          res.write(`--${boundary}\r\nContent-Type: image/jpeg\r\nContent-Length: ${f.jpeg.length}\r\nX-RT7-Seq: ${f.seq}\r\nX-RT7-Age-Ms: ${Date.now()-f.ts}\r\n\r\n`);
          res.write(f.jpeg);
          res.write('\r\n');
        } catch (_) {}
      }, Math.max(80, Math.floor(1000 / Math.max(1, STREAM_FPS))));
      req.on('close', () => clearInterval(timer));
      return;
    }
    res.statusCode = 404; res.end('RT7_LOCAL_NOT_FOUND');
  });
  srv.listen(LOCAL_HTTP_PORT, LOCAL_HTTP_BIND, () => {
    console.log(`[LAN] Direct LAN View server http://${LOCAL_HTTP_BIND}:${LOCAL_HTTP_PORT}/  public=${LOCAL_PUBLIC_HOST || '(auto host unavailable)'}`);
  });
  srv.on('error', e => console.log('[LAN] local server error', e.message || e));
}

// V6.2B: 單一 net_video.cgi 來源鎖定 CH01，避免把同一畫面誤標成 CH02/CH03/CH04。
// V6.1D: iCATCH/SoCatch net_video.cgi 可在 Authorization + Magic 下直接串流。
// /dvr/cmd 登入可能 status=200 但沒有 Set-Cookie；這是可接受狀態。
// 因此 Cookie 不再是必要條件，也不在每張 frame 前重複登入。
let RT7_SESSION_COOKIE = '';
let RT7_LOGIN_OK = false;
let RT7_LOGIN_TRIED = false;

// SoCatch 目前抓到的 URL 沒有 channel 參數。部分 iCATCH DVR 會以 hq/chn/ch 參數選通道；
// 因此保留 template 可測。若封包抓到更完整 URL，可直接在 BAT 設 ICATCH_NET_VIDEO_TEMPLATE。
const NET_VIDEO_TEMPLATE = process.env.ICATCH_NET_VIDEO_TEMPLATE ||
  'http://{host}:{port}/cgi-bin/net_video.cgi?hq=0&iframe=15&pframe=15&audio=0';

function padCh(ch) { return 'CH' + String(ch).padStart(2, '0'); }
function enc(v) { return encodeURIComponent(String(v == null ? '' : v)); }
function fill(tpl, ch) {
  const ch0 = String(Math.max(0, Number(ch) - 1));
  return String(tpl)
    .replace(/\{host\}/g, DVR_HOST)
    .replace(/\{port\}/g, DVR_HTTP_PORT)
    .replace(/\{http_port\}/g, DVR_HTTP_PORT)
    .replace(/\{user\}/g, enc(DVR_USER))
    .replace(/\{pass\}/g, enc(DVR_PASS))
    .replace(/\{password\}/g, enc(DVR_PASS))
    .replace(/\{ch\}/g, String(ch))
    .replace(/\{channel\}/g, String(ch))
    .replace(/\{ch0\}/g, ch0)
    .replace(/\{magic\}/g, MAGIC);
}
function mask(s) { return String(s).replace(/:([^:@/]*?)@/, ':****@').replace(/(password|pwd|pass|p)=([^&]*)/gi, '$1=****'); }
function isJpeg(buf) { return buf && buf.length > 4 && buf[0] === 0xff && buf[1] === 0xd8; }
function extractJpeg(buf) {
  if (!buf || !buf.length) return null;
  if (isJpeg(buf)) return buf;
  const start = buf.indexOf(Buffer.from([0xff, 0xd8]));
  if (start < 0) return null;
  const end = buf.indexOf(Buffer.from([0xff, 0xd9]), start + 2);
  if (end < 0) return null;
  return buf.slice(start, end + 2);
}

function authHeader() { return 'Basic ' + Buffer.from(DVR_USER + ':' + DVR_PASS).toString('base64'); }

function buildCommandMultipart(xmlText) {
  // PCAPdroid: POST /dvr/cmd, Content-Type multipart/form-data; boundary=----maya,
  // form-data name="datafile"; filename="command.xml"; Content-Type: text/xml
  const boundary = '----maya';
  const body = Buffer.from(
    '--' + boundary + '\r\n' +
    'Content-Disposition: form-data; name="datafile"; filename="command.xml"\r\n' +
    'Content-Type: text/xml\r\n\r\n' +
    xmlText + '\r\n' +
    '--' + boundary + '--\r\n',
    'utf8'
  );
  return { boundary, body };
}

function icatchLoginCookie(timeoutMs=5000) {
  return new Promise(resolve => {
    const xml = '<?xml version="1.0" encoding="UTF-8" ?><DVR Platform="Hi3520"><GetConfiguration File="system.xml" /></DVR>';
    const mp = buildCommandMultipart(xml);
    const options = {
      host: DVR_HOST,
      port: Number(DVR_HTTP_PORT) || 80,
      path: '/dvr/cmd',
      method: 'POST',
      timeout: timeoutMs,
      headers: {
        'Authorization': authHeader(),
        'Content-Type': 'multipart/form-data; boundary=' + mp.boundary,
        'Content-Length': mp.body.length,
        'User-Agent': 'SoCatch/RT7-V6.2A',
        'Connection': 'close'
      }
    };
    const req = http.request(options, res => {
      const chunks=[];
      res.on('data', d => { chunks.push(d); });
      res.on('end', () => {
        const text = Buffer.concat(chunks).toString('utf8');
        const setCookie = res.headers['set-cookie'];
        let cookie = '';
        if (Array.isArray(setCookie) && setCookie.length) cookie = String(setCookie[0]).split(';')[0];
        else if (setCookie) cookie = String(setCookie).split(';')[0];
        resolve({ ok:res.statusCode>=200 && res.statusCode<300, status:res.statusCode, cookie, headers:res.headers, bodySample:text.slice(0,500) });
      });
    });
    req.on('timeout', () => { try { req.destroy(new Error('login timeout')); } catch (_) {} });
    req.on('error', e => resolve({ ok:false, error:String(e.message || e) }));
    req.end(mp.body);
  });
}

async function initIcatchSessionOnce() {
  if (RT7_LOGIN_TRIED) return { ok: RT7_LOGIN_OK, cookie: RT7_SESSION_COOKIE, cached: true };
  RT7_LOGIN_TRIED = true;
  const login = await icatchLoginCookie();
  const body = String(login.bodySample || '');
  RT7_LOGIN_OK = !!(login.ok || login.status === 200 || body.includes('GetConfigurationResponse') || body.includes('Return="0"'));
  RT7_SESSION_COOKIE = login.cookie || '';
  if (RT7_SESSION_COOKIE) {
    console.log('[AUTH] login OK cookie=' + RT7_SESSION_COOKIE);
  } else if (RT7_LOGIN_OK) {
    console.log('[AUTH] login OK without cookie; sessionless mode enabled. status=' + (login.status || ''));
  } else {
    console.log('[AUTH] login warn status=' + (login.status || '') + ' error=' + (login.error || '') + ' body=' + body.replace(/\r?\n/g, ' ').slice(0, 160));
  }
  return { ok: RT7_LOGIN_OK, cookie: RT7_SESSION_COOKIE };
}

async function getVideoHeaders() {
  if (!RT7_LOGIN_TRIED) await initIcatchSessionOnce();
  const h = {
    'Authorization': authHeader(),
    'Magic': MAGIC,
    'User-Agent': 'SoCatch/RT7-V6.2A',
    'Accept': '*/*',
    'Connection': 'close'
  };
  if (RT7_SESSION_COOKIE) h['Cookie'] = RT7_SESSION_COOKIE;
  return h;
}

function headersToFfmpegText(headers) {
  let lines = '';
  for (const [k,v] of Object.entries(headers || {})) {
    if (v !== undefined && v !== null && String(v) !== '') lines += k + ': ' + String(v) + '\r\n';
  }
  return lines;
}

async function httpProbe(urlText, timeoutMs=5000) {
  const videoHeaders = await getVideoHeaders();
  return new Promise(resolve => {
    let u;
    try { u = new URL(urlText); } catch (e) { return resolve({ ok:false, error:'BAD_URL ' + e.message }); }
    const lib = u.protocol === 'https:' ? https : http;
    const headers = Object.assign({}, videoHeaders, { 'User-Agent': 'RT7-iCATCH-DirectLAN/6.2L' });
    const req = lib.request(u, { method:'GET', headers, timeout:timeoutMs }, res => {
      const chunks = [];
      res.on('data', d => {
        chunks.push(d);
        const len = chunks.reduce((a,b)=>a+b.length, 0);
        if (len > 8192) { try { req.destroy(); } catch (_) {} }
      });
      res.on('end', () => {
        const b = Buffer.concat(chunks);
        resolve({ ok:res.statusCode >= 200 && res.statusCode < 300, status:res.statusCode, headers:res.headers, bytes:b.length, sample:b.slice(0,240).toString('latin1').replace(/[^\x20-\x7E\r\n]/g,'.') });
      });
    });
    req.on('timeout', () => { try { req.destroy(new Error('probe timeout')); } catch (_) {} });
    req.on('error', e => resolve({ ok:false, error:String(e.message || e) }));
    req.end();
  });
}


async function httpRawDump(urlText, outFile, maxBytes=RAW_BYTES, timeoutMs=8000) {
  const videoHeaders = await getVideoHeaders();
  return new Promise(resolve => {
    let u;
    try { u = new URL(urlText); } catch (e) { return resolve({ ok:false, error:'BAD_URL ' + e.message }); }
    const lib = u.protocol === 'https:' ? https : http;
    const headers = Object.assign({}, videoHeaders, { 'User-Agent': 'SoCatch/RT7-V6.2A' });
    const req = lib.request(u, { method:'GET', headers, timeout:timeoutMs }, res => {
      let bytes = 0;
      const ws = fs.createWriteStream(outFile);
      res.on('data', d => {
        bytes += d.length;
        ws.write(d);
        if (bytes >= maxBytes) { try { req.destroy(); } catch (_) {} }
      });
      res.on('end', () => { ws.end(); resolve({ ok:res.statusCode>=200 && res.statusCode<300, status:res.statusCode, headers:res.headers, bytes, outFile }); });
      res.on('close', () => { ws.end(); resolve({ ok:res.statusCode>=200 && res.statusCode<300 && bytes>0, status:res.statusCode, headers:res.headers, bytes, outFile, closed:true }); });
    });
    req.on('timeout', () => { try { req.destroy(new Error('raw timeout')); } catch (_) {} });
    req.on('error', e => resolve({ ok:false, error:String(e.message || e), outFile }));
    req.end();
  });
}

async function ffmpegOneJpeg(urlText, ch) {
  const videoHeaders = await getVideoHeaders();
  return new Promise(resolve => {
    const headerText = headersToFfmpegText(Object.assign({}, videoHeaders, { 'User-Agent':'SoCatch/RT7-V6.2A' }));
    // 嘗試讓 ffmpeg 自動判斷 iCATCH multipart/octet-stream。輸出單張 MJPEG 到 stdout。
    const args = [
      '-hide_banner', '-nostdin',
      '-loglevel', DEBUG ? 'info' : 'error',
      '-headers', headerText,
      '-rw_timeout', String(Math.max(1000000, FFMPEG_TIMEOUT_MS * 1000)),
      '-i', urlText,
      '-frames:v', '1',
      '-q:v', '4',
      '-f', 'image2pipe',
      '-vcodec', 'mjpeg',
      'pipe:1'
    ];
    const child = spawn('ffmpeg', args, { windowsHide:true });
    const out = [];
    const err = [];
    const timer = setTimeout(() => { try { child.kill('SIGKILL'); } catch (_) {} }, FFMPEG_TIMEOUT_MS);
    child.stdout.on('data', d => out.push(d));
    child.stderr.on('data', d => err.push(d));
    child.on('close', code => {
      clearTimeout(timer);
      const b = Buffer.concat(out);
      const jpg = extractJpeg(b);
      const errText = Buffer.concat(err).toString('utf8').slice(-1800);
      resolve({ ok:!!jpg, code, jpeg:jpg, bytes:b.length, error:errText, url:urlText, ch });
    });
    child.on('error', e => { clearTimeout(timer); resolve({ ok:false, code:-1, error:String(e.message || e), url:urlText, ch }); });
  });
}


function rt7DetectVideoLossJpeg_(buf) {
  // V6.2M strict VIDEO LOSS reject.
  // The iCATCH DVR blue screen can be 7KB~16KB, so size alone is not enough.
  // Detect the large saturated-blue center area and reject it before saving LOCAL_LATEST.
  const out = { video_loss:false, reason:'', blue_ratio:0, center_blue_ratio:0, avg_r:0, avg_g:0, avg_b:0, samples:0, center_samples:0 };
  if (!jpegjs || !buf || buf.length < 128) return out;
  let img;
  try { img = jpegjs.decode(buf, { useTArray:true, maxMemoryUsageInMB:96 }); } catch (e) { out.reason='JPEG_DECODE_FAIL'; return out; }
  const w = img.width || 0, h = img.height || 0, data = img.data;
  if (!w || !h || !data) return out;

  const step = Math.max(3, Math.floor(Math.min(w,h) / 64));
  const x1 = Math.floor(w * 0.08), x2 = Math.floor(w * 0.92);
  const y1 = Math.floor(h * 0.18), y2 = Math.floor(h * 0.82);
  let n=0, blue=0, cn=0, cblue=0, sr=0, sg=0, sb=0;

  function isBlue(r,g,b) {
    // Covers pure DVR blue plus JPEG-compressed blue/purple blocks.
    return b > 75 && b > r + 28 && b > g + 20 && b > r * 1.35 && b > g * 1.18;
  }

  for (let y=0; y<h; y+=step) {
    for (let x=0; x<w; x+=step) {
      const i = (y*w + x) * 4;
      const r = data[i] || 0, g = data[i+1] || 0, b = data[i+2] || 0;
      // ignore black letterbox only; count white VIDEO LOSS text as non-blue.
      if (r < 16 && g < 16 && b < 16) continue;
      n++; sr += r; sg += g; sb += b;
      const ib = isBlue(r,g,b);
      if (ib) blue++;
      if (x >= x1 && x <= x2 && y >= y1 && y <= y2) {
        cn++;
        if (ib) cblue++;
      }
    }
  }
  if (n) { out.samples=n; out.blue_ratio=blue/n; out.avg_r=sr/n; out.avg_g=sg/n; out.avg_b=sb/n; }
  if (cn) { out.center_samples=cn; out.center_blue_ratio=cblue/cn; }

  out.video_loss = !!(n > 60 && (
    // Most reliable: the central view area is mostly saturated blue.
    out.center_blue_ratio > 0.46 ||
    // Fallback: overall image strongly blue-dominant.
    (out.blue_ratio > 0.36 && out.avg_b > 78 && out.avg_b > out.avg_r * 1.22 && out.avg_b > out.avg_g * 1.12) ||
    // Small blue frames are usually VIDEO LOSS from this DVR.
    (buf.length < 17500 && out.center_blue_ratio > 0.30 && out.avg_b > 80)
  ));
  if (out.video_loss) out.reason = 'STRICT_BLUE_VIDEO_LOSS_FRAME';
  return out;
}

function shouldSkipFrame(ch, frame, whyPrefix) {
  const id = padCh(ch);
  const hasGood = !!(LOCAL_LATEST[id] && LOCAL_LATEST[id].jpeg);
  if (MIN_REAL_JPEG_BYTES && frame && frame.length < MIN_REAL_JPEG_BYTES) {
    // Only skip tiny frames after at least one valid real frame exists, so startup can still recover.
    if (hasGood) return { skip:true, reason:'SMALL_FRAME_KEEP_LAST_GOOD', detail:'bytes=' + frame.length + ' min=' + MIN_REAL_JPEG_BYTES };
  }
  const vl = rt7DetectVideoLossJpeg_(frame);
  if (vl.video_loss) return { skip:true, reason:vl.reason || 'VIDEO_LOSS', detail:'blue=' + (vl.blue_ratio||0).toFixed(2) + ' avgB=' + (vl.avg_b||0).toFixed(1) };
  return { skip:false, detail:'ok' };
}

function upload(ch, jpeg, source='icatch-net-video') {
  return new Promise(resolve => {
    if (!RAILWAY_URL) return resolve({ ok:false, error:'RAILWAY_URL_EMPTY' });
    const id = padCh(ch);
    const url = new URL(RAILWAY_URL + '/api/rt7/dvr/bridge/upload/' + encodeURIComponent(id) + '?source=' + encodeURIComponent(source));
    const lib = url.protocol === 'https:' ? https : http;
    const req = lib.request(url, {
      method:'POST',
      headers:{ 'Content-Type':'image/jpeg', 'Content-Length':jpeg.length, 'X-RT7-Bridge-Token':TOKEN, 'User-Agent':'RT7-iCATCH-DirectLAN/6.2L', 'X-RT7-Single-Source':'CH01' },
      timeout:10000
    }, res => {
      const chunks=[];
      res.on('data',d=>chunks.push(d));
      res.on('end',()=>resolve({ ok:res.statusCode>=200&&res.statusCode<300, status:res.statusCode, body:Buffer.concat(chunks).toString('utf8').slice(0,300) }));
    });
    req.on('timeout',()=>{ try{req.destroy(new Error('upload timeout'));}catch(_){} resolve({ok:false,error:'upload timeout'}); });
    req.on('error', e=>resolve({ ok:false, error:String(e.message || e) }));
    req.end(jpeg);
  });
}

async function captureChannel(ch) {
  const url = fill(NET_VIDEO_TEMPLATE, ch);
  if (RAW_ONLY) {
    const rr = await httpRawDump(url, RAW_OUT);
    console.log('[RAW] ' + JSON.stringify({ ok:rr.ok, status:rr.status, bytes:rr.bytes, contentType:rr.headers && rr.headers['content-type'], outFile:rr.outFile, error:rr.error }, null, 2));
    console.log('[RAW] 如果 bytes > 0，代表 DVR net_video.cgi 已可讀；若 ffmpeg 不能轉 JPEG，表示需要下一版 depacketizer。');
    return;
  }
  const r = await ffmpegOneJpeg(url, ch);
  if (!r.ok) {
    console.log(`[${padCh(ch)}] ffmpeg FAIL code=${r.code} bytes=${r.bytes} url=${mask(url)}`);
    if (r.error) console.log(`[${padCh(ch)}] ffmpeg error: ${r.error.replace(/\r?\n/g, ' | ')}`);
    return r;
  }
  const skip = shouldSkipFrame(ch, r.jpeg);
  if (skip.skip) {
    console.log(`[${padCh(ch)}] frame=${r.jpeg.length} SKIP ${skip.reason} ${skip.detail} keep_last_good=1`);
    return Object.assign(r, { skipped:true, skip });
  }
  // V6.2K: Direct LAN local frame must be stored even when Railway upload is disabled/failed.
  // This fixes /direct showing NO_LOCAL_FRAME while DVR/FFmpeg test can already save JPEG.
  rememberLocalFrame(ch, r.jpeg);
  const up = await upload(ch, r.jpeg, 'icatch-net-video-ffmpeg-videoloss-filter');
  console.log(`[${padCh(ch)}] frame=${r.jpeg.length} local=OK upload=${up.ok?'OK':'FAIL'} ${up.status||''} ${up.error||''}`);
  return Object.assign(r, { upload:up });
}

async function testOne() {
  const ch = CHANNELS[0] || 1;
  const url = fill(NET_VIDEO_TEMPLATE, ch);
  console.log('Test URL:', mask(url));
  const p = await httpProbe(url, 5000);
  console.log('HTTP probe:', JSON.stringify({ ok:p.ok, status:p.status, bytes:p.bytes, contentType:p.headers && p.headers['content-type'], sample:p.sample || p.error }, null, 2));
  if (RAW_ONLY) {
    const rr = await httpRawDump(url, RAW_OUT);
    console.log('[RAW] ' + JSON.stringify({ ok:rr.ok, status:rr.status, bytes:rr.bytes, contentType:rr.headers && rr.headers['content-type'], outFile:rr.outFile, error:rr.error }, null, 2));
    console.log('[RAW] 如果 bytes > 0，代表 DVR net_video.cgi 已可讀；若 ffmpeg 不能轉 JPEG，表示需要下一版 depacketizer。');
    return;
  }
  const r = await ffmpegOneJpeg(url, ch);
  if (r.ok) {
    fs.writeFileSync(TEST_OUT, r.jpeg);
    const vl = rt7DetectVideoLossJpeg_(r.jpeg);
    fs.writeFileSync(TEST_OUT, r.jpeg);
    console.log(`[TEST] JPEG saved: ${TEST_OUT} bytes=${r.jpeg.length} video_loss=${vl.video_loss?'YES':'NO'} blue=${vl.blue_ratio ? vl.blue_ratio.toFixed(2) : '0.00'}`);
  } else {
    console.log(`[TEST] ffmpeg did not produce JPEG. code=${r.code} bytes=${r.bytes}`);
    if (r.error) console.log(r.error);
    console.log('[NEXT] 若這裡失敗，請把 PCAPdroid 匯出的 .pcapng 上傳，需分析 iCATCH octet-stream 私有封包頭。');
  }
}


function findNextJpegFrame(buffer) {
  if (!buffer || buffer.length < 4) return { frame:null, rest:buffer || Buffer.alloc(0) };
  const soi = buffer.indexOf(Buffer.from([0xff, 0xd8]));
  if (soi < 0) return { frame:null, rest: buffer.length > 1024*1024 ? buffer.slice(-1024) : buffer };
  const eoi = buffer.indexOf(Buffer.from([0xff, 0xd9]), soi + 2);
  if (eoi < 0) return { frame:null, rest: soi > 0 ? buffer.slice(soi) : buffer };
  const frame = buffer.slice(soi, eoi + 2);
  const rest = buffer.slice(eoi + 2);
  return { frame, rest };
}

function startContinuousFfmpegChannel(ch) {
  const id = padCh(ch);
  const urlText = fill(NET_VIDEO_TEMPLATE, ch);
  let proc = null;
  let stopping = false;
  let buffer = Buffer.alloc(0);
  let uploadBusy = false;
  let pendingFrame = null;
  let lastUploadAt = 0;
  let frameCount = 0;
  let restartCount = 0;

  async function uploadFrame(frame) {
    const skip = shouldSkipFrame(ch, frame);
    if (skip.skip) {
      console.log(`[${id}] stream SKIP ${skip.reason} ${skip.detail} keep_last_good=1`);
      return;
    }
    const now = Date.now();
    if (now - lastUploadAt < UPLOAD_MIN_INTERVAL_MS) {
      pendingFrame = frame;
      return;
    }
    lastUploadAt = now;
    uploadBusy = true;
    try {
      rememberLocalFrame(ch, frame);
      const up = await upload(ch, frame, 'icatch-direct-lan-view-v62i');
      frameCount++;
      console.log(`[${id}] stream frame=${frame.length} upload=${up.ok?'OK':'FAIL'} ${up.status||''} fps=${STREAM_FPS} n=${frameCount} ${up.error||''}`);
    } finally {
      uploadBusy = false;
      if (pendingFrame) {
        const next = pendingFrame;
        pendingFrame = null;
        setImmediate(() => uploadFrame(next));
      }
    }
  }

  async function spawnLoop() {
    if (stopping) return;
    const videoHeaders = await getVideoHeaders();
    const headerText = headersToFfmpegText(Object.assign({}, videoHeaders, { 'User-Agent':'SoCatch/RT7-V6.2H', 'Connection':'keep-alive' }));
    // V6.2H SAFE decode:
    // 不使用 -fflags nobuffer / -flags low_delay / analyzeduration 0，避免 iCATCH/Hi3520 串流
    // 還沒收到完整 SPS/PPS 或 GOP 就被 FFmpeg 立即解碼，造成藍綠屏、交叉畫面或色塊。
    // 延遲會略高於 V6.2F，但畫面穩定度優先。
    const args = [
      '-hide_banner', '-nostdin',
      '-loglevel', DEBUG ? 'info' : 'error',
      '-headers', headerText,
      '-rw_timeout', String(Math.max(8000000, FFMPEG_TIMEOUT_MS * 1000)),
      '-i', urlText,
      '-an',
      '-vf', `fps=${STREAM_FPS}`,
      '-q:v', '5',
      '-f', 'mjpeg',
      'pipe:1'
    ];
    console.log(`[${id}] SAFE_STREAM start url=${mask(urlText)} fps=${STREAM_FPS} decoder=safe-3fps-balance`);
    proc = spawn('ffmpeg', args, { windowsHide:true });
    let stderrTail = '';
    proc.stdout.on('data', d => {
      buffer = Buffer.concat([buffer, d]);
      if (buffer.length > 4 * 1024 * 1024) buffer = buffer.slice(-1024 * 1024);
      while (true) {
        const r = findNextJpegFrame(buffer);
        buffer = r.rest;
        if (!r.frame) break;
        if (!uploadBusy) uploadFrame(r.frame);
        else pendingFrame = r.frame;
      }
    });
    proc.stderr.on('data', d => { stderrTail = (stderrTail + d.toString('utf8')).slice(-1000); });
    proc.on('close', code => {
      if (stopping) return;
      restartCount++;
      console.log(`[${id}] TRUE_STREAM ffmpeg closed code=${code} restart=${restartCount} ${stderrTail.replace(/\r?\n/g,' | ')}`);
      setTimeout(spawnLoop, Math.min(5000, 900 + restartCount * 300));
    });
    proc.on('error', e => {
      restartCount++;
      console.log(`[${id}] TRUE_STREAM ffmpeg error ${String(e.message || e)} restart=${restartCount}`);
      setTimeout(spawnLoop, Math.min(5000, 900 + restartCount * 300));
    });
  }

  spawnLoop();
  return () => { stopping = true; try { if (proc) proc.kill('SIGKILL'); } catch (_) {} };
}

async function trueStreamLoop() {
  if (!RAILWAY_URL) console.log('[WARN] RAILWAY_URL_EMPTY: local Direct LAN server still runs; Railway upload disabled.');
  await initIcatchSessionOnce();
  CHANNELS.forEach(ch => startContinuousFfmpegChannel(ch));
}

async function loop() {
  if (TRUE_STREAM_MODE && !TEST_ONLY && !RAW_ONLY && !PROBE_ONLY) return trueStreamLoop();
  for (const ch of CHANNELS) await captureChannel(ch);
  setTimeout(loop, INTERVAL_MS);
}
function checkFfmpeg() {
  return new Promise(resolve => execFile('ffmpeg', ['-version'], { windowsHide:true, timeout:3000 }, (err, stdout) => resolve(err ? {ok:false,error:String(err.message||err)} : {ok:true,version:String(stdout||'').split('\n')[0]})));
}

(async () => {
  console.log(VERSION + ' starting...');
  console.log('Railway:', RAILWAY_URL || '(missing RAILWAY_URL)');
  console.log('DVR:', `iCATCH ${DVR_USER}@${DVR_HOST}:${DVR_HTTP_PORT} channels=${CHANNELS.join(',')}`);
  if (!process.env.ICATCH_NET_VIDEO_TEMPLATE && CHANNELS.length > 1) console.log('[WARN] net_video.cgi 未帶 channel 參數；多路可能全部是同一畫面。建議只用 DVR_CHANNELS=1。');
  console.log('Magic:', MAGIC);
  console.log('Template:', NET_VIDEO_TEMPLATE);
  console.log('Mode:', RAW_ONLY ? 'RAW_ONLY' : (TEST_ONLY ? 'TEST_ONLY' : 'BRIDGE_LOOP'));
  console.log('Token:', TOKEN ? '(set)' : '(empty)');
  const ff = await checkFfmpeg();
  console.log('FFmpeg:', ff.ok ? ff.version : ('NOT FOUND: ' + ff.error));
  if (!ff.ok) console.log('[ERROR] 請先安裝 FFmpeg，並確認 ffmpeg.exe 可在命令列執行。');
  if (!RAILWAY_URL) console.log('[WARN] RAILWAY_URL_EMPTY: 可測 DVR/FFmpeg，但無法上傳 Railway。');
  await initIcatchSessionOnce();
  if (PROBE_ONLY || TEST_ONLY) { await testOne(); return; }
  console.log('Press Ctrl+C to stop.');
startLocalLanServer();
  loop();
})();
