// RT7_V6_0B_ICATCH_HTTP_PROTOCOL_SCANNER
// iCATCH / SoCatch DVR HTTP CGI Scanner + LAN Bridge
//
// 目的：你的 iCATCH RMH-0428EU-K A3 回應 HTTP/1.0 401 Unauthorized / mini_httpd，
// 代表 80 port 有服務但需要認證。V6_0B 先掃 HTTP CGI / Snapshot / MJPEG URL，
// 找到 JPEG 後主動上傳 Railway，避免一直猜 RTSP。

const { execFile } = require('child_process');
const http = require('http');
const https = require('https');
const net = require('net');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const VERSION = 'RT7_V6_0B_ICATCH_HTTP_PROTOCOL_SCANNER';
const RAILWAY_URL = (process.env.RAILWAY_URL || '').replace(/\/+$/, '');
const TOKEN = process.env.RT7_DVR_BRIDGE_TOKEN || 'rt7-dvr-bridge';
const DVR_HOST = process.env.DVR_HOST || '192.168.0.123';
const DVR_USER = process.env.DVR_USER || 'admin';
const DVR_PASS = process.env.DVR_PASS || '';
const DVR_HTTP_PORT = process.env.DVR_HTTP_PORT || process.env.DVR_PORT || '80';
const DVR_RTSP_PORT = process.env.DVR_RTSP_PORT || '554';
const CHANNELS = String(process.env.DVR_CHANNELS || '1,2,3,4').split(',').map(s => parseInt(s.trim(), 10)).filter(Boolean);
const INTERVAL_MS = Math.max(1000, parseInt(process.env.INTERVAL_MS || '3000', 10) || 3000);
const HTTP_TIMEOUT_MS = Math.max(1200, parseInt(process.env.HTTP_TIMEOUT_MS || '4500', 10) || 4500);
const SOURCE_DIR = (process.env.RT7_BRIDGE_SOURCE_DIR || '').trim();
const SCAN_ONLY = String(process.env.SCAN_ONLY || '').trim() === '1';
const PROBE_ONLY = String(process.env.PROBE_ONLY || '').trim() === '1';
const DEBUG = String(process.env.DEBUG || '').trim() === '1';
const SCAN_LIMIT = Math.max(1, parseInt(process.env.SCAN_LIMIT || '260', 10) || 260);
const RESULT_FILE = process.env.SCAN_RESULT_FILE || path.join(__dirname, 'icatch_scan_result.json');
const WORKING_FILE = process.env.WORKING_URL_FILE || path.join(__dirname, 'icatch_working_urls.json');

function enc(v) { return encodeURIComponent(String(v == null ? '' : v)); }
function padCh(ch) { return 'CH' + String(ch).padStart(2, '0'); }
function mask(url) { return String(url).replace(/:([^:@/]*?)@/, ':****@').replace(/(password|pwd|pass|p)=([^&]*)/gi, '$1=****'); }
function isJpeg(buf) { return buf && buf.length > 4 && buf[0] === 0xff && buf[1] === 0xd8; }
function isPng(buf) { return buf && buf.length > 8 && buf[0] === 0x89 && buf.slice(1,4).toString() === 'PNG'; }
function extractJpeg(buf) {
  if (!buf || !buf.length) return null;
  if (isJpeg(buf)) return buf;
  const start = buf.indexOf(Buffer.from([0xff, 0xd8]));
  if (start < 0) return null;
  const end = buf.indexOf(Buffer.from([0xff, 0xd9]), start + 2);
  if (end < 0) return null;
  return buf.slice(start, end + 2);
}
function fill(tpl, ch) {
  const ch0 = String(Math.max(0, Number(ch) - 1));
  const ch2 = String(ch).padStart(2, '0');
  return tpl
    .replace(/\{host\}/g, DVR_HOST)
    .replace(/\{http_port\}/g, DVR_HTTP_PORT)
    .replace(/\{rtsp_port\}/g, DVR_RTSP_PORT)
    .replace(/\{port\}/g, DVR_HTTP_PORT)
    .replace(/\{user\}/g, enc(DVR_USER))
    .replace(/\{pass\}/g, enc(DVR_PASS))
    .replace(/\{password\}/g, enc(DVR_PASS))
    .replace(/\{ch\}/g, String(ch))
    .replace(/\{ch0\}/g, ch0)
    .replace(/\{ch2\}/g, ch2)
    .replace(/\{channel\}/g, String(ch));
}

function md5(s) { return crypto.createHash('md5').update(s).digest('hex'); }
function parseDigestHeader(h) {
  const out = {};
  String(h || '').replace(/^Digest\s+/i, '').replace(/(\w+)=((?:"[^"]+")|[^,]+)/g, (_, k, v) => {
    out[k] = String(v || '').replace(/^"|"$/g, '');
    return '';
  });
  return out;
}
function digestAuthHeader(method, urlObj, wwwAuth) {
  const d = parseDigestHeader(wwwAuth);
  if (!d.realm || !d.nonce) return '';
  const uri = urlObj.pathname + (urlObj.search || '');
  const qop = (d.qop || '').split(',').map(x => x.trim()).find(x => x === 'auth') || '';
  const nc = '00000001';
  const cnonce = crypto.randomBytes(8).toString('hex');
  const ha1 = md5(`${DVR_USER}:${d.realm}:${DVR_PASS}`);
  const ha2 = md5(`${method}:${uri}`);
  const response = qop ? md5(`${ha1}:${d.nonce}:${nc}:${cnonce}:${qop}:${ha2}`) : md5(`${ha1}:${d.nonce}:${ha2}`);
  let s = `Digest username="${DVR_USER}", realm="${d.realm}", nonce="${d.nonce}", uri="${uri}", response="${response}"`;
  if (d.opaque) s += `, opaque="${d.opaque}"`;
  if (d.algorithm) s += `, algorithm=${d.algorithm}`;
  if (qop) s += `, qop=${qop}, nc=${nc}, cnonce="${cnonce}"`;
  return s;
}

function httpGet(urlText, authMode='basic', timeoutMs=HTTP_TIMEOUT_MS, cookie='') {
  return new Promise(resolve => {
    let u;
    try { u = new URL(urlText); } catch (e) { return resolve({ ok:false, error:'BAD_URL ' + e.message, url:urlText }); }
    const lib = u.protocol === 'https:' ? https : http;
    const headers = { 'User-Agent':'Mozilla/5.0 RT7-iCATCH-Scanner/6.0B', 'Accept':'image/jpeg,image/*,multipart/x-mixed-replace,*/*', 'Connection':'close' };
    if (cookie) headers.Cookie = cookie;
    if (authMode === 'basic' && DVR_USER && !urlText.includes('@')) headers.Authorization = 'Basic ' + Buffer.from(DVR_USER + ':' + DVR_PASS).toString('base64');
    const req = lib.request(u, { method:'GET', headers, timeout:timeoutMs }, res => {
      const chunks = [];
      res.on('data', d => { chunks.push(d); if (Buffer.concat(chunks).length > 2_500_000) req.destroy(); });
      res.on('end', () => {
        const buffer = Buffer.concat(chunks);
        resolve({ status:res.statusCode, headers:res.headers, buffer, text:buffer.slice(0,500).toString('latin1').replace(/[^\x20-\x7e\r\n]/g,'.'), url:urlText });
      });
    });
    req.on('timeout', () => { try { req.destroy(new Error('HTTP timeout')); } catch (_) {} });
    req.on('error', e => resolve({ ok:false, error:String(e.message || e), url:urlText }));
    req.end();
  });
}
async function requestImage(urlText) {
  let r = await httpGet(urlText, 'basic');
  if (r.status === 401 && r.headers && /Digest/i.test(String(r.headers['www-authenticate'] || ''))) {
    const u = new URL(urlText);
    const auth = digestAuthHeader('GET', u, r.headers['www-authenticate']);
    if (auth) {
      r = await new Promise(resolve => {
        const lib = u.protocol === 'https:' ? https : http;
        const headers = { 'User-Agent':'Mozilla/5.0 RT7-iCATCH-Scanner/6.0B', 'Accept':'image/jpeg,image/*,multipart/x-mixed-replace,*/*', 'Connection':'close', 'Authorization':auth };
        const req = lib.request(u, { method:'GET', headers, timeout:HTTP_TIMEOUT_MS }, res => {
          const chunks=[]; res.on('data', d=>{chunks.push(d); if (Buffer.concat(chunks).length > 2_500_000) req.destroy();});
          res.on('end',()=>resolve({ status:res.statusCode, headers:res.headers, buffer:Buffer.concat(chunks), url:urlText }));
        });
        req.on('timeout',()=>{try{req.destroy(new Error('HTTP timeout'));}catch(_){}});
        req.on('error', e=>resolve({ ok:false, error:String(e.message||e), url:urlText }));
        req.end();
      });
    }
  }
  const jpg = extractJpeg(r.buffer);
  const ct = String((r.headers && r.headers['content-type']) || '');
  return { ok:!!jpg, jpeg:jpg, status:r.status || 0, contentType:ct, bytes:(r.buffer && r.buffer.length) || 0, error:r.error || '', text:r.text || '', url:urlText };
}

// iCATCH / mini_httpd / old DVR HTTP candidate list.
const HTTP_TEMPLATES = [
  'http://{host}:{http_port}/',
  'http://{host}:{http_port}/image.jpg',
  'http://{host}:{http_port}/snapshot.jpg',
  'http://{host}:{http_port}/snap.jpg',
  'http://{host}:{http_port}/video.jpg',
  'http://{host}:{http_port}/current.jpg',
  'http://{host}:{http_port}/tmpfs/auto.jpg',
  'http://{host}:{http_port}/tmpfs/snap.jpg',
  'http://{host}:{http_port}/tmpfs/auto_{ch}.jpg',
  'http://{host}:{http_port}/tmpfs/auto{ch}.jpg',
  'http://{host}:{http_port}/jpg/image.jpg',
  'http://{host}:{http_port}/jpg/{ch}.jpg',
  'http://{host}:{http_port}/ch{ch}.jpg',
  'http://{host}:{http_port}/CH{ch}.jpg',
  'http://{host}:{http_port}/cgi-bin/snapshot.cgi',
  'http://{host}:{http_port}/cgi-bin/snapshot.cgi?channel={ch}',
  'http://{host}:{http_port}/cgi-bin/snapshot.cgi?channel={ch0}',
  'http://{host}:{http_port}/cgi-bin/snapshot.cgi?chn={ch}',
  'http://{host}:{http_port}/cgi-bin/snapshot.cgi?chn={ch0}',
  'http://{host}:{http_port}/snapshot.cgi',
  'http://{host}:{http_port}/snapshot.cgi?channel={ch}',
  'http://{host}:{http_port}/snapshot.cgi?chn={ch0}',
  'http://{host}:{http_port}/cgi-bin/currentpic.cgi',
  'http://{host}:{http_port}/cgi-bin/currentpic.cgi?channel={ch}',
  'http://{host}:{http_port}/cgi-bin/currentpic.cgi?chn={ch0}',
  'http://{host}:{http_port}/cgi-bin/viewer/video.jpg',
  'http://{host}:{http_port}/cgi-bin/viewer/video.jpg?channel={ch}',
  'http://{host}:{http_port}/cgi-bin/video.jpg',
  'http://{host}:{http_port}/cgi-bin/video.jpg?channel={ch}',
  'http://{host}:{http_port}/cgi-bin/view.cgi?chn={ch}',
  'http://{host}:{http_port}/cgi-bin/view.cgi?channel={ch}',
  'http://{host}:{http_port}/cgi-bin/hi3510/snap.cgi?chn={ch0}&u={user}&p={pass}',
  'http://{host}:{http_port}/cgi-bin/hi3510/snap.cgi?chn={ch}&u={user}&p={pass}',
  'http://{host}:{http_port}/cgi-bin/hi3510/param.cgi?cmd=snap&chn={ch0}&u={user}&p={pass}',
  'http://{host}:{http_port}/webcapture.jpg?command=snap&channel={ch}',
  'http://{host}:{http_port}/webcapture.jpg?command=snap&channel={ch0}',
  'http://{host}:{http_port}/cgi-bin/CGIProxy.fcgi?cmd=snapPicture2&usr={user}&pwd={pass}',
  'http://{host}:{http_port}/cgi-bin/CGIProxy.fcgi?cmd=snapPicture&usr={user}&pwd={pass}',
  'http://{host}:{http_port}/cgi-bin/CGIProxy.fcgi?cmd=snapPicture2&usr={user}&pwd={pass}&channel={ch}',
  'http://{host}:{http_port}/cgi-bin/CGIProxy.fcgi?cmd=snapPicture&usr={user}&pwd={pass}&channel={ch}',
  'http://{host}:{http_port}/cgi-bin/net_jpeg.cgi?ch={ch}',
  'http://{host}:{http_port}/cgi-bin/net_jpeg.cgi?ch={ch0}',
  'http://{host}:{http_port}/cgi-bin/net_jpeg.cgi?channel={ch}',
  'http://{host}:{http_port}/cgi-bin/mjpg/video.cgi?channel={ch}',
  'http://{host}:{http_port}/cgi-bin/mjpg/video.cgi?chn={ch0}',
  'http://{host}:{http_port}/cgi-bin/mjpeg?channel={ch}',
  'http://{host}:{http_port}/mjpeg.cgi?channel={ch}',
  'http://{host}:{http_port}/videostream.cgi?user={user}&pwd={pass}',
  'http://{host}:{http_port}/videostream.cgi?user={user}&pwd={pass}&resolution=32&rate=0',
  'http://{host}:{http_port}/videostream.cgi?loginuse={user}&loginpas={pass}',
  'http://{host}:{http_port}/livestream.cgi?user={user}&pwd={pass}&channel={ch}',
  'http://{host}:{http_port}/cgi-bin/guest/Video.cgi?media=JPEG&channel={ch}',
  'http://{host}:{http_port}/cgi-bin/jpg/image.cgi?channel={ch}',
  'http://{host}:{http_port}/cgi-bin/jpg/image.cgi?ch={ch}',
  'http://{host}:{http_port}/cgi-bin/snapshot.cgi?loginuse={user}&loginpas={pass}&channel={ch}',
  'http://{host}:{http_port}/cgi-bin/snapshot.cgi?user={user}&pwd={pass}&channel={ch}',
  'http://{host}:{http_port}/snapshot.cgi?user={user}&pwd={pass}&channel={ch}',
  'http://{host}:{http_port}/cgi-bin/encoder?USER={user}&PWD={pass}&SNAPSHOT&CHANNEL={ch}',
  'http://{host}:{http_port}/cgi-bin/encoder?USER={user}&PWD={pass}&SNAPSHOT&CHANNEL={ch0}',
  'http://{user}:{pass}@{host}:{http_port}/image.jpg',
  'http://{user}:{pass}@{host}:{http_port}/snapshot.jpg',
  'http://{user}:{pass}@{host}:{http_port}/cgi-bin/snapshot.cgi?channel={ch}',
  'http://{user}:{pass}@{host}:{http_port}/tmpfs/auto.jpg'
];
const customHttp = process.env.ICATCH_HTTP_TEMPLATE || process.env.ICATCH_SNAPSHOT_TEMPLATE || process.env.DVR_SNAPSHOT_TEMPLATE || '';
const TEMPLATES = customHttp ? [customHttp] : HTTP_TEMPLATES;

function readJson(file, fallback) { try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch (_) { return fallback; } }
function writeJson(file, obj) { try { fs.writeFileSync(file, JSON.stringify(obj, null, 2), 'utf8'); } catch (e) { console.log('[WARN] write json failed', e.message); } }

function upload(ch, jpeg, source='icatch-http-scanner') {
  return new Promise(resolve => {
    if (!RAILWAY_URL) return resolve({ ok:false, error:'RAILWAY_URL_EMPTY' });
    const id = padCh(ch);
    const url = new URL(RAILWAY_URL + '/api/rt7/dvr/bridge/upload/' + encodeURIComponent(id) + '?source=' + encodeURIComponent(source));
    const lib = url.protocol === 'https:' ? https : http;
    const req = lib.request(url, { method:'POST', headers:{ 'Content-Type':'image/jpeg', 'Content-Length':jpeg.length, 'X-RT7-Bridge-Token':TOKEN, 'User-Agent':'RT7-iCATCH-HTTP-Scanner/6.0B' }, timeout:10000 }, res => {
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
    try { if (fs.existsSync(f)) { const b=fs.readFileSync(f); const jpg=extractJpeg(b); if (jpg) return { ok:true, buffer:jpg, file:f }; } } catch (_) {}
  }
  return null;
}
async function scanChannel(ch, verbose=true) {
  const results = [];
  const max = Math.min(TEMPLATES.length, SCAN_LIMIT);
  for (let i = 0; i < max; i++) {
    const tpl = TEMPLATES[i];
    const url = fill(tpl, ch);
    const r = await requestImage(url);
    results.push({ i, ok:r.ok, status:r.status, contentType:r.contentType, bytes:r.bytes, url:mask(url), sample:r.text || r.error || '' });
    if (r.ok) {
      if (verbose) console.log(`[${padCh(ch)}] HTTP JPEG FOUND bytes=${r.jpeg.length} ${mask(url)}`);
      return { ok:true, ch, url, tpl, jpeg:r.jpeg, results };
    }
    if (verbose && (DEBUG || i < 12 || r.status === 200 || r.status === 401)) {
      console.log(`[${padCh(ch)}] scan fail #${i} status=${r.status || '-'} bytes=${r.bytes || 0} ${mask(url)}`);
    }
  }
  return { ok:false, ch, results };
}
async function scanAll() {
  const out = { version:VERSION, time:new Date().toISOString(), dvr:{host:DVR_HOST,http_port:DVR_HTTP_PORT,user:DVR_USER, pass_set:!!DVR_PASS}, channels:{} };
  const working = readJson(WORKING_FILE, {});
  for (const ch of CHANNELS) {
    const r = await scanChannel(ch, true);
    out.channels[padCh(ch)] = { ok:r.ok, url:r.url ? mask(r.url) : '', template:r.tpl || '', tried:r.results.length, results:r.results };
    if (r.ok) {
      working[padCh(ch)] = { template:r.tpl, found_at:new Date().toISOString(), url:mask(r.url) };
      const up = await upload(ch, r.jpeg, 'icatch-http-scan');
      console.log(`[${padCh(ch)}] scan upload=${up.ok?'OK':'FAIL'} ${up.status||''} ${up.error||''}`);
    }
  }
  writeJson(RESULT_FILE, out);
  writeJson(WORKING_FILE, working);
  console.log('Scan result saved:', RESULT_FILE);
  console.log('Working URL saved:', WORKING_FILE);
  return out;
}
async function oneChannel(ch) {
  const manual = readManualFrame(ch);
  if (manual) {
    const up = await upload(ch, manual.buffer, 'manual-folder');
    console.log(`[${padCh(ch)}] manual=${manual.file} jpeg=${manual.buffer.length} upload=${up.ok?'OK':'FAIL'} ${up.status||''} ${up.error||''}`);
    return;
  }
  const working = readJson(WORKING_FILE, {});
  const w = working[padCh(ch)] && working[padCh(ch)].template;
  if (w) {
    const url = fill(w, ch);
    const r = await requestImage(url);
    if (r.ok) {
      const up = await upload(ch, r.jpeg, 'icatch-http-working');
      console.log(`[${padCh(ch)}] working HTTP jpeg=${r.jpeg.length} upload=${up.ok?'OK':'FAIL'} ${up.status||''} ${mask(url)}`);
      return;
    }
    console.log(`[${padCh(ch)}] working URL failed, rescan this channel... ${r.status||r.error||''} ${mask(url)}`);
  }
  const s = await scanChannel(ch, false);
  if (s.ok) {
    working[padCh(ch)] = { template:s.tpl, found_at:new Date().toISOString(), url:mask(s.url) };
    writeJson(WORKING_FILE, working);
    const up = await upload(ch, s.jpeg, 'icatch-http-autoscan');
    console.log(`[${padCh(ch)}] autoscan HTTP jpeg=${s.jpeg.length} upload=${up.ok?'OK':'FAIL'} ${up.status||''} ${mask(s.url)}`);
  } else {
    console.log(`[${padCh(ch)}] HTTP CGI scanner found no JPEG. 請開啟 DVR 網頁登入後，或用 ONVIF/Wireshark 找真實 JPEG/RTSP URL，填到 ICATCH_HTTP_TEMPLATE。`);
  }
}
function tcpProbe(port, payload) {
  return new Promise(resolve => {
    const sock = new net.Socket();
    const chunks = [];
    const timer = setTimeout(() => { try { sock.destroy(); } catch (_) {} resolve({ ok:false, port, error:'timeout' }); }, 2500);
    sock.connect(Number(port), DVR_HOST, () => { if (payload) sock.write(payload); else setTimeout(() => { try { sock.end(); } catch (_) {} }, 500); });
    sock.on('data', d => chunks.push(d));
    sock.on('close', () => { clearTimeout(timer); const b = Buffer.concat(chunks); resolve({ ok:true, port, bytes:b.length, text:b.slice(0,180).toString('latin1').replace(/[^\x20-\x7E]/g,'.') }); });
    sock.on('error', e => { clearTimeout(timer); resolve({ ok:false, port, error:String(e.message || e) }); });
  });
}
async function diagnostic() {
  console.log('--- iCATCH HTTP diagnostic ---');
  const r = await tcpProbe(DVR_HTTP_PORT, Buffer.from('GET / HTTP/1.0\r\n\r\n'));
  console.log(`[TCP ${DVR_HTTP_PORT}]`, r.ok ? `open bytes=${r.bytes} ${r.text || ''}` : r.error);
  const rr = await httpGet(`http://${DVR_HOST}:${DVR_HTTP_PORT}/`, 'none', 3500);
  console.log('[HTTP / no-auth]', 'status=' + (rr.status || '-'), 'www-auth=' + String((rr.headers && rr.headers['www-authenticate']) || '').slice(0,120), 'server=' + String((rr.headers && rr.headers.server) || ''));
  console.log('--- end diagnostic ---');
}
async function loop() {
  for (const ch of CHANNELS) await oneChannel(ch);
  setTimeout(loop, INTERVAL_MS);
}
function checkFfmpeg() {
  return new Promise(resolve => execFile('ffmpeg', ['-version'], { windowsHide:true, timeout:3000 }, (err, stdout) => resolve(err ? {ok:false,error:String(err.message||err)} : {ok:true,version:String(stdout||'').split('\n')[0]})));
}

(async () => {
  console.log(VERSION + ' starting...');
  console.log('Railway:', RAILWAY_URL || '(missing RAILWAY_URL)');
  console.log('DVR:', `iCATCH ${DVR_USER}@${DVR_HOST} http=${DVR_HTTP_PORT} channels=${CHANNELS.join(',')}`);
  console.log('Token:', TOKEN ? '(set)' : '(empty)');
  console.log('Mode:', SCAN_ONLY ? 'SCAN_ONLY' : (PROBE_ONLY ? 'PROBE_ONLY' : 'BRIDGE_LOOP'));
  if (SOURCE_DIR) console.log('Manual source dir:', SOURCE_DIR);
  const ff = await checkFfmpeg();
  console.log('FFmpeg:', ff.ok ? ff.version : ('not required / not found: ' + ff.error));
  await diagnostic();
  if (!RAILWAY_URL) console.log('[WARN] RAILWAY_URL_EMPTY: 仍可掃描 DVR，但無法上傳 Railway。請在 BAT 設定 RAILWAY_URL。');
  if (PROBE_ONLY) return;
  if (SCAN_ONLY) { await scanAll(); return; }
  console.log('Press Ctrl+C to stop. First run will scan HTTP CGI.');
  loop();
})();
