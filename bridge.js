// RT7_V6_1B_ICATCH_NET_VIDEO_HTTP_BRIDGE
// iCATCH / SoCatch DVR net_video.cgi LAN Bridge
//
// 根據 PCAPdroid 已確認真正影像 API：
//   GET /cgi-bin/net_video.cgi?hq=0&iframe=15&pframe=15&audio=0
//   Authorization: Basic admin:<blank>
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

const VERSION = 'RT7_V6_1B_ICATCH_NET_VIDEO_HTTP_BRIDGE';
const RAILWAY_URL = (process.env.RAILWAY_URL || '').replace(/\/+$/, '');
const TOKEN = process.env.RT7_DVR_BRIDGE_TOKEN || 'rt7-dvr-bridge';
const DVR_HOST = process.env.DVR_HOST || '192.168.0.123';
const DVR_USER = process.env.DVR_USER || 'admin';
const DVR_PASS = process.env.DVR_PASS || '';
const DVR_HTTP_PORT = process.env.DVR_HTTP_PORT || process.env.DVR_PORT || '80';
const CHANNELS = String(process.env.DVR_CHANNELS || '1,2,3,4').split(',').map(s => parseInt(s.trim(), 10)).filter(Boolean);
const INTERVAL_MS = Math.max(1000, parseInt(process.env.INTERVAL_MS || '3000', 10) || 3000);
const FFMPEG_TIMEOUT_MS = Math.max(2500, parseInt(process.env.FFMPEG_TIMEOUT_MS || '10000', 10) || 10000);
const DEBUG = String(process.env.DEBUG || '').trim() === '1';
const PROBE_ONLY = String(process.env.PROBE_ONLY || '').trim() === '1';
const TEST_ONLY = String(process.env.TEST_ONLY || '').trim() === '1';
const TEST_OUT = process.env.TEST_OUT || path.join(__dirname, 'icatch_ch1_test.jpg');

const RAW_ONLY = String(process.env.RAW_ONLY || '').trim() === '1';
const RAW_OUT = process.env.RAW_OUT || path.join(__dirname, 'icatch_ch1_raw.bin');
const RAW_BYTES = Math.max(65536, parseInt(process.env.RAW_BYTES || '524288', 10) || 524288);
const MAGIC = process.env.ICATCH_MAGIC || process.env.DVR_MAGIC || '39e739de-8d69-aadb-78b9-946a2905858d';

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

function httpProbe(urlText, timeoutMs=5000) {
  return new Promise(resolve => {
    let u;
    try { u = new URL(urlText); } catch (e) { return resolve({ ok:false, error:'BAD_URL ' + e.message }); }
    const lib = u.protocol === 'https:' ? https : http;
    const headers = {
      'Authorization': authHeader(),
      'Magic': MAGIC,
      'User-Agent': 'RT7-iCATCH-NetVideo/6.1B',
      'Accept': '*/*',
      'Connection': 'close'
    };
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


function httpRawDump(urlText, outFile, maxBytes=RAW_BYTES, timeoutMs=8000) {
  return new Promise(resolve => {
    let u;
    try { u = new URL(urlText); } catch (e) { return resolve({ ok:false, error:'BAD_URL ' + e.message }); }
    const lib = u.protocol === 'https:' ? https : http;
    const headers = {
      'Authorization': authHeader(),
      'Magic': MAGIC,
      'User-Agent': 'SoCatch/RT7-V6.1B',
      'Accept': '*/*',
      'Connection': 'close'
    };
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

function ffmpegOneJpeg(urlText, ch) {
  return new Promise(resolve => {
    const headerText = `Authorization: ${authHeader()}\r\nMagic: ${MAGIC}\r\nUser-Agent: SoCatch/RT7\r\n`;
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

function upload(ch, jpeg, source='icatch-net-video') {
  return new Promise(resolve => {
    if (!RAILWAY_URL) return resolve({ ok:false, error:'RAILWAY_URL_EMPTY' });
    const id = padCh(ch);
    const url = new URL(RAILWAY_URL + '/api/rt7/dvr/bridge/upload/' + encodeURIComponent(id) + '?source=' + encodeURIComponent(source));
    const lib = url.protocol === 'https:' ? https : http;
    const req = lib.request(url, {
      method:'POST',
      headers:{ 'Content-Type':'image/jpeg', 'Content-Length':jpeg.length, 'X-RT7-Bridge-Token':TOKEN, 'User-Agent':'RT7-iCATCH-NetVideo/6.1B' },
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
  const up = await upload(ch, r.jpeg, 'icatch-net-video-ffmpeg');
  console.log(`[${padCh(ch)}] frame=${r.jpeg.length} upload=${up.ok?'OK':'FAIL'} ${up.status||''} ${up.error||''}`);
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
    console.log(`[TEST] JPEG saved: ${TEST_OUT} bytes=${r.jpeg.length}`);
  } else {
    console.log(`[TEST] ffmpeg did not produce JPEG. code=${r.code} bytes=${r.bytes}`);
    if (r.error) console.log(r.error);
    console.log('[NEXT] 若這裡失敗，請把 PCAPdroid 匯出的 .pcapng 上傳，需分析 iCATCH octet-stream 私有封包頭。');
  }
}

async function loop() {
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
  console.log('Magic:', MAGIC);
  console.log('Template:', NET_VIDEO_TEMPLATE);
  console.log('Mode:', RAW_ONLY ? 'RAW_ONLY' : (TEST_ONLY ? 'TEST_ONLY' : 'BRIDGE_LOOP'));
  console.log('Token:', TOKEN ? '(set)' : '(empty)');
  const ff = await checkFfmpeg();
  console.log('FFmpeg:', ff.ok ? ff.version : ('NOT FOUND: ' + ff.error));
  if (!ff.ok) console.log('[ERROR] 請先安裝 FFmpeg，並確認 ffmpeg.exe 可在命令列執行。');
  if (!RAILWAY_URL) console.log('[WARN] RAILWAY_URL_EMPTY: 可測 DVR/FFmpeg，但無法上傳 Railway。');
  if (PROBE_ONLY || TEST_ONLY) { await testOne(); return; }
  console.log('Press Ctrl+C to stop.');
  loop();
})();
