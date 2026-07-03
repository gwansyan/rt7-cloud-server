// RT7_V6_3B_DIRECT_MJPEG_PIPELINE_BLUE_GATE_FIX
// iCATCH / SoCatch DVR Direct LAN true MJPEG pipeline
//
// 目的：放棄 V6.2 latest.jpg + HTTP poll 架構，改為：
// DVR net_video.cgi -> FFmpeg -> MJPEG pipe -> Node multipart/x-mixed-replace -> 手機 Chrome
// 不寫 latest.jpg、不排隊、不經 Railway，降低同一 LAN 手機延遲。

const { spawn, execFile } = require('child_process');
const http = require('http');
const os = require('os');

const VERSION = 'RT7_V6_3B_DIRECT_MJPEG_PIPELINE_BLUE_GATE_FIX';
const DVR_HOST = process.env.DVR_HOST || '192.168.0.123';
const DVR_USER = process.env.DVR_USER || 'admin';
const DVR_PASS = process.env.DVR_PASS || 'vbnmmnbv';
const DVR_HTTP_PORT = process.env.DVR_HTTP_PORT || process.env.DVR_PORT || '80';
const LOCAL_HTTP_PORT = parseInt(process.env.LOCAL_HTTP_PORT || '8787', 10) || 8787;
const LOCAL_HTTP_BIND = process.env.LOCAL_HTTP_BIND || '0.0.0.0';
const STREAM_FPS = Math.max(1, Math.min(8, parseInt(process.env.STREAM_FPS || '5', 10) || 5));
const JPEG_QUALITY = Math.max(2, Math.min(12, parseInt(process.env.JPEG_QUALITY || '5', 10) || 5));
const MAGIC = process.env.ICATCH_MAGIC || process.env.DVR_MAGIC || '39e739de-8d69-aadb-78b9-946a2905858d';
const DEBUG = String(process.env.DEBUG || '').trim() === '1';
const TEST_ONLY = String(process.env.TEST_ONLY || '').trim() === '1';
const TEST_OUT = process.env.TEST_OUT || 'icatch_v63a_test.jpg';

// PCAPdroid confirmed endpoint for your iCATCH / Hi3520 DVR.
const NET_VIDEO_TEMPLATE = process.env.ICATCH_NET_VIDEO_TEMPLATE ||
  `http://{host}:{port}/cgi-bin/net_video.cgi?hq=0&iframe=15&pframe=15&audio=0`;

const BASIC = Buffer.from(`${DVR_USER}:${DVR_PASS}`).toString('base64');
let sessionCookie = '';
let ffmpegProc = null;
let buffer = Buffer.alloc(0);
let latestFrame = null;
let latestSeq = 0;
let latestBytes = 0;
let latestTime = 0;
let totalFrames = 0;
let restartCount = 0;
let clients = new Set();
const RAW_W = parseInt(process.env.COLOR_GATE_W || '16', 10);
const RAW_H = parseInt(process.env.COLOR_GATE_H || '9', 10);
const RAW_FRAME_BYTES = RAW_W * RAW_H * 3;
const BLUE_RATIO_LIMIT = parseFloat(process.env.BLUE_RATIO_LIMIT || '0.78');
const BLUE_CENTER_LIMIT = parseFloat(process.env.BLUE_CENTER_LIMIT || '0.72');
let rawBuffer = Buffer.alloc(0);
let rawQueue = [];
let jpegQueue = [];
let droppedBlue = 0;
let droppedNoRaw = 0;

function autoLanHost() {
  const nets = os.networkInterfaces();
  const addrs = [];
  for (const arr of Object.values(nets)) for (const n of (arr || [])) {
    if (n && n.family === 'IPv4' && !n.internal) addrs.push(n.address);
  }
  return (addrs.find(a => /^192\.168\.0\./.test(a)) || addrs.find(a => /^192\.168\./.test(a)) || addrs[0] || '127.0.0.1') + ':' + LOCAL_HTTP_PORT;
}
const PUBLIC_HOST = process.env.LOCAL_PUBLIC_HOST || autoLanHost();

function fillUrl() {
  return NET_VIDEO_TEMPLATE
    .replace(/\{host\}/g, DVR_HOST)
    .replace(/\{port\}/g, DVR_HTTP_PORT)
    .replace(/\{user\}/g, encodeURIComponent(DVR_USER))
    .replace(/\{pass\}/g, encodeURIComponent(DVR_PASS));
}
function mask(s) { return String(s).replace(DVR_PASS, '****'); }

function httpRequest(opts, body) {
  return new Promise(resolve => {
    const req = http.request(opts, res => {
      const chunks = [];
      res.on('data', d => chunks.push(d));
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) }));
    });
    req.on('error', e => resolve({ status: 0, headers: {}, body: Buffer.alloc(0), error: String(e.message || e) }));
    req.setTimeout(4000, () => { req.destroy(); resolve({ status: 0, headers: {}, body: Buffer.alloc(0), error: 'timeout' }); });
    if (body) req.write(body);
    req.end();
  });
}

async function login() {
  const boundary = '----maya';
  const xml = `<?xml version="1.0" encoding="UTF-8" ?><DVR Platform="Hi3520"><GetConfiguration File="profile.xml" /></DVR>`;
  const body = Buffer.from(`--${boundary}\r\nContent-Disposition: form-data; name="datafile"; filename="command.xml"\r\nContent-Type: text/xml\r\n\r\n${xml}\r\n--${boundary}--\r\n`);
  const r = await httpRequest({
    hostname: DVR_HOST,
    port: parseInt(DVR_HTTP_PORT, 10),
    path: '/dvr/cmd',
    method: 'POST',
    headers: {
      'Authorization': `Basic ${BASIC}`,
      'Content-Type': `multipart/form-data; boundary=${boundary}`,
      'Content-Length': body.length
    }
  }, body);
  const setCookie = r.headers && r.headers['set-cookie'];
  if (setCookie && setCookie[0]) {
    const m = String(setCookie[0]).match(/sessionid=[^;]+/i);
    if (m) sessionCookie = m[0];
  }
  console.log(`[AUTH] login status=${r.status} cookie=${sessionCookie || '(none)'}`);
}

function headersForFfmpeg() {
  let h = '';
  h += `Authorization: Basic ${BASIC}\r\n`;
  h += `Magic: ${MAGIC}\r\n`;
  if (sessionCookie) h += `Cookie: ${sessionCookie}\r\n`;
  h += `User-Agent: SoCatch/RT7-V6.3B\r\n`;
  h += `Connection: keep-alive\r\n`;
  return h;
}

function findNextJpegFrame(buf) {
  const soi = buf.indexOf(Buffer.from([0xff, 0xd8]));
  if (soi < 0) return { frame: null, rest: buf.length > 1024 * 1024 ? buf.slice(-1024) : buf };
  const eoi = buf.indexOf(Buffer.from([0xff, 0xd9]), soi + 2);
  if (eoi < 0) return { frame: null, rest: soi > 0 ? buf.slice(soi) : buf };
  return { frame: buf.slice(soi, eoi + 2), rest: buf.slice(eoi + 2) };
}

function writeFrameToClient(res, frame) {
  try {
    res.write(`--rt7frame\r\nContent-Type: image/jpeg\r\nContent-Length: ${frame.length}\r\nCache-Control: no-store\r\nX-RT7-Seq: ${latestSeq}\r\n\r\n`);
    res.write(frame);
    res.write('\r\n');
  } catch (_) {
    try { res.destroy(); } catch (__) {}
  }
}


function analyzeRawColor(raw) {
  if (!raw || raw.length < RAW_FRAME_BYTES) return { blueRatio: 0, centerBlueRatio: 0, avgB: 0, isBlue: false };
  let blue = 0, total = RAW_W * RAW_H, sumB = 0;
  let cBlue = 0, cTotal = 0;
  const cx1 = Math.floor(RAW_W * 0.25), cx2 = Math.ceil(RAW_W * 0.75);
  const cy1 = Math.floor(RAW_H * 0.22), cy2 = Math.ceil(RAW_H * 0.78);
  for (let y = 0; y < RAW_H; y++) {
    for (let x = 0; x < RAW_W; x++) {
      const i = (y * RAW_W + x) * 3;
      const r = raw[i], g = raw[i + 1], b = raw[i + 2];
      sumB += b;
      // iCATCH VIDEO LOSS is strong blue background. Use very cheap RGB gate.
      const isBluePx = (b > 105 && b > r * 1.35 && b > g * 1.25);
      if (isBluePx) blue++;
      if (x >= cx1 && x < cx2 && y >= cy1 && y < cy2) {
        cTotal++;
        if (isBluePx) cBlue++;
      }
    }
  }
  const blueRatio = blue / Math.max(1, total);
  const centerBlueRatio = cBlue / Math.max(1, cTotal);
  const avgB = sumB / Math.max(1, total);
  const isBlue = blueRatio >= BLUE_RATIO_LIMIT && centerBlueRatio >= BLUE_CENTER_LIMIT && avgB > 120;
  return { blueRatio, centerBlueRatio, avgB, isBlue };
}

function tryProcessQueues() {
  while (jpegQueue.length && rawQueue.length) {
    const frame = jpegQueue.shift();
    const raw = rawQueue.shift();
    const a = analyzeRawColor(raw);
    if (a.isBlue) {
      droppedBlue++;
      if (droppedBlue <= 10 || droppedBlue % 20 === 0) {
        console.log(`[PIPE] DROP_BLUE_KEEP_LAST_GOOD blue=${a.blueRatio.toFixed(2)} center=${a.centerBlueRatio.toFixed(2)} avgB=${a.avgB.toFixed(1)} bytes=${frame.length} droppedBlue=${droppedBlue}`);
      }
      continue;
    }
    broadcastFrame(frame);
  }
  // Never queue old frames. If raw gate falls behind, discard oldest JPEGs.
  while (jpegQueue.length > 2) { jpegQueue.shift(); droppedNoRaw++; }
  while (rawQueue.length > 2) rawQueue.shift();
}

function broadcastFrame(frame) {
  latestFrame = frame;
  latestSeq++;
  latestBytes = frame.length;
  latestTime = Date.now();
  totalFrames++;

  for (const res of [...clients]) {
    if (res.destroyed || res.writableEnded) {
      clients.delete(res);
      continue;
    }
    writeFrameToClient(res, frame);
  }

  if (totalFrames % STREAM_FPS === 0) {
    console.log(`[PIPE] frame=${frame.length} seq=${latestSeq} fps=${STREAM_FPS} clients=${clients.size} age_ms=${Date.now() - latestTime}`);
  }
}

function startFfmpeg() {
  const url = fillUrl();
  const vf = `[0:v]fps=${STREAM_FPS},split=2[vjpeg][vraw];[vraw]scale=${RAW_W}:${RAW_H},format=rgb24[vrawout]`;
  const args = [
    '-hide_banner', '-nostdin',
    '-loglevel', DEBUG ? 'info' : 'error',
    '-headers', headersForFfmpeg(),
    // Safe decode: no nobuffer/low_delay because older tests produced blue-green artifacts.
    '-i', url,
    '-an',
    '-filter_complex', vf,
    '-map', '[vjpeg]', '-q:v', String(JPEG_QUALITY), '-f', 'mjpeg', 'pipe:1',
    '-map', '[vrawout]', '-f', 'rawvideo', 'pipe:3'
  ];
  console.log(`[PIPE] start ffmpeg url=${mask(url)} fps=${STREAM_FPS} quality=${JPEG_QUALITY} rawGate=${RAW_W}x${RAW_H} blueLimit=${BLUE_RATIO_LIMIT}/${BLUE_CENTER_LIMIT}`);
  ffmpegProc = spawn('ffmpeg', args, { windowsHide: true, stdio: ['ignore','pipe','pipe','pipe'] });
  let stderrTail = '';

  ffmpegProc.stdout.on('data', d => {
    buffer = Buffer.concat([buffer, d]);
    // zero-buffer policy: never keep old MJPEG data.
    if (buffer.length > 2 * 1024 * 1024) buffer = buffer.slice(-512 * 1024);
    while (true) {
      const r = findNextJpegFrame(buffer);
      buffer = r.rest;
      if (!r.frame) break;
      // No disk, no poll. Queue only until matching low-res raw color sample arrives.
      jpegQueue.push(r.frame);
      tryProcessQueues();
    }
  });
  if (ffmpegProc.stdio && ffmpegProc.stdio[3]) {
    ffmpegProc.stdio[3].on('data', d => {
      rawBuffer = Buffer.concat([rawBuffer, d]);
      while (rawBuffer.length >= RAW_FRAME_BYTES) {
        rawQueue.push(rawBuffer.slice(0, RAW_FRAME_BYTES));
        rawBuffer = rawBuffer.slice(RAW_FRAME_BYTES);
        tryProcessQueues();
      }
      if (rawBuffer.length > RAW_FRAME_BYTES * 4) rawBuffer = rawBuffer.slice(-RAW_FRAME_BYTES);
    });
  }
  ffmpegProc.stderr.on('data', d => { stderrTail = (stderrTail + d.toString('utf8')).slice(-2000); });
  ffmpegProc.on('close', code => {
    restartCount++;
    console.log(`[PIPE] ffmpeg closed code=${code} restart=${restartCount} ${stderrTail.replace(/\r?\n/g, ' | ')}`);
    setTimeout(startFfmpeg, Math.min(4000, 800 + restartCount * 250));
  });
  ffmpegProc.on('error', e => {
    restartCount++;
    console.log(`[PIPE] ffmpeg error ${String(e.message || e)} restart=${restartCount}`);
    setTimeout(startFfmpeg, Math.min(4000, 800 + restartCount * 250));
  });
}

function statusJson() {
  return {
    ok: true,
    version: VERSION,
    port: LOCAL_HTTP_PORT,
    bind: LOCAL_HTTP_BIND,
    public_host: PUBLIC_HOST,
    dvr: `${DVR_HOST}:${DVR_HTTP_PORT}`,
    pipeline: 'DVR -> FFmpeg -> MJPEG pipe -> Node multipart -> Browser',
    mode: 'zero_buffer_direct_mjpeg',
    fps: STREAM_FPS,
    seq: latestSeq,
    bytes: latestBytes,
    age_ms: latestTime ? Date.now() - latestTime : null,
    clients: clients.size,
    dropped_blue: droppedBlue,
    dropped_no_raw: droppedNoRaw,
    raw_gate: `${RAW_W}x${RAW_H}`,
    restarts: restartCount,
    session: !!sessionCookie
  };
}

function startServer() {
  const server = http.createServer((req, res) => {
    const u = new URL(req.url, `http://${req.headers.host || PUBLIC_HOST}`);
    if (u.pathname === '/status') {
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
      return res.end(JSON.stringify(statusJson(), null, 2));
    }
    if (u.pathname === '/latest/CH01.jpg') {
      if (!latestFrame) { res.writeHead(503, { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-store' }); return res.end('NO_FRAME'); }
      res.writeHead(200, { 'Content-Type': 'image/jpeg', 'Content-Length': latestFrame.length, 'Cache-Control': 'no-store, no-cache, must-revalidate', 'Pragma': 'no-cache' });
      return res.end(latestFrame);
    }
    if (u.pathname === '/stream/CH01.mjpg' || u.pathname === '/mjpeg/CH01' || u.pathname === '/direct.mjpg') {
      res.writeHead(200, {
        'Content-Type': 'multipart/x-mixed-replace; boundary=rt7frame',
        'Cache-Control': 'no-store, no-cache, must-revalidate',
        'Pragma': 'no-cache',
        'Connection': 'keep-alive',
        'X-Accel-Buffering': 'no'
      });
      clients.add(res);
      if (latestFrame) writeFrameToClient(res, latestFrame);
      req.on('close', () => clients.delete(res));
      return;
    }
    if (u.pathname === '/' || u.pathname === '/direct') {
      const html = `<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>RT7 V6.3B Direct MJPEG</title><style>body{margin:0;background:#071f25;color:white;font-family:system-ui,-apple-system,'Noto Sans TC',sans-serif}.wrap{max-width:760px;margin:0 auto;padding:14px}.card{background:white;color:#10212b;border-radius:18px;padding:14px;margin:12px 0}.view{background:#000;border-radius:14px;overflow:hidden}.view img{width:100%;display:block}.btn{display:inline-block;background:#0ea5e9;color:#fff;text-decoration:none;border-radius:10px;padding:10px 12px;font-weight:900;margin:4px}.meta{font-size:18px;line-height:1.45}</style></head><body><div class="wrap"><h1>RT7 V6.3B Direct MJPEG<br>Zero Buffer</h1><div class="card"><b>LAN Bridge：</b>http://${PUBLIC_HOST}<br><b>真 MJPEG：</b>/stream/CH01.mjpg<br>本版不寫 latest.jpg、不做輪詢、不排隊、不經 Railway；新增同一個 FFmpeg 低解析 raw color gate，藍底 VIDEO LOSS 不送出，瀏覽器保留上一張真實影像。</div><div class="card"><div class="view"><img id="img" src="/stream/CH01.mjpg?ts=${Date.now()}"></div><p class="meta" id="meta">MJPEG 串流連線中...</p><p><a class="btn" href="/latest/CH01.jpg?ts=${Date.now()}">看單張</a><a class="btn" href="/stream/CH01.mjpg?ts=${Date.now()}">直接MJPEG</a><a class="btn" href="/status">狀態 JSON</a></p></div><script>async function s(){try{const r=await fetch('/status?ts='+Date.now(),{cache:'no-store'});const j=await r.json();document.getElementById('meta').textContent='ONLINE seq='+j.seq+' age_ms='+j.age_ms+' bytes='+j.bytes+' clients='+j.clients+' droppedBlue='+j.dropped_blue+' restarts='+j.restarts;}catch(e){document.getElementById('meta').textContent='status error '+e;}setTimeout(s,1000);}s();</script></div></body></html>`;
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
      return res.end(html);
    }
    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('RT7 V6.3B: use /direct or /stream/CH01.mjpg');
  });
  server.listen(LOCAL_HTTP_PORT, LOCAL_HTTP_BIND, () => {
    console.log(`[LAN] Direct MJPEG server http://${PUBLIC_HOST}/direct`);
    console.log(`[LAN] MJPEG stream       http://${PUBLIC_HOST}/stream/CH01.mjpg`);
  });
}

function checkFfmpeg() {
  return new Promise(resolve => execFile('ffmpeg', ['-version'], { windowsHide:true, timeout:3000 }, (err, stdout) => resolve(err ? {ok:false,error:String(err.message||err)} : {ok:true,version:String(stdout||'').split('\n')[0]})));
}

async function testOne() {
  await login();
  return new Promise(resolve => {
    const url = fillUrl();
    const args = ['-hide_banner','-nostdin','-loglevel','error','-headers',headersForFfmpeg(),'-i',url,'-an','-frames:v','1','-q:v',String(JPEG_QUALITY),'-f','mjpeg','pipe:1'];
    const p = spawn('ffmpeg', args, { windowsHide:true });
    let b = Buffer.alloc(0);
    let e = '';
    p.stdout.on('data', d => b = Buffer.concat([b,d]));
    p.stderr.on('data', d => e += d.toString('utf8'));
    p.on('close', code => {
      if (b.length > 1000) {
        require('fs').writeFileSync(TEST_OUT, b);
        console.log(`[TEST] JPEG saved ${TEST_OUT} bytes=${b.length}`);
      } else {
        console.log(`[TEST] no JPEG code=${code} bytes=${b.length} ${e.replace(/\r?\n/g,' | ')}`);
      }
      resolve();
    });
  });
}

(async () => {
  console.log(`${VERSION} starting...`);
  console.log('DVR:', `iCATCH ${DVR_USER}@${DVR_HOST}:${DVR_HTTP_PORT}`);
  console.log('Magic:', MAGIC);
  console.log('Template:', NET_VIDEO_TEMPLATE);
  console.log('Mode:', TEST_ONLY ? 'TEST_ONLY' : 'DIRECT_MJPEG_PIPELINE');
  const ff = await checkFfmpeg();
  console.log('FFmpeg:', ff.ok ? ff.version : ('NOT FOUND: ' + ff.error));
  if (!ff.ok) return console.log('[ERROR] 請先安裝 FFmpeg，並確認 ffmpeg.exe 可在命令列執行。');
  if (TEST_ONLY) return testOne();
  await login();
  startServer();
  startFfmpeg();
  console.log('Press Ctrl+C to stop.');
})();
