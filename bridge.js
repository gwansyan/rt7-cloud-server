'use strict';

/*
  RT7_V6_4E_DIRECT_SOCKET_RELAY_OCTET_TO_JPEG_FIX
  目的：不再用 latest.jpg / poll / jpeg-js / FFmpeg decode。
  Node 只做 HTTP socket relay：DVR net_video.cgi -> 手機瀏覽器。

  重要：此版完全不過濾藍底 VIDEO LOSS，也不做 JPEG 判斷。
  若 DVR 本身輸出 VIDEO LOSS，手機會看到；但延遲最低。
*/

const http = require('http');
const net = require('net');
const os = require('os');

const VERSION = 'RT7_V6_4E_DIRECT_SOCKET_RELAY_OCTET_TO_JPEG_FIX';
const PORT = Number(process.env.LOCAL_PORT || process.env.PORT || 8787);
const DVR_HOST = process.env.DVR_HOST || '192.168.0.123';
const DVR_HTTP_PORT = Number(process.env.DVR_HTTP_PORT || 80);
const DVR_USER = process.env.DVR_USER || 'admin';
const DVR_PASS = process.env.DVR_PASS || 'vbnmmnbv';
const DVR_CHANNEL = process.env.DVR_CHANNEL || '1';
const MAGIC = process.env.ICATCH_MAGIC || '39e739de-8d69-aadb-78b9-946a2905858d';
const TEMPLATE = process.env.ICATCH_HTTP_TEMPLATE || '/cgi-bin/net_video.cgi?hq=0&iframe=15&pframe=15&audio=0';
const RELAY_TIMEOUT_MS = Number(process.env.RELAY_TIMEOUT_MS || 15000);

let sessionCookie = '';
let totalClients = 0;
let activeClients = 0;
let relayStarts = 0;
let relayErrors = 0;
let lastRelayAt = 0;
let lastBytes = 0;
let lastError = '';


function normalizeMultipartChunk(chunk) {
  // iCATCH net_video.cgi 的每一張圖常用：Content-Type: application/octet-stream
  // Android/Chrome <img> MJPEG 對 octet-stream 相容性差，會顯示破圖/MJPEG_ERROR。
  // 只改 HTTP part header 文字，不改 JPEG binary。
  const a = Buffer.from('Content-Type: application/octet-stream', 'latin1');
  const b = Buffer.from('Content-Type: image/jpeg', 'latin1');
  const a2 = Buffer.from('Content-type: application/octet-stream', 'latin1');
  const b2 = Buffer.from('Content-Type: image/jpeg', 'latin1');
  let out = chunk;
  if (out.indexOf(a) >= 0) out = Buffer.from(out.toString('latin1').replaceAll('Content-Type: application/octet-stream', 'Content-Type: image/jpeg'), 'latin1');
  if (out.indexOf(a2) >= 0) out = Buffer.from(out.toString('latin1').replaceAll('Content-type: application/octet-stream', 'Content-Type: image/jpeg'), 'latin1');
  return out;
}

function authHeader() {
  return 'Basic ' + Buffer.from(`${DVR_USER}:${DVR_PASS}`).toString('base64');
}

function localIPv4() {
  if (process.env.LOCAL_PUBLIC_HOST && process.env.LOCAL_PUBLIC_HOST.trim()) {
    return process.env.LOCAL_PUBLIC_HOST.trim();
  }
  const nets = os.networkInterfaces();
  const all = [];
  for (const name of Object.keys(nets)) {
    if (/vmware|virtualbox|tailscale|loopback|docker|hyper-v/i.test(name)) continue;
    for (const ni of nets[name] || []) {
      if (ni.family === 'IPv4' && !ni.internal) all.push(ni.address);
    }
  }
  // Prefer the same /24 as DVR, e.g. DVR 192.168.0.123 -> PC 192.168.0.xx
  const parts = String(DVR_HOST).split('.');
  if (parts.length === 4) {
    const prefix = parts.slice(0, 3).join('.') + '.';
    const same = all.find(ip => ip.startsWith(prefix));
    if (same) return same;
  }
  const lan = all.find(ip => ip.startsWith('192.168.')) || all.find(ip => ip.startsWith('10.')) || all.find(ip => ip.startsWith('172.'));
  return lan || all[0] || '127.0.0.1';
}

function dvrPath(ch) {
  let p = TEMPLATE;
  p = p.replaceAll('{ch}', String(ch));
  p = p.replaceAll('{channel}', String(ch));
  return p.startsWith('/') ? p : '/' + p;
}

function postDvrCmd(xml, timeoutMs = 3500) {
  return new Promise((resolve) => {
    const boundary = '----rt7v64a';
    const body =
      `--${boundary}\r\n` +
      `Content-Disposition: form-data; name="datafile"; filename="command.xml"\r\n` +
      `Content-Type: text/xml\r\n\r\n` +
      xml + `\r\n` +
      `--${boundary}--\r\n`;

    const req = http.request({
      host: DVR_HOST,
      port: DVR_HTTP_PORT,
      method: 'POST',
      path: '/dvr/cmd',
      timeout: timeoutMs,
      headers: {
        'Authorization': authHeader(),
        'Content-Type': `multipart/form-data; boundary=${boundary}`,
        'Content-Length': Buffer.byteLength(body),
        'Connection': 'close'
      }
    }, (res) => {
      let sample = '';
      if (res.headers['set-cookie'] && res.headers['set-cookie'][0]) {
        const m = /sessionid=([^;]+)/.exec(res.headers['set-cookie'][0]);
        if (m) sessionCookie = `sessionid=${m[1]}`;
      }
      res.on('data', (d) => { if (sample.length < 300) sample += d.toString('utf8'); });
      res.on('end', () => resolve({ ok: res.statusCode === 200, status: res.statusCode, cookie: sessionCookie, sample }));
    });
    req.on('timeout', () => { req.destroy(new Error('login timeout')); });
    req.on('error', (e) => resolve({ ok: false, status: 0, error: e.message }));
    req.end(body);
  });
}

async function ensureLogin() {
  const xml = '<?xml version="1.0" encoding="UTF-8"?><DVR Platform="Hi3520"><GetConfiguration File="system.xml" /></DVR>';
  const r = await postDvrCmd(xml);
  if (r.ok) {
    console.log(`[AUTH] login OK cookie=${sessionCookie || '(none)'}`);
  } else {
    console.log(`[AUTH] login FAIL status=${r.status || 0} error=${r.error || ''}`);
  }
  return r;
}

function sendDvrGetSocket(res, ch = DVR_CHANNEL) {
  totalClients++;
  activeClients++;
  relayStarts++;
  lastRelayAt = Date.now();
  lastBytes = 0;
  lastError = '';

  const path = dvrPath(ch);
  const sock = net.createConnection({ host: DVR_HOST, port: DVR_HTTP_PORT });
  let buf = Buffer.alloc(0);
  let state = 'status_header'; // status_header -> maybe_inner_header -> streaming
  let clientClosed = false;

  function closeAll(reason) {
    if (clientClosed) return;
    clientClosed = true;
    activeClients = Math.max(0, activeClients - 1);
    try { sock.destroy(); } catch (_) {}
    try { if (!res.writableEnded) res.end(); } catch (_) {}
    if (reason) console.log(`[RELAY] close reason=${reason} active=${activeClients}`);
  }

  function startBrowser(boundary, firstBody) {
    if (res.headersSent) return;
    boundary = String(boundary || 'myboundary').replace(/^--/, '').trim() || 'myboundary';
    res.writeHead(200, {
      'Content-Type': `multipart/x-mixed-replace; boundary=${boundary}`,
      'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0',
      'Pragma': 'no-cache',
      'Connection': 'close',
      'X-RT7-Version': VERSION,
      'X-RT7-Mode': 'direct-socket-relay-octet-to-jpeg'
    });
    state = 'streaming';
    if (firstBody && firstBody.length) {
      lastBytes += firstBody.length;
      res.write(normalizeMultipartChunk(firstBody));
    }
  }

  res.on('close', () => closeAll('browser_close'));
  res.on('error', () => closeAll('browser_error'));

  sock.setTimeout(RELAY_TIMEOUT_MS);
  sock.on('connect', () => {
    // 注意：最後一定要 \r\n\r\n。不要多加，也不要少加。
    const reqLines = [
      `GET ${path} HTTP/1.0`,
      `Host: ${DVR_HOST}:${DVR_HTTP_PORT}`,
      `Authorization: ${authHeader()}`,
      `Magic: ${MAGIC}`,
      sessionCookie ? `Cookie: ${sessionCookie}` : null,
      'User-Agent: RT7-V6.4D-SocketRelay',
      'Accept: multipart/x-mixed-replace,image/jpeg,*/*',
      'Connection: close'
    ].filter(Boolean);
    const requestText = reqLines.join('\r\n') + '\r\n\r\n';
    console.log(`[RELAY] start CH${String(ch).padStart(2,'0')} ${DVR_HOST}:${DVR_HTTP_PORT}${path} clients=${activeClients}`);
    sock.write(requestText, 'latin1');
  });

  sock.on('data', (chunk) => {
    if (clientClosed) return;

    if (state === 'streaming') {
      lastBytes += chunk.length;
      const ok = res.write(normalizeMultipartChunk(chunk));
      if (!ok && res.writableLength > 512 * 1024) {
        relayErrors++;
        lastError = 'BROWSER_BACKPRESSURE_CLOSE';
        closeAll('browser_backpressure');
      }
      return;
    }

    buf = Buffer.concat([buf, chunk]);

    if (state === 'status_header') {
      const idx = buf.indexOf('\r\n\r\n');
      if (idx < 0) return;
      const rawHeader = buf.subarray(0, idx).toString('latin1');
      const body = buf.subarray(idx + 4);
      const statusLine = rawHeader.split(/\r?\n/)[0] || '';
      const ctLine = (rawHeader.match(/content-type\s*:\s*([^\r\n]+)/i) || [])[1] || '';
      if (!/200/.test(statusLine)) {
        relayErrors++;
        lastError = `DVR_STATUS_${statusLine}`;
        res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-store' });
        res.end(`DVR relay failed: ${statusLine}\nPath: ${path}\n`);
        closeAll('dvr_non_200');
        return;
      }
      // 正常 DVR 可能在 HTTP header 內帶 Content-Type，也可能先只回 HTTP/1.0 200 OK，
      // 再把 Content-type: multipart... 當成後續 body 送出。手機 Chrome 不能把那段當影像，必須剝掉。
      if (/multipart\/x-mixed-replace/i.test(ctLine)) {
        const b = (ctLine.match(/boundary\s*=\s*([^;\s]+)/i) || [])[1] || 'myboundary';
        startBrowser(b, body);
        return;
      }
      buf = body;
      state = 'maybe_inner_header';
    }

    if (state === 'maybe_inner_header') {
      // 等待 DVR 延遲送出的內層 multipart header，例如：
      // Content-type: multipart/x-mixed-replace;boundary=--myboundary\r\n\r\n--myboundary...
      const txtHead = buf.subarray(0, Math.min(buf.length, 512)).toString('latin1');
      if (/^\s*Content-Type\s*:/i.test(txtHead) || /^\s*Content-type\s*:/i.test(txtHead)) {
        const idx2 = buf.indexOf('\r\n\r\n');
        if (idx2 < 0) {
          if (buf.length > 4096) {
            relayErrors++;
            lastError = 'INNER_HEADER_TOO_LONG';
            res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
            res.end('DVR inner multipart header too long\n');
            closeAll('inner_header_too_long');
          }
          return;
        }
        const innerHeader = buf.subarray(0, idx2).toString('latin1');
        const innerBody = buf.subarray(idx2 + 4);
        const ct = (innerHeader.match(/content-type\s*:\s*([^\r\n]+)/i) || [])[1] || '';
        const b = (ct.match(/boundary\s*=\s*([^;\s]+)/i) || [])[1] || 'myboundary';
        console.log(`[RELAY] delayed multipart header boundary=${b} firstBody=${innerBody.length}`);
        startBrowser(b, normalizeMultipartChunk(innerBody));
        return;
      }

      // 若已經直接看到 boundary 或 JPEG，就直接開始。
      if (buf.length > 0) {
        const m = txtHead.match(/--([A-Za-z0-9_\-]+)/);
        const b = m ? m[1] : 'myboundary';
        startBrowser(b, normalizeMultipartChunk(buf));
        buf = Buffer.alloc(0);
        return;
      }
    }
  });

  sock.on('timeout', () => {
    relayErrors++;
    lastError = 'DVR_SOCKET_TIMEOUT';
    closeAll('dvr_timeout');
  });
  sock.on('error', (e) => {
    relayErrors++;
    lastError = e.message;
    if (!res.headersSent) {
      res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-store' });
      res.end(`DVR socket error: ${e.message}\n`);
    }
    closeAll('dvr_error');
  });
  sock.on('close', () => closeAll('dvr_close'));
}

function htmlPage() {
  const host = localIPv4();
  const base = `http://${host}:${PORT}`;
  return `<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>RT7 V6.4E Direct Socket Relay</title>
<style>
body{margin:0;background:#06242a;color:#fff;font-family:Arial,'Microsoft JhengHei',sans-serif}.wrap{max-width:760px;margin:auto;padding:32px 22px}h1{font-size:44px;line-height:1.12}.card{background:#fff;color:#123;border-radius:24px;padding:22px;margin:20px 0}.video{width:100%;border-radius:18px;background:#000}.btn{display:inline-block;background:#11aee8;color:#fff;padding:14px 18px;border-radius:12px;margin:8px;text-decoration:none;font-weight:700}.muted{color:#607080;font-size:15px;line-height:1.6}code{font-size:16px}
</style></head><body><div class="wrap">
<h1>RT7 V6.4E<br>Direct Socket Relay</h1>
<div class="card"><b>LAN Bridge：</b>${base}<br><b>Socket Relay：</b>/relay/CH01.mjpg<br><span class="muted">本版修正 iCATCH DVR 每張 frame 使用 application/octet-stream，手機 Chrome 無法當 MJPEG 顯示；Bridge 會改寫為 image/jpeg 後再送手機。</span></div>
<div class="card"><img id="v" class="video" src="/relay/CH01.mjpg?ts=${Date.now()}" onerror="document.getElementById('st').textContent='MJPEG_ERROR：請按重連，或改用 /status 檢查 Bridge。'">
<p id="st" class="muted">若仍出現 MJPEG_ERROR，請開 /relay/CH01.mjpg 直接測試；本版會把 octet-stream part header 改為 image/jpeg。</p>
<a class="btn" href="javascript:location.reload()">重連</a><a class="btn" href="/relay/CH01.mjpg">直接MJPEG</a><a class="btn" href="/status">狀態JSON</a></div>
</div></body></html>`;
}

function statusJson() {
  return {
    ok: true,
    version: VERSION,
    mode: 'direct_socket_relay_no_decode',
    dvr: { host: DVR_HOST, port: DVR_HTTP_PORT, user: DVR_USER, path: dvrPath(DVR_CHANNEL) },
    local: { port: PORT, host: localIPv4(), direct: `http://${localIPv4()}:${PORT}/direct` },
    auth: { cookie_set: !!sessionCookie },
    relay: { active_clients: activeClients, total_clients: totalClients, starts: relayStarts, errors: relayErrors, last_bytes: lastBytes, age_ms: lastRelayAt ? Date.now() - lastRelayAt : null, last_error: lastError }
  };
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  if (url.pathname === '/' || url.pathname === '/direct') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
    res.end(htmlPage());
    return;
  }
  if (url.pathname === '/status' || url.pathname === '/status.json') {
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
    res.end(JSON.stringify(statusJson(), null, 2));
    return;
  }
  if (url.pathname === '/login') {
    const r = await ensureLogin();
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
    res.end(JSON.stringify(r, null, 2));
    return;
  }
  const m = /^\/relay\/(CH)?(\d+)\.mjpg$/i.exec(url.pathname) || /^\/stream\/(CH)?(\d+)\.mjpg$/i.exec(url.pathname);
  if (m) {
    sendDvrGetSocket(res, m[2] || DVR_CHANNEL);
    return;
  }
  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end(`RT7 V6.4E 404\n/direct\n/status\n/relay/CH01.mjpg\n`);
});

server.on('clientError', (err, socket) => {
  try { socket.end('HTTP/1.1 400 Bad Request\r\n\r\n'); } catch (_) {}
});

server.listen(PORT, '0.0.0.0', async () => {
  console.log(`${VERSION} starting...`);
  console.log(`DVR: iCATCH ${DVR_USER}@${DVR_HOST}:${DVR_HTTP_PORT} channel=${DVR_CHANNEL}`);
  console.log(`Template: ${TEMPLATE}`);
  console.log(`Direct LAN View: http://${localIPv4()}:${PORT}/direct`);
  await ensureLogin();
  console.log('Press Ctrl+C to stop.');
});
