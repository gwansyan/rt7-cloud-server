'use strict';
const http = require('http');
const net = require('net');
const { Buffer } = require('buffer');

const VERSION = 'RT7_V6_4G_DIRECT_JPEG_POLL_VIEW_FIX';
const DVR_HOST = process.env.DVR_HOST || '192.168.0.123';
const DVR_HTTP_PORT = parseInt(process.env.DVR_HTTP_PORT || '80', 10);
const DVR_USER = process.env.DVR_USER || 'admin';
const DVR_PASS = process.env.DVR_PASS || 'vbnmmnbv';
const LOCAL_PORT = parseInt(process.env.LOCAL_PORT || '8787', 10);
const LOCAL_PUBLIC_HOST = process.env.LOCAL_PUBLIC_HOST || '';
const DVR_CHANNEL = process.env.DVR_CHANNEL || '1';
const CATCH_MAGIC = process.env.ICATCH_MAGIC || '39e739de-8d69-aadb-78b9-946a2905858d';
const PATH = process.env.ICATCH_HTTP_TEMPLATE || `/cgi-bin/net_video.cgi?hq=0&iframe=15&pframe=15&audio=0`;
const POLL_MS = parseInt(process.env.POLL_MS || '250', 10);
const RESTART_MS = parseInt(process.env.RESTART_MS || '2500', 10);

let cookie = '';
let latestJpeg = null;
let latestAt = 0;
let seq = 0;
let activeSocket = null;
let extracting = false;
let restarts = 0;
let totalBytes = 0;
let extracted = 0;
let droppedSmall = 0;
let lastError = '';
let lastFrameBytes = 0;

function basicAuth() {
  return 'Basic ' + Buffer.from(`${DVR_USER}:${DVR_PASS}`).toString('base64');
}
function now() { return Date.now(); }
function htmlEscape(s){ return String(s).replace(/[&<>"']/g, c=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c])); }

function httpReq(opts, body) {
  return new Promise((resolve) => {
    const req = http.request(opts, (res) => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks)}));
    });
    req.on('error', e => resolve({status:0, headers:{}, body:Buffer.from(String(e)), error:e.message}));
    req.setTimeout(2500, () => { req.destroy(new Error('timeout')); });
    if (body) req.write(body);
    req.end();
  });
}

async function login() {
  const boundary = '----maya';
  const xml = `<?xml version="1.0" encoding="UTF-8"?><DVR Platform="Hi3520"><GetConfiguration File="profile.xml" /></DVR>`;
  const body = `--${boundary}\r\nContent-Disposition: form-data; name="datafile"; filename="command.xml"\r\nContent-Type: text/xml\r\n\r\n${xml}\r\n--${boundary}--\r\n`;
  const r = await httpReq({
    hostname: DVR_HOST, port: DVR_HTTP_PORT, method: 'POST', path: '/dvr/cmd',
    headers: {
      'Authorization': basicAuth(),
      'Content-Type': `multipart/form-data; boundary=${boundary}`,
      'Content-Length': Buffer.byteLength(body)
    }
  }, body);
  const sc = r.headers['set-cookie'];
  if (sc && sc.length) cookie = String(sc[0]).split(';')[0];
  console.log(`[AUTH] login status=${r.status} cookie=${cookie || '(none)'}`);
}

function findEOI(buf, start) {
  for (let i = start; i < buf.length - 1; i++) {
    if (buf[i] === 0xff && buf[i+1] === 0xd9) return i + 2;
  }
  return -1;
}

function acceptJpeg(jpg) {
  if (jpg.length < 2500) { droppedSmall++; return false; }
  if (!(jpg[0] === 0xff && jpg[1] === 0xd8 && jpg[jpg.length-2] === 0xff && jpg[jpg.length-1] === 0xd9)) return false;
  return true;
}

function startDvrStream() {
  if (extracting) return;
  extracting = true;
  restarts++;
  let buf = Buffer.alloc(0);
  const sock = net.createConnection({host: DVR_HOST, port: DVR_HTTP_PORT}, () => {
    const headers = [
      `GET ${PATH} HTTP/1.0`,
      `Host: ${DVR_HOST}:${DVR_HTTP_PORT}`,
      `Authorization: ${basicAuth()}`,
      cookie ? `Cookie: ${cookie}` : '',
      `Magic: ${CATCH_MAGIC}`,
      `User-Agent: RT7/${VERSION}`,
      `Connection: close`,
      '', ''
    ].filter((x, i, a) => x !== '' || i >= a.length - 2).join('\r\n');
    sock.write(headers);
    console.log(`[PIPE] connect DVR ${DVR_HOST}:${DVR_HTTP_PORT}${PATH}`);
  });
  activeSocket = sock;
  sock.setNoDelay(true);
  sock.setTimeout(15000);
  sock.on('data', (chunk) => {
    totalBytes += chunk.length;
    buf = Buffer.concat([buf, chunk]);
    // discard HTTP header before JPEG stream
    const headerEnd = buf.indexOf(Buffer.from('\r\n\r\n'));
    if (headerEnd >= 0 && buf.slice(0, 20).toString('latin1').startsWith('HTTP/')) {
      buf = buf.slice(headerEnd + 4);
    }
    while (true) {
      const soi = buf.indexOf(Buffer.from([0xff,0xd8]));
      if (soi < 0) {
        if (buf.length > 1024 * 1024) buf = Buffer.alloc(0);
        break;
      }
      if (soi > 0) buf = buf.slice(soi);
      const eoi = findEOI(buf, 2);
      if (eoi < 0) break;
      const jpg = buf.slice(0, eoi);
      buf = buf.slice(eoi);
      if (acceptJpeg(jpg)) {
        latestJpeg = jpg;
        latestAt = now();
        seq++;
        extracted++;
        lastFrameBytes = jpg.length;
        if (seq % 10 === 0) console.log(`[PIPE] seq=${seq} jpg=${jpg.length} age_ms=0 extracted=${extracted}`);
      }
    }
  });
  function close(reason) {
    if (activeSocket === sock) activeSocket = null;
    extracting = false;
    lastError = reason;
    try { sock.destroy(); } catch(e) {}
    console.log(`[PIPE] close reason=${reason} seq=${seq} bytes=${totalBytes}`);
    setTimeout(startDvrStream, RESTART_MS);
  }
  sock.on('timeout', () => close('DVR_SOCKET_TIMEOUT'));
  sock.on('error', e => close(e.message || 'SOCKET_ERROR'));
  sock.on('close', () => { if (extracting) close('DVR_SOCKET_CLOSE'); });
}

function statusJson() {
  const age = latestAt ? now() - latestAt : null;
  return {
    ok: true, version: VERSION,
    mode: 'jpeg_poll_view_continuous_extractor',
    dvr: {host: DVR_HOST, port: DVR_HTTP_PORT, user: DVR_USER, path: PATH},
    local: {port: LOCAL_PORT, host: LOCAL_PUBLIC_HOST || '(auto)', direct: `http://${LOCAL_PUBLIC_HOST || '127.0.0.1'}:${LOCAL_PORT}/direct`},
    stream: {online: !!latestJpeg && age < 5000, seq, age_ms: age, last_frame: lastFrameBytes, total_bytes: totalBytes, extracted, droppedSmall, restarts, extracting, last_error: lastError}
  };
}

function page(host) {
  const direct = `http://${host}/direct`;
  return `<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><meta charset="utf-8"><title>RT7 V6.4G Direct JPEG Poll</title>
<style>body{margin:0;background:#062326;color:#123;font-family:Arial,'Noto Sans TC',sans-serif}.wrap{max-width:900px;margin:auto;padding:30px}.title{font-size:48px;color:white;font-weight:900;line-height:1.15}.card{background:white;border-radius:28px;padding:22px;margin:24px 0}.hint{font-size:26px;line-height:1.5}.video{width:100%;border-radius:18px;background:#111;display:block}.stat{font-size:26px;margin:16px 0;color:#667485}.btn{display:inline-block;background:#15aee5;color:white;text-decoration:none;border-radius:14px;padding:14px 24px;margin:8px;font-size:24px;font-weight:800}@media(max-width:600px){.wrap{padding:20px}.title{font-size:48px}.hint,.stat{font-size:22px}.btn{font-size:22px}}</style>
</head><body><div class="wrap"><div class="title">RT7 V6.4G<br>Direct JPEG Poll View</div><div class="card hint"><b>LAN Bridge：</b>http://${host}<br><b>顯示：</b>/frame.jpg 每 250ms 更新<br>本版不使用手機 MJPEG 解碼；Bridge 先從 DVR octet-stream 抽出 JPEG，手機用單張 JPEG 快速輪詢，避免 MJPEG_ERROR。</div><div class="card"><img id="img" class="video" src="/frame.jpg?t=${Date.now()}"><div id="stat" class="stat">讀取中...</div><a class="btn" href="javascript:reloadImg()">重讀一張</a><a class="btn" href="/status">狀態 JSON</a><a class="btn" href="/frame.jpg" target="_blank">單張 JPG</a></div></div>
<script>
let lastSeq=-1, fail=0;
const img=document.getElementById('img'), stat=document.getElementById('stat');
function reloadImg(){ img.src='/frame.jpg?t='+Date.now(); }
async function tick(){
  try{
    const j=await fetch('/status?t='+Date.now(),{cache:'no-store'}).then(r=>r.json());
    const s=j.stream||{};
    stat.textContent=(s.online?'ONLINE':'WAIT')+' seq='+s.seq+' age_ms='+s.age_ms+' bytes='+s.last_frame+' extracted='+s.extracted+' restarts='+s.restarts;
    if(s.seq!==lastSeq && s.online){ lastSeq=s.seq; reloadImg(); }
  }catch(e){ fail++; stat.textContent='poll error '+e; }
}
setInterval(tick, ${POLL_MS}); tick();
</script></body></html>`;
}

const server = http.createServer((req, res) => {
  const u = new URL(req.url, 'http://x');
  if (u.pathname === '/' || u.pathname === '/direct') {
    res.writeHead(200, {'Content-Type':'text/html; charset=utf-8', 'Cache-Control':'no-store'});
    return res.end(page(req.headers.host || `127.0.0.1:${LOCAL_PORT}`));
  }
  if (u.pathname === '/status') {
    res.writeHead(200, {'Content-Type':'application/json; charset=utf-8', 'Cache-Control':'no-store', 'Access-Control-Allow-Origin':'*'});
    return res.end(JSON.stringify(statusJson(), null, 2));
  }
  if (u.pathname === '/frame.jpg') {
    if (!latestJpeg) {
      res.writeHead(503, {'Content-Type':'text/plain; charset=utf-8', 'Cache-Control':'no-store'});
      return res.end('NO_JPEG_FRAME_YET');
    }
    res.writeHead(200, {'Content-Type':'image/jpeg', 'Content-Length': latestJpeg.length, 'Cache-Control':'no-store, no-cache, must-revalidate, max-age=0', 'Pragma':'no-cache', 'Access-Control-Allow-Origin':'*'});
    return res.end(latestJpeg);
  }
  res.writeHead(404, {'Content-Type':'text/plain'}); res.end('404');
});

(async function main(){
  console.log(`${VERSION} starting...`);
  console.log(`DVR: iCATCH ${DVR_USER}@${DVR_HOST}:${DVR_HTTP_PORT} channel=${DVR_CHANNEL}`);
  const localIp = LOCAL_PUBLIC_HOST || '192.168.0.55';
  console.log(`Phone URL: http://${localIp}:${LOCAL_PORT}/direct`);
  await login();
  startDvrStream();
  server.listen(LOCAL_PORT, '0.0.0.0', () => console.log(`[LAN] Direct JPEG Poll server http://0.0.0.0:${LOCAL_PORT}/ public=${localIp}:${LOCAL_PORT}`));
})();
