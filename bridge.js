'use strict';

const http = require('http');
const net = require('net');
const { spawn } = require('child_process');
const { Buffer } = require('buffer');

const VERSION = 'RT7_V6_6A_DVR_NATIVE_H264_DECODER';
const DVR_HOST = process.env.DVR_HOST || '192.168.0.123';
const DVR_HTTP_PORT = parseInt(process.env.DVR_HTTP_PORT || '80', 10);
const DVR_USER = process.env.DVR_USER || 'admin';
const DVR_PASS = process.env.DVR_PASS || 'vbnmmnbv';
const LOCAL_PORT = parseInt(process.env.LOCAL_PORT || '8787', 10);
const LOCAL_PUBLIC_HOST = process.env.LOCAL_PUBLIC_HOST || '192.168.0.55';
const DVR_CHANNEL = parseInt(process.env.DVR_CHANNEL || '1', 10);
const PATH = process.env.ICATCH_HTTP_TEMPLATE || '/cgi-bin/net_video.cgi?hq=0&iframe=15&pframe=15&audio=0';
const CATCH_MAGIC = process.env.ICATCH_MAGIC || '39e739de-8d69-aadb-78b9-946a2905858d';
const FPS = parseInt(process.env.FPS || '5', 10);
const JPEG_Q = parseInt(process.env.JPEG_Q || '5', 10);
const MIN_JPEG = parseInt(process.env.MIN_JPEG || '9000', 10);
const MAX_JPEG = parseInt(process.env.MAX_JPEG || '250000', 10);
const AUTO_RESTART_MS = parseInt(process.env.AUTO_RESTART_MS || '12000', 10);
const STALL_MS = parseInt(process.env.STALL_MS || '3500', 10);
const DEBUG_NAL = process.env.DEBUG_NAL === '1';
const TEST_DUMP_SECONDS = parseInt(process.env.TEST_DUMP_SECONDS || '12', 10);

let cookie = '';
let seq = 0;
let latestJpeg = null;
let latestAt = 0;
let latestNal = {};
let extractedNals = 0;
let decoderFrames = 0;
let restarts = 0;
let connectCount = 0;
let reconnectTimer = null;
let dvrSocket = null;
let ff = null;
let ffBuf = Buffer.alloc(0);
let streamBuf = Buffer.alloc(0);
let running = false;
let clients = 0;
let lastError = '';
let lastDvrBytes = 0;
let droppedSmall = 0;
let sps = 0, pps = 0, idr = 0, nonIdr = 0;
let haveSpsPps = false;

function log(s){ console.log(s); }
function now(){ return Date.now(); }
function b64Auth(){ return Buffer.from(`${DVR_USER}:${DVR_PASS}`).toString('base64'); }
function basicAuth(){ return 'Basic ' + b64Auth(); }

function httpReq(opts, body){
  return new Promise(resolve=>{
    const req = http.request(opts, res=>{
      const chunks=[];
      res.on('data', c=>chunks.push(c));
      res.on('end', ()=>resolve({status:res.statusCode, headers:res.headers, body:Buffer.concat(chunks)}));
    });
    req.on('error', e=>resolve({status:0, headers:{}, body:Buffer.from(String(e)), error:e.message}));
    req.setTimeout(2500, ()=>req.destroy(new Error('timeout')));
    if(body) req.write(body);
    req.end();
  });
}

async function login(){
  const boundary='----maya';
  const xml='<?xml version="1.0" encoding="UTF-8"?><DVR Platform="Hi3520"><GetConfiguration File="profile.xml" /></DVR>';
  const body=`--${boundary}\r\nContent-Disposition: form-data; name="datafile"; filename="command.xml"\r\nContent-Type: text/xml\r\n\r\n${xml}\r\n--${boundary}--\r\n`;
  const r = await httpReq({hostname:DVR_HOST, port:DVR_HTTP_PORT, method:'POST', path:'/dvr/cmd', headers:{
    Authorization: basicAuth(), 'Content-Type':`multipart/form-data; boundary=${boundary}`, 'Content-Length':Buffer.byteLength(body)
  }}, body);
  const sc = r.headers['set-cookie'];
  if(sc && sc.length) cookie = String(sc[0]).split(';')[0];
  log(`[AUTH] login status=${r.status} cookie=${cookie || '(none)'}`);
  return r.status === 200;
}

function startCodeLen(buf, pos){
  if(pos + 3 <= buf.length && buf[pos]===0 && buf[pos+1]===0 && buf[pos+2]===1) return 3;
  if(pos + 4 <= buf.length && buf[pos]===0 && buf[pos+1]===0 && buf[pos+2]===0 && buf[pos+3]===1) return 4;
  return 0;
}
function findStart(buf, from){
  for(let i=from; i<buf.length-3; i++){
    if(buf[i]===0 && buf[i+1]===0 && (buf[i+2]===1 || (buf[i+2]===0 && buf[i+3]===1))) return i;
  }
  return -1;
}
function nalType(nal){
  let p = 0;
  const sl = startCodeLen(nal,0);
  if(sl) p = sl;
  return nal[p] ? (nal[p] & 0x1f) : -1;
}
function nalName(t){
  return ({1:'P',5:'IDR',6:'SEI',7:'SPS',8:'PPS',9:'AUD'})[t] || String(t);
}

function feedNal(nal){
  const t = nalType(nal);
  if(t < 0) return;
  extractedNals++;
  latestNal[nalName(t)] = (latestNal[nalName(t)] || 0) + 1;
  if(t===7) sps++;
  if(t===8) pps++;
  if(t===5) idr++;
  if(t===1) nonIdr++;
  if(t===7 || t===8) haveSpsPps = true;
  // Do not feed P frames before SPS/PPS. Native parser keeps decoder from starting on broken packets.
  if(!haveSpsPps && t!==7 && t!==8) return;
  if(ff && ff.stdin && !ff.killed){
    try { ff.stdin.write(nal); } catch(e){ lastError = 'FFMPEG_STDIN_' + e.message; }
  }
  if(DEBUG_NAL && (extractedNals % 50 === 0 || t===7 || t===8 || t===5)){
    log(`[NAL] #${extractedNals} type=${nalName(t)} bytes=${nal.length} sps=${sps} pps=${pps} idr=${idr} p=${nonIdr}`);
  }
}

function consumeH264(chunk){
  lastDvrBytes += chunk.length;
  streamBuf = Buffer.concat([streamBuf, chunk]);
  if(streamBuf.length > 1024*1024) streamBuf = streamBuf.slice(-512*1024);
  while(true){
    const a = findStart(streamBuf, 0);
    if(a < 0){
      if(streamBuf.length > 4096) streamBuf = streamBuf.slice(-4096);
      return;
    }
    if(a > 0) streamBuf = streamBuf.slice(a);
    const b = findStart(streamBuf, startCodeLen(streamBuf,0) + 1);
    if(b < 0) return;
    const nal = streamBuf.slice(0, b);
    streamBuf = streamBuf.slice(b);
    feedNal(nal);
  }
}

function extractJpegs(chunk){
  ffBuf = Buffer.concat([ffBuf, chunk]);
  while(true){
    const s = ffBuf.indexOf(Buffer.from([0xff,0xd8]));
    if(s < 0){ if(ffBuf.length > 1024*1024) ffBuf = ffBuf.slice(-2048); return; }
    const e = ffBuf.indexOf(Buffer.from([0xff,0xd9]), s+2);
    if(e < 0){ if(s > 0) ffBuf = ffBuf.slice(s); return; }
    const jpg = ffBuf.slice(s, e+2);
    ffBuf = ffBuf.slice(e+2);
    if(jpg.length < MIN_JPEG || jpg.length > MAX_JPEG){ droppedSmall++; continue; }
    latestJpeg = jpg;
    latestAt = now();
    seq++;
    decoderFrames++;
  }
}

function startDecoder(){
  if(ff) return;
  const args = [
    '-hide_banner','-loglevel','warning',
    '-fflags','nobuffer','-flags','low_delay','-analyzeduration','0','-probesize','32768',
    '-f','h264','-i','pipe:0',
    '-an','-vf',`fps=${FPS}`,
    '-q:v',String(JPEG_Q),'-f','image2pipe','-vcodec','mjpeg','pipe:1'
  ];
  ff = spawn('ffmpeg', args, {stdio:['pipe','pipe','pipe']});
  ff.stdout.on('data', extractJpegs);
  ff.stderr.on('data', d=>{ const s=String(d).trim(); if(s) lastError=s.slice(-300); });
  ff.on('close', code=>{ lastError = 'FFMPEG_CLOSE_' + code; ff = null; scheduleRestart(1000); });
  log('[DECODER] ffmpeg native H264 stdin decoder started');
}

function makeDvrRequest(){
  const lines = [
    `GET ${PATH} HTTP/1.1`,
    `Host: ${DVR_HOST}:${DVR_HTTP_PORT}`,
    `Authorization: ${basicAuth()}`,
    cookie ? `Cookie: ${cookie}` : '',
    `Magic: ${CATCH_MAGIC}`,
    'User-Agent: RT7-V6.6A-NativeH264',
    'Accept: */*',
    'Connection: close',
    '', ''
  ].filter(x=>x!==null && x!==undefined).join('\r\n');
  return lines;
}

async function connectDvr(){
  if(running) return;
  running = true;
  restarts++;
  connectCount++;
  streamBuf = Buffer.alloc(0);
  ffBuf = Buffer.alloc(0);
  haveSpsPps = false;
  startDecoder();
  const sock = net.createConnection({host:DVR_HOST, port:DVR_HTTP_PORT});
  dvrSocket = sock;
  let headerDone = false;
  let hdr = Buffer.alloc(0);
  sock.on('connect', ()=>{
    log(`[PIPE] connect DVR ${DVR_HOST}:${DVR_HTTP_PORT}${PATH}`);
    sock.write(makeDvrRequest());
  });
  sock.on('data', data=>{
    if(!headerDone){
      hdr = Buffer.concat([hdr, data]);
      const p = hdr.indexOf('\r\n\r\n');
      if(p < 0){ if(hdr.length > 65536){ lastError='HTTP_HEADER_TOO_LONG'; sock.destroy(); } return; }
      const head = hdr.slice(0,p).toString('latin1');
      const body = hdr.slice(p+4);
      headerDone = true;
      if(!/^HTTP\/\d\.\d 200/.test(head)){
        lastError = 'DVR_HTTP_' + head.split('\r\n')[0];
        sock.destroy(); return;
      }
      if(body.length) consumeH264(body);
      return;
    }
    consumeH264(data);
  });
  sock.on('error', e=>{ lastError = 'DVR_SOCKET_' + e.message; });
  sock.on('close', ()=>{ running = false; if(dvrSocket===sock) dvrSocket=null; log('[PIPE] DVR closed; restart'); scheduleRestart(800); });
  sock.setTimeout(AUTO_RESTART_MS, ()=>{ log('[PIPE] periodic reconnect'); sock.destroy(); });
}
function scheduleRestart(ms){
  if(reconnectTimer) return;
  reconnectTimer = setTimeout(()=>{ reconnectTimer=null; connectDvr(); }, ms);
}
function watchdog(){
  setInterval(()=>{
    const age = latestAt ? now()-latestAt : 999999;
    if(age > STALL_MS && running && dvrSocket){
      lastError = 'STALL_RECONNECT age=' + age;
      try{ dvrSocket.destroy(); }catch(e){}
    }
    log(`[PIPE] seq=${seq} fps=${FPS} clients=${clients} age_ms=${latestAt?now()-latestAt:-1} nals=${extractedNals} frames=${decoderFrames} bytes=${lastDvrBytes}`);
  }, 5000);
}

function html(){
  return `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${VERSION}</title>
  <style>body{margin:0;background:#062326;color:#10222a;font-family:Arial,'Noto Sans TC',sans-serif}.wrap{padding:26px;max-width:900px;margin:auto}.title{font-size:44px;line-height:1.15;color:white;font-weight:900;margin:40px 0}.card{background:white;border-radius:26px;padding:22px;margin:22px 0;box-shadow:0 8px 28px #0003}.img{width:100%;border-radius:20px;background:#111;min-height:220px;object-fit:contain}button{border:0;background:#16a9df;color:white;font-weight:900;border-radius:14px;padding:14px 22px;margin:8px;font-size:20px}.muted{color:#687986;font-size:20px;line-height:1.5}.status{font-size:22px;margin:12px 0;white-space:pre-wrap}</style></head><body><div class="wrap">
  <div class="title">RT7 V6.6A DVR<br>Native H264 Decoder</div>
  <div class="card"><b>LAN Bridge：</b> http://${LOCAL_PUBLIC_HOST}:${LOCAL_PORT}<br><b>顯示：</b> /frame.jpg 每 200ms 更新<br><span class="muted">本版不把 DVR octet-stream 直接給手機；先用 Node 原生切 NAL，再交給 FFmpeg H264 stdin 解碼成標準 JPEG，避免手機讀到私有 iCATCH 封包。</span></div>
  <div class="card"><img id="img" class="img"><div id="st" class="status">等待影像...</div><button onclick="reloadImg()">重讀一張</button><button onclick="location.href='/status'">狀態 JSON</button><button onclick="location.href='/dump'">NAL 偵測</button></div>
  </div><script>
  const img=document.getElementById('img'), st=document.getElementById('st');
  let last=0;
  async function poll(){try{const r=await fetch('/status?ts='+Date.now(),{cache:'no-store'});const j=await r.json();let age=j.stream.age_ms;st.textContent=(j.stream.online?'ONLINE':'WAIT')+' seq='+j.stream.seq+' age_ms='+age+' frames='+j.stream.decoder_frames+' nals='+j.stream.nals+' sps='+j.stream.sps+' pps='+j.stream.pps+' idr='+j.stream.idr+' bytes='+j.stream.last_jpeg_bytes+' clients='+j.local.clients+' err='+(j.stream.last_error||''); if(j.stream.seq!==last){last=j.stream.seq; img.src='/frame.jpg?seq='+last+'&t='+Date.now();}}catch(e){st.textContent='poll error '+e;}setTimeout(poll,200);} 
  function reloadImg(){img.src='/frame.jpg?t='+Date.now();}
  poll();
  </script></body></html>`;
}

function sendJpeg(res){
  clients++;
  res.on('close',()=>clients=Math.max(0,clients-1));
  if(!latestJpeg){ res.writeHead(503, {'Content-Type':'text/plain','Cache-Control':'no-store'}); res.end('NO_FRAME_YET'); return; }
  res.writeHead(200, {'Content-Type':'image/jpeg','Content-Length':latestJpeg.length,'Cache-Control':'no-store, no-cache, must-revalidate','Pragma':'no-cache','Expires':'0','Access-Control-Allow-Origin':'*'});
  res.end(latestJpeg);
}
function statusJson(){
  return {
    ok:true, version:VERSION, mode:'native_nal_parser_ffmpeg_h264_stdin',
    dvr:{host:DVR_HOST, port:DVR_HTTP_PORT, user:DVR_USER, path:PATH, channel:DVR_CHANNEL},
    local:{port:LOCAL_PORT, host:LOCAL_PUBLIC_HOST, direct:`http://${LOCAL_PUBLIC_HOST}:${LOCAL_PORT}/direct`, clients},
    auth:{cookie_set:!!cookie},
    stream:{online:!!latestJpeg && now()-latestAt < 5000, seq, age_ms:latestAt?now()-latestAt:-1, last_jpeg_bytes:latestJpeg?latestJpeg.length:0, nals:extractedNals, decoder_frames:decoderFrames, sps, pps, idr, non_idr:nonIdr, dropped_small:droppedSmall, restarts, connect_count:connectCount, dvr_bytes:lastDvrBytes, last_error:lastError, latest_nal:latestNal}
  };
}

async function dumpOnce(res){
  const startNals = extractedNals, startFrames = decoderFrames, startBytes = lastDvrBytes;
  setTimeout(()=>{
    const out = {ok:true, seconds:TEST_DUMP_SECONDS, nals:extractedNals-startNals, frames:decoderFrames-startFrames, bytes:lastDvrBytes-startBytes, total:statusJson().stream};
    res.writeHead(200, {'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store'}); res.end(JSON.stringify(out,null,2));
  }, TEST_DUMP_SECONDS*1000);
}

async function main(){
  log(`${VERSION} starting...`);
  log(`DVR: iCATCH ${DVR_USER}@${DVR_HOST}:${DVR_HTTP_PORT} channel=${DVR_CHANNEL}`);
  log(`Phone URL: http://${LOCAL_PUBLIC_HOST}:${LOCAL_PORT}/direct`);
  log(`Template: ${PATH}`);
  await login();
  connectDvr();
  watchdog();
  const server = http.createServer((req,res)=>{
    const u = new URL(req.url, `http://${req.headers.host || 'x'}`);
    if(u.pathname === '/' || u.pathname === '/direct'){ res.writeHead(200, {'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-store'}); res.end(html()); return; }
    if(u.pathname === '/frame.jpg'){ sendJpeg(res); return; }
    if(u.pathname === '/status'){ res.writeHead(200, {'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store','Access-Control-Allow-Origin':'*'}); res.end(JSON.stringify(statusJson(),null,2)); return; }
    if(u.pathname === '/dump'){ dumpOnce(res); return; }
    res.writeHead(404, {'Content-Type':'text/plain'}); res.end('not found');
  });
  server.listen(LOCAL_PORT, '0.0.0.0', ()=>log(`[LAN] server http://0.0.0.0:${LOCAL_PORT}/ public=http://${LOCAL_PUBLIC_HOST}:${LOCAL_PORT}/direct`));
}
main().catch(e=>{ console.error(e); process.exit(1); });
