'use strict';

/* RT7_V6_4F_DIRECT_SOCKET_RELAY_JPEG_FRAME_EXTRACT_FIX
   iCATCH net_video.cgi -> Node socket -> extract JPEG SOI/EOI -> browser MJPEG.
   不使用 FFmpeg / jpeg-js / latest.jpg。避免 Android Chrome 不接受 DVR 原始 multipart/octet-stream。
*/
const http = require('http');
const net = require('net');
const os = require('os');

const VERSION = 'RT7_V6_4F_DIRECT_SOCKET_RELAY_JPEG_FRAME_EXTRACT_FIX';
const PORT = Number(process.env.LOCAL_PORT || process.env.PORT || 8787);
const DVR_HOST = process.env.DVR_HOST || '192.168.0.123';
const DVR_HTTP_PORT = Number(process.env.DVR_HTTP_PORT || 80);
const DVR_USER = process.env.DVR_USER || 'admin';
const DVR_PASS = process.env.DVR_PASS || 'vbnmmnbv';
const DVR_CHANNEL = process.env.DVR_CHANNEL || '1';
const MAGIC = process.env.ICATCH_MAGIC || '39e739de-8d69-aadb-78b9-946a2905858d';
const TEMPLATE = process.env.ICATCH_HTTP_TEMPLATE || '/cgi-bin/net_video.cgi?hq=0&iframe=15&pframe=15&audio=0';
const RELAY_TIMEOUT_MS = Number(process.env.RELAY_TIMEOUT_MS || 30000);
const MIN_JPEG_BYTES = Number(process.env.MIN_JPEG_BYTES || 7000);
const MAX_BUFFER_BYTES = Number(process.env.MAX_BUFFER_BYTES || 1024 * 1024);

let sessionCookie = '';
let totalClients = 0, activeClients = 0, relayStarts = 0, relayErrors = 0;
let lastRelayAt = 0, lastBytes = 0, lastError = '', lastFrameBytes = 0, lastSeq = 0, extracted = 0, droppedSmall = 0;

function authHeader(){ return 'Basic ' + Buffer.from(`${DVR_USER}:${DVR_PASS}`).toString('base64'); }
function localIPv4(){
  if (process.env.LOCAL_PUBLIC_HOST && process.env.LOCAL_PUBLIC_HOST.trim()) return process.env.LOCAL_PUBLIC_HOST.trim();
  const nets=os.networkInterfaces(), all=[];
  for (const name of Object.keys(nets)) {
    if (/vmware|virtualbox|tailscale|loopback|docker|hyper-v/i.test(name)) continue;
    for (const ni of nets[name]||[]) if (ni.family==='IPv4' && !ni.internal) all.push(ni.address);
  }
  const parts=String(DVR_HOST).split('.');
  if (parts.length===4){ const pre=parts.slice(0,3).join('.')+'.'; const same=all.find(ip=>ip.startsWith(pre)); if(same) return same; }
  return all.find(ip=>ip.startsWith('192.168.')) || all.find(ip=>ip.startsWith('10.')) || all[0] || '127.0.0.1';
}
function dvrPath(ch){ let p=TEMPLATE.replaceAll('{ch}',String(ch)).replaceAll('{channel}',String(ch)); return p.startsWith('/')?p:'/'+p; }

function postDvrCmd(xml, timeoutMs=3500){
  return new Promise(resolve=>{
    const boundary='----rt7v64f';
    const body=`--${boundary}\r\nContent-Disposition: form-data; name="datafile"; filename="command.xml"\r\nContent-Type: text/xml\r\n\r\n${xml}\r\n--${boundary}--\r\n`;
    const req=http.request({host:DVR_HOST,port:DVR_HTTP_PORT,method:'POST',path:'/dvr/cmd',timeout:timeoutMs,headers:{Authorization:authHeader(),'Content-Type':`multipart/form-data; boundary=${boundary}`,'Content-Length':Buffer.byteLength(body),Connection:'close'}},res=>{
      let sample='';
      if(res.headers['set-cookie']&&res.headers['set-cookie'][0]){const m=/sessionid=([^;]+)/.exec(res.headers['set-cookie'][0]); if(m) sessionCookie=`sessionid=${m[1]}`;}
      res.on('data',d=>{ if(sample.length<220) sample+=d.toString('utf8'); });
      res.on('end',()=>resolve({ok:res.statusCode===200,status:res.statusCode,cookie:sessionCookie,sample}));
    });
    req.on('timeout',()=>req.destroy(new Error('login timeout')));
    req.on('error',e=>resolve({ok:false,status:0,error:e.message}));
    req.end(body);
  });
}
async function ensureLogin(){
  const xml='<?xml version="1.0" encoding="UTF-8"?><DVR Platform="Hi3520"><GetConfiguration File="system.xml" /></DVR>';
  const r=await postDvrCmd(xml);
  console.log(r.ok?`[AUTH] login OK cookie=${sessionCookie||'(none)'}`:`[AUTH] login FAIL status=${r.status||0} error=${r.error||''}`);
  return r;
}

function writeMjpegHeader(res){
  if(res.headersSent) return;
  res.writeHead(200,{
    'Content-Type':'multipart/x-mixed-replace; boundary=rt7frame',
    'Cache-Control':'no-store, no-cache, must-revalidate, max-age=0',
    'Pragma':'no-cache',
    'Connection':'close',
    'X-RT7-Version':VERSION,
    'X-Accel-Buffering':'no'
  });
  if (res.flushHeaders) res.flushHeaders();
}
function sendJpegPart(res, jpg){
  writeMjpegHeader(res);
  res.write(`--rt7frame\r\nContent-Type: image/jpeg\r\nContent-Length: ${jpg.length}\r\nX-Seq: ${++lastSeq}\r\n\r\n`,'latin1');
  res.write(jpg);
  res.write('\r\n','latin1');
  lastFrameBytes=jpg.length; lastRelayAt=Date.now(); extracted++;
}

function relayExtractJpeg(res, ch=DVR_CHANNEL){
  totalClients++; activeClients++; relayStarts++;
  let closed=false, buf=Buffer.alloc(0), headerDone=false;
  const path=dvrPath(ch);
  const sock=net.createConnection({host:DVR_HOST,port:DVR_HTTP_PORT});
  function closeAll(reason){
    if(closed) return; closed=true; activeClients=Math.max(0,activeClients-1);
    try{sock.destroy();}catch(_){ } try{ if(!res.writableEnded) res.end(); }catch(_){ }
    console.log(`[RELAY] close reason=${reason} active=${activeClients}`);
  }
  res.on('close',()=>closeAll('browser_close')); res.on('error',()=>closeAll('browser_error'));
  sock.setTimeout(RELAY_TIMEOUT_MS);
  sock.on('connect',()=>{
    const lines=[`GET ${path} HTTP/1.0`,`Host: ${DVR_HOST}:${DVR_HTTP_PORT}`,`Authorization: ${authHeader()}`,`Magic: ${MAGIC}`,sessionCookie?`Cookie: ${sessionCookie}`:null,'User-Agent: RT7-V6.4F-JPEG-Extractor','Accept: */*','Connection: close'].filter(Boolean);
    console.log(`[RELAY] start CH${String(ch).padStart(2,'0')} ${DVR_HOST}:${DVR_HTTP_PORT}${path} clients=${activeClients}`);
    sock.write(lines.join('\r\n')+'\r\n\r\n','latin1');
  });
  sock.on('data',chunk=>{
    if(closed) return; lastBytes += chunk.length;
    if(!headerDone){
      buf=Buffer.concat([buf,chunk]);
      const idx=buf.indexOf('\r\n\r\n');
      if(idx<0){ if(buf.length>16384){ relayErrors++; lastError='DVR_HEADER_TOO_LONG'; closeAll('header_too_long'); } return; }
      const hdr=buf.subarray(0,idx).toString('latin1');
      if(!/^HTTP\/\d\.\d\s+200/i.test(hdr)){
        relayErrors++; lastError=(hdr.split(/\r?\n/)[0]||'DVR_NON_200');
        if(!res.headersSent){res.writeHead(502,{'Content-Type':'text/plain; charset=utf-8'}); res.end(lastError+'\n');}
        closeAll('dvr_non_200'); return;
      }
      chunk=buf.subarray(idx+4); buf=Buffer.alloc(0); headerDone=true;
      writeMjpegHeader(res); // 先讓手機知道是 MJPEG；後續只送真正 JPEG frame。
      if(!chunk.length) return;
    }
    buf=Buffer.concat([buf,chunk]);
    while(true){
      const soi=buf.indexOf(Buffer.from([0xff,0xd8]));
      if(soi<0){ if(buf.length>MAX_BUFFER_BYTES) buf=buf.subarray(buf.length-1024); return; }
      if(soi>0) buf=buf.subarray(soi);
      const eoi=buf.indexOf(Buffer.from([0xff,0xd9]),2);
      if(eoi<0){ if(buf.length>MAX_BUFFER_BYTES) buf=buf.subarray(0, MAX_BUFFER_BYTES); return; }
      const jpg=buf.subarray(0,eoi+2);
      buf=buf.subarray(eoi+2);
      if(jpg.length<MIN_JPEG_BYTES){ droppedSmall++; continue; }
      sendJpegPart(res,jpg);
      if(res.writableLength>512*1024){ relayErrors++; lastError='BROWSER_BACKPRESSURE'; closeAll('browser_backpressure'); return; }
    }
  });
  sock.on('timeout',()=>{ relayErrors++; lastError='DVR_SOCKET_TIMEOUT'; closeAll('dvr_timeout'); });
  sock.on('error',e=>{ relayErrors++; lastError=e.message; if(!res.headersSent){res.writeHead(502,{'Content-Type':'text/plain; charset=utf-8'});res.end(e.message+'\n');} closeAll('dvr_error'); });
  sock.on('close',()=>closeAll('dvr_close'));
}

function htmlPage(){
 const host=localIPv4(), base=`http://${host}:${PORT}`;
 return `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>RT7 V6.4F Direct Socket Relay</title><style>body{margin:0;background:#06242a;color:#fff;font-family:Arial,'Microsoft JhengHei',sans-serif}.wrap{max-width:760px;margin:auto;padding:32px 22px}h1{font-size:44px;line-height:1.12}.card{background:#fff;color:#123;border-radius:24px;padding:22px;margin:20px 0}.video{width:100%;border-radius:18px;background:#000}.btn{display:inline-block;background:#11aee8;color:#fff;padding:14px 18px;border-radius:12px;margin:8px;text-decoration:none;font-weight:700}.muted{color:#607080;font-size:15px;line-height:1.6}</style></head><body><div class="wrap"><h1>RT7 V6.4F<br>Direct Socket Relay</h1><div class="card"><b>LAN Bridge：</b>${base}<br><b>JPEG Extract Relay：</b>/relay/CH01.mjpg<br><span class="muted">本版不直接轉送 DVR 原始 octet-stream；改從資料流抽出真正 JPEG，重包成標準 MJPEG，解決手機 MJPEG_ERROR。</span></div><div class="card"><img class="video" src="/relay/CH01.mjpg?ts=${Date.now()}" onerror="document.getElementById('st').textContent='MJPEG_ERROR：請按重連，或開 /status 檢查 last_error。'"><p id="st" class="muted">ONLINE 時會直接顯示 /relay/CH01.mjpg。</p><a class="btn" href="javascript:location.reload()">重連</a><a class="btn" href="/relay/CH01.mjpg">直接MJPEG</a><a class="btn" href="/status">狀態JSON</a></div></div></body></html>`;
}
function statusJson(){return {ok:true,version:VERSION,mode:'jpeg_frame_extract_no_decode',dvr:{host:DVR_HOST,port:DVR_HTTP_PORT,user:DVR_USER,path:dvrPath(DVR_CHANNEL)},local:{port:PORT,host:localIPv4(),direct:`http://${localIPv4()}:${PORT}/direct`},auth:{cookie_set:!!sessionCookie},relay:{active_clients:activeClients,total_clients:totalClients,starts:relayStarts,errors:relayErrors,last_bytes:lastBytes,last_frame_bytes:lastFrameBytes,seq:lastSeq,extracted,droppedSmall,age_ms:lastRelayAt?Date.now()-lastRelayAt:null,last_error:lastError}};}

const server=http.createServer(async (req,res)=>{
 const url=new URL(req.url,`http://${req.headers.host||'localhost'}`);
 if(url.pathname==='/'||url.pathname==='/direct'){res.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-store'});res.end(htmlPage());return;}
 if(url.pathname==='/status'||url.pathname==='/status.json'){res.writeHead(200,{'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store'});res.end(JSON.stringify(statusJson(),null,2));return;}
 if(url.pathname==='/login'){const r=await ensureLogin();res.writeHead(200,{'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store'});res.end(JSON.stringify(r,null,2));return;}
 const m=/^\/relay\/(CH)?(\d+)\.mjpg$/i.exec(url.pathname)||/^\/stream\/(CH)?(\d+)\.mjpg$/i.exec(url.pathname);
 if(m){relayExtractJpeg(res,m[2]||DVR_CHANNEL);return;}
 res.writeHead(404,{'Content-Type':'text/plain; charset=utf-8'});res.end('RT7 V6.4F 404\n/direct\n/status\n/relay/CH01.mjpg\n');
});
server.on('clientError',(err,socket)=>{try{socket.end('HTTP/1.1 400 Bad Request\r\n\r\n')}catch(_){}});
server.listen(PORT,'0.0.0.0',async()=>{console.log(`${VERSION} starting...`);console.log(`DVR: iCATCH ${DVR_USER}@${DVR_HOST}:${DVR_HTTP_PORT} channel=${DVR_CHANNEL}`);console.log(`Template: ${TEMPLATE}`);console.log(`Direct LAN View: http://${localIPv4()}:${PORT}/direct`);await ensureLogin();console.log('Press Ctrl+C to stop.');});
