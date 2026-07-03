'use strict';
// RT7_V6_5B_DIRECT_FFMPEG_IFRAME_MJPEG_FIX
// Goal: iCATCH net_video.cgi -> FFmpeg H264 decode -> browser MJPEG/JPEG polling.
// Fixes V6.5A green BMP by NOT using BMP/raw RGB. FFmpeg outputs real JPEG frames.
// Also changes DVR request to iframe=1&pframe=0 by default to reduce P-frame corruption.

const http = require('http');
const { spawn } = require('child_process');
const jpeg = require('jpeg-js');

const VERSION = 'RT7_V6_5B_DIRECT_FFMPEG_IFRAME_MJPEG_FIX';
const PORT = Number(process.env.LOCAL_PORT || 8787);
const HOST = process.env.VR_HOST || '192.168.0.123';
const DVR_PORT = Number(process.env.VR_HTTP_PORT || 80);
const USER = process.env.VR_USER || 'admin';
const PASS = process.env.VR_PASS || 'vbnmmnbv';
const FPS = Number(process.env.OUT_FPS || 5);
const WIDTH = Number(process.env.OUT_WIDTH || 480);
const CHANNEL = process.env.VR_CHANNEL || '1';
// Important: pframe=0 asks DVR for I-frame-only style stream when supported.
const PATH = process.env.CATCH_HTTP_TEMPLATE || `/cgi-bin/net_video.cgi?hq=0&iframe=1&pframe=0&audio=0`;
const BLUE_DROP = process.env.BLUE_DROP !== '0';

let ff = null;
let latestJpg = null;
let latestSeq = 0;
let latestAt = 0;
let accepted = 0, droppedBlue = 0, droppedBad = 0, restarts = 0, clients = 0;
let lastError = '';
let sessionCookie = '';

function authHeader(){ return 'Basic ' + Buffer.from(`${USER}:${PASS}`).toString('base64'); }
function dvrUrl(){ return `http://${HOST}:${DVR_PORT}${PATH}`; }

function login(cb){
  // Login with configuration endpoint just to obtain session cookie; stream also carries Basic Auth.
  const body = '----maya\r\nContent-Disposition: form-data; name="datafile"; filename="command.xml"\r\nContent-Type: text/xml\r\n\r\n<?xml version="1.0" encoding="UTF-8"?><DVR Platform="Hi3520"><GetConfiguration File="system.xml" /></DVR>\r\n----maya--\r\n';
  const req = http.request({host: HOST, port: DVR_PORT, path:'/dvr/cmd', method:'POST', headers:{
    'Authorization': authHeader(), 'Content-Type':'multipart/form-data; boundary=--maya', 'Content-Length': Buffer.byteLength(body), 'Connection':'close'
  }, timeout: 3000}, res => {
    const sc = res.headers['set-cookie'];
    if (sc && sc[0]) sessionCookie = sc[0].split(';')[0];
    res.resume(); res.on('end', () => { console.log(`[AUTH] status=${res.statusCode} cookie=${sessionCookie||'(none)'}`); cb && cb(); });
  });
  req.on('timeout', ()=>req.destroy(new Error('login timeout')));
  req.on('error', e => { console.log('[AUTH] error', e.message); cb && cb(); });
  req.end(body);
}

function isProbablyBlueVideoLoss(jpg){
  if (!BLUE_DROP) return false;
  if (!jpg || jpg.length < 4000) return false;
  try {
    const img = jpeg.decode(jpg, {useTArray:true, maxMemoryUsageInMB:32});
    const {width:w, height:h, data} = img;
    if (!w || !h) return false;
    let blueish=0, greenish=0, total=0;
    const stepX = Math.max(1, Math.floor(w/48));
    const stepY = Math.max(1, Math.floor(h/32));
    // Sample center area; VIDEO LOSS has huge blue/green solid areas.
    for(let y=Math.floor(h*0.18); y<Math.floor(h*0.86); y+=stepY){
      for(let x=Math.floor(w*0.05); x<Math.floor(w*0.95); x+=stepX){
        const i=(y*w+x)*4, r=data[i], g=data[i+1], b=data[i+2];
        total++;
        if (b > 130 && b > r*1.45 && b > g*1.10) blueish++;
        if (g > 120 && g > r*1.45 && g > b*1.10) greenish++;
      }
    }
    const br=blueish/Math.max(1,total), gr=greenish/Math.max(1,total);
    return br > 0.62 || gr > 0.62;
  } catch(e){
    droppedBad++; return true;
  }
}

function extractJpegs(chunker){
  let buf = Buffer.alloc(0);
  return function onData(chunk){
    buf = Buffer.concat([buf, chunk]);
    while (true) {
      const s = buf.indexOf(Buffer.from([0xff,0xd8]));
      if (s < 0) { if (buf.length > 2_000_000) buf = Buffer.alloc(0); return; }
      const e = buf.indexOf(Buffer.from([0xff,0xd9]), s+2);
      if (e < 0) { if (s > 0) buf = buf.slice(s); return; }
      const jpg = buf.slice(s, e+2);
      buf = buf.slice(e+2);
      chunker(jpg);
    }
  };
}

function startPipe(){
  if (ff) return;
  restarts++;
  const url = dvrUrl();
  console.log(`[FFMPEG] start ${url} fps=${FPS} width=${WIDTH} iframe_only path=${PATH}`);
  const headers = [`Authorization: ${authHeader()}`, sessionCookie ? `Cookie: ${sessionCookie}` : '', 'Connection: close'].filter(Boolean).join('\r\n') + '\r\n';
  const args = [
    '-hide_banner','-loglevel','warning',
    '-fflags','nobuffer+discardcorrupt','-flags','low_delay','-strict','experimental',
    '-analyzeduration','1000000','-probesize','1000000',
    '-headers', headers,
    '-i', url,
    '-an','-vf',`fps=${FPS},scale=${WIDTH}:-2`,
    '-q:v','5','-f','image2pipe','-vcodec','mjpeg','pipe:1'
  ];
  ff = spawn('ffmpeg', args, {stdio:['ignore','pipe','pipe']});
  const onJpg = (jpg) => {
    if (jpg.length < 4500) { droppedBad++; return; }
    if (isProbablyBlueVideoLoss(jpg)) { droppedBlue++; return; }
    latestJpg = jpg; latestSeq++; latestAt = Date.now(); accepted++;
    if (latestSeq % 25 === 0) console.log(`[PIPE] accept seq=${latestSeq} jpg=${jpg.length} age=0 accepted=${accepted} droppedBlue=${droppedBlue}`);
  };
  ff.stdout.on('data', extractJpegs(onJpg));
  ff.stderr.on('data', d => { const s=d.toString().trim(); if(s){ lastError=s.slice(-300); console.log('[FFMPEG]', s.split('\n').slice(-1)[0]); }});
  ff.on('exit', (code, sig)=>{ console.log(`[FFMPEG] exit code=${code} sig=${sig}`); ff=null; setTimeout(startPipe, 1200); });
}

function page(){ return `<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><title>${VERSION}</title>
<style>body{margin:0;background:#062326;color:#123;font-family:Arial,'Noto Sans TC',sans-serif}.wrap{max-width:720px;margin:auto;padding:28px 18px}h1{font-size:44px;color:white}.card{background:white;border-radius:24px;padding:22px;margin:18px 0}.img{width:100%;border-radius:16px;background:#111;min-height:180px;object-fit:contain}button{font-size:24px;font-weight:700;border:0;border-radius:14px;padding:14px 22px;background:#12aee8;color:white;margin:8px}.meta{font-size:24px;color:#64748b;line-height:1.4}</style></head><body><div class="wrap"><h1>RT7 V6.5B Direct FFmpeg JPEG View</h1><div class="card"><b>LAN Bridge：</b>http://${localHost()}:${PORT}<br><b>顯示：</b>/frame.jpg 每 200ms 更新<br>V6.5B：不用 BMP，改 FFmpeg 直接輸出 JPEG；DVR 使用 iframe=1&pframe=0，降低綠屏/藍屏。</div><div class="card"><img id="im" class="img"><div id="m" class="meta">loading...</div><button onclick="reload()">重讀一張</button><button onclick="location='/status'">狀態 JSON</button><button onclick="location='/frame.jpg?x='+Date.now()">單張 JPG</button></div></div><script>
const im=document.getElementById('im'), m=document.getElementById('m'); let last=0;
function reload(){ fetch('/status?x='+Date.now()).then(r=>r.json()).then(s=>{m.textContent=(s.online?'ONLINE':'WAIT')+' seq='+s.seq+' age_ms='+s.age_ms+' accepted='+s.accepted+' droppedBlue='+s.droppedBlue+' restarts='+s.restarts; if(s.seq&&s.seq!==last){last=s.seq; im.src='/frame.jpg?seq='+s.seq+'&t='+Date.now();}}).catch(e=>m.textContent='poll '+e); }
setInterval(reload,200); reload();</script></body></html>`; }
function localHost(){ const os=require('os'); for(const ns of Object.values(os.networkInterfaces())) for(const n of ns||[]) if(n.family==='IPv4'&&!n.internal&&n.address.startsWith('192.168.')) return n.address; return '127.0.0.1'; }

const server = http.createServer((req,res)=>{
  if (req.url.startsWith('/status')) {
    const age = latestAt ? Date.now()-latestAt : 999999;
    res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
    res.end(JSON.stringify({ok:true,version:VERSION,dvr:{host:HOST,port:DVR_PORT,path:PATH},local:{port:PORT,direct:`http://${localHost()}:${PORT}/direct`},online:!!latestJpg&&age<5000,seq:latestSeq,age_ms:age,bytes:latestJpg?latestJpg.length:0,accepted,droppedBlue,droppedBad,restarts,clients,lastError}, null, 2)); return;
  }
  if (req.url.startsWith('/frame.jpg')) {
    if (!latestJpg) { res.writeHead(503, {'Content-Type':'text/plain'}); res.end('NO_FRAME_YET'); return; }
    res.writeHead(200, {'Content-Type':'image/jpeg','Cache-Control':'no-store, no-cache, must-revalidate','Pragma':'no-cache','Access-Control-Allow-Origin':'*'});
    res.end(latestJpg); return;
  }
  if (req.url.startsWith('/direct') || req.url === '/') { res.writeHead(200, {'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-store'}); res.end(page()); return; }
  res.writeHead(404); res.end('not found');
});

server.listen(PORT, '0.0.0.0', ()=>{ console.log(`${VERSION} starting...`); console.log(`DVR: iCATCH ${USER}@${HOST}:${DVR_PORT} channel=${CHANNEL}`); console.log(`Phone URL: http://${localHost()}:${PORT}/direct`); login(startPipe); });
