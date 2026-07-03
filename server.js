'use strict';
const http = require('http');
const { spawn } = require('child_process');
const { Buffer } = require('buffer');

const VERSION = 'RT7_V6_5A_DIRECT_FFMPEG_RGB_BMP_VIEW';
const DVR_HOST = process.env.DVR_HOST || '192.168.0.123';
const DVR_HTTP_PORT = parseInt(process.env.DVR_HTTP_PORT || '80', 10);
const DVR_USER = process.env.DVR_USER || 'admin';
const DVR_PASS = process.env.DVR_PASS || 'vbnmmnbv';
const LOCAL_PORT = parseInt(process.env.LOCAL_PORT || '8787', 10);
const LOCAL_PUBLIC_HOST = process.env.LOCAL_PUBLIC_HOST || '';
const CATCH_MAGIC = process.env.ICATCH_MAGIC || '39e739de-8d69-aadb-78b9-946a2905858d';
const PATH = process.env.ICATCH_HTTP_TEMPLATE || '/cgi-bin/net_video.cgi?hq=0&iframe=15&pframe=15&audio=0';
const FPS = parseInt(process.env.FPS || '5', 10);
const W = parseInt(process.env.OUT_W || '480', 10);
const H = parseInt(process.env.OUT_H || '270', 10);
const FRAME_BYTES = W * H * 3;
const POLL_MS = parseInt(process.env.POLL_MS || '250', 10);
const BLUE_RATIO = parseFloat(process.env.BLUE_RATIO || '0.62');
const BLUE_CENTER_RATIO = parseFloat(process.env.BLUE_CENTER_RATIO || '0.55');

let cookie = '';
let ff = null;
let rawBuf = Buffer.alloc(0);
let latestBmp = null;
let latestAt = 0;
let seq = 0, accepted = 0, droppedBlue = 0, droppedSmall = 0, restarts = 0, totalRaw = 0;
let lastError = '';
let lastBlue = null;
let clients = 0;

function basicAuth(){ return 'Basic ' + Buffer.from(`${DVR_USER}:${DVR_PASS}`).toString('base64'); }
function now(){ return Date.now(); }

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
  console.log(`[AUTH] login status=${r.status} cookie=${cookie || '(none)'}`);
}

function blueStats(rgb){
  let blue=0, total=0, centerBlue=0, centerTotal=0;
  const x1=Math.floor(W*0.18), x2=Math.floor(W*0.82), y1=Math.floor(H*0.18), y2=Math.floor(H*0.82);
  // sample every 4 pixels for speed
  for(let y=0; y<H; y+=4){
    for(let x=0; x<W; x+=4){
      const i=(y*W+x)*3;
      const r=rgb[i], g=rgb[i+1], b=rgb[i+2];
      const isBlue = b > 105 && b > r*1.35 && b > g*1.15;
      if(isBlue) blue++;
      total++;
      if(x>=x1 && x<=x2 && y>=y1 && y<=y2){ if(isBlue) centerBlue++; centerTotal++; }
    }
  }
  return {blue: total?blue/total:0, center: centerTotal?centerBlue/centerTotal:0};
}

function makeBmp(rgb){
  const rowBytes = W*3;
  const pad = (4 - (rowBytes % 4)) % 4;
  const imgSize = (rowBytes + pad) * H;
  const fileSize = 54 + imgSize;
  const out = Buffer.alloc(fileSize);
  out.write('BM',0,'ascii');
  out.writeUInt32LE(fileSize,2);
  out.writeUInt32LE(54,10);
  out.writeUInt32LE(40,14);
  out.writeInt32LE(W,18);
  out.writeInt32LE(H,22); // bottom-up BMP
  out.writeUInt16LE(1,26);
  out.writeUInt16LE(24,28);
  out.writeUInt32LE(0,30);
  out.writeUInt32LE(imgSize,34);
  let p=54;
  for(let y=H-1; y>=0; y--){
    const row=y*W*3;
    for(let x=0; x<W; x++){
      const i=row+x*3;
      out[p++] = rgb[i+2]; // B
      out[p++] = rgb[i+1]; // G
      out[p++] = rgb[i];   // R
    }
    for(let k=0;k<pad;k++) out[p++]=0;
  }
  return out;
}

function startFfmpeg(){
  if(ff) return;
  restarts++;
  const url = `http://${DVR_HOST}:${DVR_HTTP_PORT}${PATH}`;
  const headerLines = [`Authorization: ${basicAuth()}`, cookie ? `Cookie: ${cookie}` : '', `Magic: ${CATCH_MAGIC}`, 'User-Agent: RT7-V6.5A'].filter(Boolean).join('\r\n') + '\r\n';
  const args = [
    '-hide_banner','-loglevel','error',
    '-fflags','nobuffer','-flags','low_delay','-analyzeduration','0','-probesize','32768',
    '-headers', headerLines,
    '-i', url,
    '-an','-vf',`fps=${FPS},scale=${W}:${H}:flags=fast_bilinear`,
    '-pix_fmt','rgb24','-f','rawvideo','pipe:1'
  ];
  console.log(`[FFMPEG] start ${url} fps=${FPS} out=${W}x${H}`);
  ff = spawn('ffmpeg', args, {stdio:['ignore','pipe','pipe']});
  rawBuf = Buffer.alloc(0);
  ff.stdout.on('data', chunk=>{
    totalRaw += chunk.length;
    rawBuf = Buffer.concat([rawBuf, chunk]);
    // keep only newest if client/CPU lags
    if(rawBuf.length > FRAME_BYTES * 6) rawBuf = rawBuf.slice(rawBuf.length - FRAME_BYTES * 3);
    while(rawBuf.length >= FRAME_BYTES){
      const frame = rawBuf.slice(0, FRAME_BYTES);
      rawBuf = rawBuf.slice(FRAME_BYTES);
      seq++;
      const st = blueStats(frame);
      lastBlue = st;
      if(st.blue >= BLUE_RATIO || st.center >= BLUE_CENTER_RATIO){
        droppedBlue++;
        if(droppedBlue % 20 === 0) console.log(`[PIPE] DROP_BLUE_KEEP_LAST blue=${st.blue.toFixed(2)} center=${st.center.toFixed(2)} droppedBlue=${droppedBlue}`);
        continue;
      }
      latestBmp = makeBmp(frame);
      latestAt = now();
      accepted++;
      if(accepted % 10 === 0) console.log(`[PIPE] accept seq=${seq} accepted=${accepted} age_ms=0 bmp=${latestBmp.length} clients=${clients} blue=${st.blue.toFixed(2)}`);
    }
  });
  ff.stderr.on('data', d=>{ const s=d.toString('utf8').trim(); if(s){ lastError=s.slice(-500); console.log('[FFMPEG]', s); } });
  ff.on('close', code=>{
    console.log(`[FFMPEG] close code=${code}`);
    ff=null; lastError=`ffmpeg_close_${code}`;
    setTimeout(async()=>{ await login(); startFfmpeg(); }, 1200);
  });
  ff.on('error', e=>{ console.log('[FFMPEG] error', e.message); lastError=e.message; ff=null; setTimeout(startFfmpeg,1200); });
}

function statusJson(){
  const age = latestAt ? now()-latestAt : null;
  return {ok:true, version:VERSION, mode:'ffmpeg_raw_rgb_to_bmp_poll_blue_filter',
    dvr:{host:DVR_HOST, port:DVR_HTTP_PORT, user:DVR_USER, path:PATH},
    local:{port:LOCAL_PORT, host:LOCAL_PUBLIC_HOST || '(auto)'},
    stream:{online:!!latestBmp && age < 5000, seq, accepted, age_ms:age, bmp_bytes:latestBmp?latestBmp.length:0, total_raw:totalRaw, droppedBlue, droppedSmall, restarts, clients, ffmpeg_running:!!ff, last_blue:lastBlue, last_error:lastError}
  };
}

function page(host){
  return `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>RT7 V6.5A BMP View</title>
<style>body{margin:0;background:#062326;color:#123;font-family:Arial,'Noto Sans TC',sans-serif}.wrap{max-width:900px;margin:auto;padding:28px}.title{font-size:52px;line-height:1.15;color:#fff;font-weight:900}.card{background:#fff;border-radius:28px;padding:22px;margin:24px 0}.hint{font-size:26px;line-height:1.45}.video{width:100%;border-radius:18px;background:#111;display:block}.stat{font-size:25px;color:#5f6f80;line-height:1.35;margin-top:14px}.btn{display:inline-block;background:#19aee6;color:white;text-decoration:none;border-radius:14px;padding:14px 22px;margin:8px 6px;font-size:23px;font-weight:800}@media(max-width:600px){.wrap{padding:18px}.title{font-size:46px}.hint,.stat{font-size:22px}.btn{font-size:21px}}</style>
</head><body><div class="wrap"><div class="title">RT7 V6.5A<br>Direct BMP Poll</div><div class="card hint"><b>LAN Bridge：</b>http://${host}<br><b>顯示：</b>/frame.bmp 每 ${POLL_MS}ms 更新<br>本版改用 FFmpeg 解 DVR，輸出 RGB，再由 Node 轉 BMP；手機不解 MJPEG、不讀壞 JPG，並過濾藍底 VIDEO LOSS。</div><div class="card"><img id="img" class="video" src="/frame.bmp?t=${Date.now()}"><div id="stat" class="stat">讀取中...</div><a class="btn" href="javascript:reloadImg()">重讀一張</a><a class="btn" href="/status">狀態 JSON</a><a class="btn" href="/frame.bmp" target="_blank">單張 BMP</a></div></div>
<script>let lastA=-1;const img=document.getElementById('img'),stat=document.getElementById('stat');function reloadImg(){img.src='/frame.bmp?t='+Date.now()}async function tick(){try{const j=await fetch('/status?t='+Date.now(),{cache:'no-store'}).then(r=>r.json());const s=j.stream||{};stat.textContent=(s.online?'ONLINE':'WAIT')+' accepted='+s.accepted+' seq='+s.seq+' age_ms='+s.age_ms+' blueDrop='+s.droppedBlue+' bmp='+s.bmp_bytes;if(s.accepted!==lastA&&s.online){lastA=s.accepted;reloadImg()}}catch(e){stat.textContent='poll error '+e}}setInterval(tick,${POLL_MS});tick()</script></body></html>`;
}

const server = http.createServer((req,res)=>{
  const u = new URL(req.url, 'http://x');
  if(u.pathname==='/' || u.pathname==='/direct'){
    clients++;
    res.writeHead(200, {'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-store'});
    res.end(page(req.headers.host || `127.0.0.1:${LOCAL_PORT}`));
    setTimeout(()=>{clients=Math.max(0,clients-1);},1000);
    return;
  }
  if(u.pathname==='/status'){
    res.writeHead(200, {'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store','Access-Control-Allow-Origin':'*'});
    res.end(JSON.stringify(statusJson(), null, 2)); return;
  }
  if(u.pathname==='/frame.bmp'){
    if(!latestBmp){ res.writeHead(503, {'Content-Type':'text/plain; charset=utf-8','Cache-Control':'no-store'}); res.end('NO_BMP_FRAME_YET'); return; }
    res.writeHead(200, {'Content-Type':'image/bmp','Content-Length':latestBmp.length,'Cache-Control':'no-store, no-cache, must-revalidate, max-age=0','Pragma':'no-cache','Access-Control-Allow-Origin':'*'});
    res.end(latestBmp); return;
  }
  if(u.pathname==='/frame.jpg'){
    res.writeHead(302, {Location:'/frame.bmp'}); res.end(); return;
  }
  res.writeHead(404, {'Content-Type':'text/plain'}); res.end('404');
});

(async function main(){
  console.log(`${VERSION} starting...`);
  console.log(`DVR: iCATCH ${DVR_USER}@${DVR_HOST}:${DVR_HTTP_PORT} channel=1`);
  console.log(`Phone URL: http://${LOCAL_PUBLIC_HOST || '192.168.0.55'}:${LOCAL_PORT}/direct`);
  await login();
  startFfmpeg();
  server.listen(LOCAL_PORT, '0.0.0.0', ()=>console.log(`[LAN] server http://0.0.0.0:${LOCAL_PORT}/ public=${LOCAL_PUBLIC_HOST || '192.168.0.55'}:${LOCAL_PORT}`));
})();
