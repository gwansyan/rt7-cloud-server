// RT7 DVR LAN Bridge V2 - SoCatch / DVR Bridge RUN FIX
// 在「DVR 同一個內網」的 Windows / Mac / Linux 電腦執行。
// 功能：用 FFmpeg 從 DVR RTSP 擷取 CH01~CH04 JPEG，主動上傳到 Railway。
//
// 必填：
//   RAILWAY_URL=https://你的-railway.up.railway.app
// 選填：
//   RT7_DVR_BRIDGE_TOKEN=rt7-dvr-bridge
//   DVR_HOST=192.168.0.123
//   DVR_USER=admin
//   DVR_PASS=
//   DVR_RTSP_PORT=554
//   DVR_CHANNELS=1,2,3,4
//   DVR_RTSP_TEMPLATE=rtsp://{user}:{pass}@{host}:{port}/cam/realmonitor?channel={ch}&subtype=0
//   INTERVAL_MS=3000
//
// 成功時 Railway /api/rt7/dvr/bridge/status 會顯示 online:true。

const { spawn, execFile } = require('child_process');
const http = require('http');
const https = require('https');

const RAILWAY_URL = (process.env.RAILWAY_URL || '').replace(/\/+$/, '');
const TOKEN = process.env.RT7_DVR_BRIDGE_TOKEN || 'rt7-dvr-bridge';
const DVR_HOST = process.env.DVR_HOST || '192.168.0.123';
const DVR_USER = process.env.DVR_USER || 'admin';
const DVR_PASS = process.env.DVR_PASS || '';
const DVR_RTSP_PORT = process.env.DVR_RTSP_PORT || '554';
const CHANNELS = String(process.env.DVR_CHANNELS || '1,2,3,4').split(',').map(s => parseInt(s.trim(), 10)).filter(Boolean);
const INTERVAL_MS = Math.max(1000, parseInt(process.env.INTERVAL_MS || '3000', 10) || 3000);
const RTSP_TIMEOUT_MS = Math.max(3000, parseInt(process.env.RTSP_TIMEOUT_MS || '9000', 10) || 9000);
const DEBUG = String(process.env.DEBUG || '').trim() === '1';

const DEFAULT_TEMPLATES = [
  // Dahua-like
  'rtsp://{user}:{pass}@{host}:{port}/cam/realmonitor?channel={ch}&subtype=0',
  'rtsp://{user}:{pass}@{host}:{port}/cam/realmonitor?channel={ch}&subtype=1',
  // Hikvision-like
  'rtsp://{user}:{pass}@{host}:{port}/Streaming/Channels/{ch}01',
  'rtsp://{user}:{pass}@{host}:{port}/Streaming/Channels/{ch}02',
  'rtsp://{user}:{pass}@{host}:{port}/ISAPI/Streaming/channels/{ch}01',
  // Generic / older DVR / SoCatch-like candidates
  'rtsp://{user}:{pass}@{host}:{port}/ch{ch}/main/av_stream',
  'rtsp://{user}:{pass}@{host}:{port}/ch{ch}/sub/av_stream',
  'rtsp://{user}:{pass}@{host}:{port}/user={user}&password={pass}&channel={ch}&stream=0.sdp',
  'rtsp://{user}:{pass}@{host}:{port}/user={user}&password={pass}&channel={ch}&stream=1.sdp',
  'rtsp://{user}:{pass}@{host}:{port}/chID={ch}&streamType=main',
  'rtsp://{user}:{pass}@{host}:{port}/chID={ch}&streamType=sub',
  'rtsp://{user}:{pass}@{host}:{port}/live/ch{ch}',
  'rtsp://{user}:{pass}@{host}:{port}/video{ch}',
  'rtsp://{user}:{pass}@{host}:{port}/av{ch}_0',
  'rtsp://{user}:{pass}@{host}:{port}/av{ch}_1',
  'rtsp://{user}:{pass}@{host}:{port}/{ch}',
  'rtsp://{host}:{port}/user={user}&password={pass}&channel={ch}&stream=0.sdp',
  'rtsp://{host}:{port}/chID={ch}&streamType=main'
];
const TEMPLATES = process.env.DVR_RTSP_TEMPLATE ? [process.env.DVR_RTSP_TEMPLATE] : DEFAULT_TEMPLATES;

function enc(v) { return encodeURIComponent(String(v || '')); }
function fill(tpl, ch) {
  return tpl
    .replace(/\{host\}/g, DVR_HOST)
    .replace(/\{port\}/g, DVR_RTSP_PORT)
    .replace(/\{user\}/g, enc(DVR_USER))
    .replace(/\{pass\}/g, enc(DVR_PASS))
    .replace(/\{ch\}/g, String(ch))
    .replace(/\{channel\}/g, String(ch));
}
function mask(url) {
  return String(url).replace(/:([^:@/]*?)@/, ':****@');
}
function isJpeg(buf) { return buf && buf.length > 4 && buf[0] === 0xff && buf[1] === 0xd8; }

function checkFfmpeg() {
  return new Promise(resolve => {
    execFile('ffmpeg', ['-version'], { windowsHide: true, timeout: 3000 }, (err, stdout) => {
      if (err) return resolve({ ok:false, error:String(err.message || err) });
      resolve({ ok:true, version:String(stdout || '').split('\n')[0] });
    });
  });
}

function ffmpegSnapshot(rtspUrl) {
  return new Promise((resolve) => {
    const args = [
      '-hide_banner', '-loglevel', DEBUG ? 'info' : 'error',
      '-rtsp_transport', 'tcp',
      '-stimeout', String(Math.max(1000000, RTSP_TIMEOUT_MS * 1000)),
      '-i', rtspUrl,
      '-frames:v', '1',
      '-q:v', '3',
      '-f', 'image2pipe',
      '-vcodec', 'mjpeg',
      '-'
    ];
    const p = spawn('ffmpeg', args, { windowsHide: true });
    const chunks = [];
    let err = '';
    const timer = setTimeout(() => { try { p.kill('SIGKILL'); } catch (_) {} }, RTSP_TIMEOUT_MS + 1500);
    p.stdout.on('data', d => chunks.push(d));
    p.stderr.on('data', d => { err += d.toString(); });
    p.on('close', code => {
      clearTimeout(timer);
      const buf = Buffer.concat(chunks);
      resolve({ ok: isJpeg(buf), code, buffer: buf, error: err.trim().slice(0, 900), rtspUrl });
    });
    p.on('error', e => {
      clearTimeout(timer);
      resolve({ ok: false, error: String(e && e.message || e), rtspUrl });
    });
  });
}

function upload(ch, jpeg) {
  return new Promise((resolve) => {
    if (!RAILWAY_URL) return resolve({ ok:false, error:'RAILWAY_URL_EMPTY' });
    const id = 'CH' + String(ch).padStart(2, '0');
    const url = new URL(RAILWAY_URL + '/api/rt7/dvr/bridge/upload/' + encodeURIComponent(id));
    const lib = url.protocol === 'https:' ? https : http;
    const req = lib.request(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'image/jpeg',
        'Content-Length': jpeg.length,
        'X-RT7-Bridge-Token': TOKEN,
        'User-Agent': 'RT7-DVR-LAN-Bridge/2.0'
      },
      timeout: 10000
    }, res => {
      const chunks = [];
      res.on('data', d => chunks.push(d));
      res.on('end', () => {
        const body = Buffer.concat(chunks).toString('utf8');
        resolve({ ok: res.statusCode >= 200 && res.statusCode < 300, status: res.statusCode, body });
      });
    });
    req.on('timeout', () => { try { req.destroy(new Error('upload timeout')); } catch (_) {} resolve({ ok:false, error:'upload timeout' }); });
    req.on('error', e => resolve({ ok:false, error:String(e && e.message || e) }));
    req.end(jpeg);
  });
}

const working = new Map();
async function oneChannel(ch) {
  const templates = working.has(ch) ? [working.get(ch), ...TEMPLATES.filter(t => t !== working.get(ch))] : TEMPLATES;
  let lastErr = '';
  for (const tpl of templates) {
    const url = fill(tpl, ch);
    if (DEBUG) console.log(`[CH${ch}] try`, mask(url));
    const snap = await ffmpegSnapshot(url);
    if (!snap.ok) {
      lastErr = snap.error || ('ffmpeg code ' + snap.code);
      console.log(`[CH${ch}] RTSP fail`, lastErr.slice(0, 160), mask(url));
      continue;
    }
    working.set(ch, tpl);
    const up = await upload(ch, snap.buffer);
    console.log(`[CH${ch}] jpeg=${snap.buffer.length} upload=${up.ok ? 'OK' : 'FAIL'} status=${up.status || ''} ${up.error || ''}`);
    if (!up.ok && up.body) console.log(`[CH${ch}] upload body`, up.body.slice(0, 300));
    return;
  }
  console.log(`[CH${ch}] all RTSP templates failed. Last error=${lastErr.slice(0, 220)}`);
  console.log(`[CH${ch}] 建議：確認 DVR 已啟用 RTSP，或設定 DVR_RTSP_TEMPLATE 為正確 RTSP URL。`);
}
async function loop() {
  for (const ch of CHANNELS) await oneChannel(ch);
  setTimeout(loop, INTERVAL_MS);
}

(async () => {
  console.log('RT7 DVR LAN Bridge V2');
  console.log('Railway:', RAILWAY_URL || '(missing RAILWAY_URL)');
  console.log('DVR:', DVR_USER + '@' + DVR_HOST + ':' + DVR_RTSP_PORT, 'channels=', CHANNELS.join(','));
  console.log('Token:', TOKEN ? '(set)' : '(empty)');
  const ff = await checkFfmpeg();
  if (!ff.ok) {
    console.log('[ERROR] 找不到 ffmpeg。請先安裝 FFmpeg，並確認 ffmpeg.exe 在 PATH。', ff.error);
    process.exitCode = 1;
    return;
  }
  console.log('FFmpeg:', ff.version);
  if (!RAILWAY_URL) console.log('[ERROR] RAILWAY_URL_EMPTY，請設定 Railway 網址。');
  console.log('Press Ctrl+C to stop.');
  loop();
})();
