const express = require('express');
const fs = require('fs');
const path = require('path');
const http = require('http');
const crypto = require('crypto');
const WebSocket = require('ws');
const jpeg = require('jpeg-js');
let webpush = null;
try { webpush = require('web-push'); } catch (_) { webpush = null; }

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: '/ws' });

app.use(express.json({ limit: '8mb' }));
app.use(express.urlencoded({ extended: true, limit: '8mb' }));

const DATA_DIR = process.env.RT7_DATA_DIR || path.join(__dirname, 'data');
const EVENT_LOG = path.join(DATA_DIR, 'rt7_event_log.jsonl');
const DEVICES_FILE = path.join(DATA_DIR, 'devices.json');
const LEGACY_DEVICES_FILE = path.join(DATA_DIR, 'rt7_devices.json');

const PUSH_SUBS_FILE = path.join(DATA_DIR, 'push_subscriptions.json');
const VAPID_FILE = path.join(DATA_DIR, 'vapid_keys.json');

// V5.7A user login/register
const USERS_FILE = path.join(DATA_DIR, 'rt7_users.json');
const SESSIONS_FILE = path.join(DATA_DIR, 'rt7_sessions.json');
// V5.7D master device UID binding
const MASTER_REGISTRY_FILE = path.join(DATA_DIR, 'rt7_master_registry.json');
const AUTH_COOKIE = 'rt7_sid';
const PLATFORM_COOKIE = 'rt7_platform_admin';

function rt7ReadJsonFile_(file, fallback) {
  try { return JSON.parse(fs.readFileSync(file, 'utf8') || 'null') || fallback; } catch (_) { return fallback; }
}
function rt7WriteJsonFile_(file, data) {
  ensureDataDir();
  fs.writeFileSync(file, JSON.stringify(data, null, 2), 'utf8');
}

function rt7NormalizeMasterUid_(v) {
  v = safeString(v).trim().toUpperCase().replace(/[^A-Z0-9\-_:]/g, '');
  if (!v) return '';
  if (v.indexOf('RT7-MASTER-') !== 0) v = 'RT7-MASTER-' + v.replace(/^RT7[-_]?/,'').replace(/^MASTER[-_]?/,'');
  return v.slice(0, 64);
}
function rt7DefaultMasterUid_() {
  return rt7NormalizeMasterUid_(process.env.RT7_MASTER_UID || process.env.MASTER_UID || 'RT7-MASTER-0001');
}
function rt7ReadMasterRegistry_() {
  ensureDataDir();
  const fallback = { master_uid: rt7DefaultMasterUid_(), owner: null, devices: ['#1','#2','#3','#4'], created_at: nowIso(), updated_at: nowIso() };
  const obj = rt7ReadJsonFile_(MASTER_REGISTRY_FILE, fallback);
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return fallback;
  obj.master_uid = rt7NormalizeMasterUid_(obj.master_uid || fallback.master_uid) || fallback.master_uid;
  obj.devices = Array.isArray(obj.devices) && obj.devices.length ? obj.devices : ['#1','#2','#3','#4'];
  obj.real_master_uid = rt7NormalizeMasterUid_(obj.real_master_uid || obj.heartbeat_master_uid || '');
  obj.master_uid_verified = !!(obj.real_master_uid && obj.real_master_uid === obj.master_uid);
  obj.master_online = !!(obj.last_master_heartbeat && (Date.now() - Date.parse(obj.last_master_heartbeat)) < 120000);
  return obj;
}
function rt7SaveMasterRegistry_(obj) {
  obj = obj && typeof obj === 'object' ? obj : {};
  obj.master_uid = rt7NormalizeMasterUid_(obj.master_uid || rt7DefaultMasterUid_()) || rt7DefaultMasterUid_();
  obj.devices = Array.isArray(obj.devices) && obj.devices.length ? obj.devices : ['#1','#2','#3','#4'];
  obj.updated_at = nowIso();
  rt7WriteJsonFile_(MASTER_REGISTRY_FILE, obj);
  return obj;
}

// V5.8E: multi-community master UID registry.
// masters 是多筆在線主門禁清單，不再讓 B 社區新模組覆蓋 A 社區 UID。
function rt7MasterMap_(master) {
  master = master && typeof master === 'object' ? master : rt7ReadMasterRegistry_();
  if (!master.masters || typeof master.masters !== 'object' || Array.isArray(master.masters)) master.masters = {};
  // migrate legacy single heartbeat into masters map for backward compatibility
  const legacyUid = rt7NormalizeMasterUid_(master.real_master_uid || master.heartbeat_master_uid || '');
  if (legacyUid && !master.masters[legacyUid]) {
    master.masters[legacyUid] = {
      uid: legacyUid,
      master_uid: legacyUid,
      ip: master.heartbeat_ip || master.registered_ip || '',
      device_id: master.heartbeat_device_id || '#1',
      mac: master.heartbeat_mac || '',
      first_seen: master.last_master_heartbeat || master.updated_at || nowIso(),
      last_heartbeat: master.last_master_heartbeat || master.updated_at || nowIso(),
      source: 'legacy_migrate'
    };
  }
  return master.masters;
}
function rt7MasterOnline_(m) {
  const t = Date.parse((m && (m.last_heartbeat || m.last_master_heartbeat)) || '');
  return !!(t && (Date.now() - t) < 120000);
}
function rt7GetMasterInfo_(uid) {
  uid = rt7NormalizeMasterUid_(uid || '');
  if (!uid) return null;
  const master = rt7ReadMasterRegistry_();
  const map = rt7MasterMap_(master);
  const m = map[uid] || null;
  return m ? Object.assign({}, m, { online: rt7MasterOnline_(m) }) : null;
}
function rt7UpdateMasterHeartbeat_(uid, data) {
  uid = rt7NormalizeMasterUid_(uid || '');
  if (!uid) return null;
  const master = rt7ReadMasterRegistry_();
  const map = rt7MasterMap_(master);
  const now = nowIso();
  const old = map[uid] || {};
  map[uid] = Object.assign({}, old, {
    uid,
    master_uid: uid,
    ip: safeString(data && data.ip || old.ip || '').trim(),
    device_id: safeString(data && data.device_id || old.device_id || '#1').trim() || '#1',
    mac: safeString(data && data.mac || old.mac || '').trim(),
    first_seen: old.first_seen || now,
    last_heartbeat: now,
    last_seen: now,
    source: data && data.source || old.source || 'heartbeat'
  });
  master.masters = map;
  // Keep legacy fields as the latest heartbeat only for old pages/API compatibility.
  // Login verification in V5.8E uses masters[community_uid], not this single global value.
  master.real_master_uid = uid;
  master.heartbeat_master_uid = uid;
  master.heartbeat_device_id = map[uid].device_id;
  master.heartbeat_ip = map[uid].ip || safeString(data && data.request_ip || '');
  master.last_master_heartbeat = now;
  master.master_online = true;
  const users = rt7ReadUsers_();
  const boundBySameUid = users.some(u => rt7NormalizeMasterUid_(u.master_uid || '') === uid);
  master.master_uid_verified = !!boundBySameUid;
  rt7SaveMasterRegistry_(master);
  return map[uid];
}
function rt7MasterUidUsedByOtherCommunity_(uid, community) {
  uid = rt7NormalizeMasterUid_(uid || '');
  const thisKey = rt7CommunityKey_(community || '');
  if (!uid) return '';
  const users = rt7ReadUsers_();
  for (const u of users) {
    if (rt7NormalizeMasterUid_(u.master_uid || '') === uid && rt7CommunityKey_(rt7CommunityName_(u)) !== thisKey) return rt7CommunityName_(u);
  }
  return '';
}
function rt7KnownMastersArray_() {
  const master = rt7ReadMasterRegistry_();
  const map = rt7MasterMap_(master);
  return Object.keys(map).sort().map(uid => Object.assign({}, map[uid], { online: rt7MasterOnline_(map[uid]) }));
}
function rt7NormalizeDeviceIds_(v) {
  const arr = Array.isArray(v) ? v : String(v || '').split(/[,\s]+/);
  const out = [];
  arr.forEach(x => {
    x = safeString(x).trim().toUpperCase();
    if (/^[1-4]$/.test(x)) x = '#' + x;
    if (/^#[1-4]$/.test(x) && !out.includes(x)) out.push(x);
  });
  return out.length ? out : ['#1'];
}
function rt7UserSystemEnabled_(u) {
  return !!(u && u.enabled !== false && u.system_enabled !== false && rt7NormalizeMasterUid_(u.master_uid || ''));
}
function rt7RealMasterVerifyForUser_(u) {
  const userUid = rt7NormalizeMasterUid_(u && u.master_uid || '');
  const info = rt7GetMasterInfo_(userUid);
  const online = !!(info && info.online);
  // V5.8E: 每個社區用自己帳號綁定的 master_uid 去查 masters map。
  // 不再拿全域 real_master_uid 比對，避免 B 社區新模組覆蓋 A 社區。
  const verified = !!(userUid && info);
  const ok = verified;
  let reason = '';
  if (!userUid) reason = 'USER_MASTER_UID_EMPTY';
  else if (!info) reason = 'COMMUNITY_MASTER_UID_NOT_SEEN_BY_HEARTBEAT';
  return { ok, reason, user_master_uid:userUid, bound_master_uid:userUid, real_master_uid:info && info.uid || '', online, verified, persistent_verified:verified, last_master_heartbeat:info && info.last_heartbeat || '', heartbeat_ip:info && info.ip || '', community:rt7CommunityName_(u) };
}
function rt7UserAllowedDeviceIds_(u) {
  if (!u) return ['#1','#2','#3','#4'];
  if (!rt7UserSystemEnabled_(u)) return [];
  if ((u.role || 'user') === 'admin') return ['#1','#2','#3','#4'];
  return rt7NormalizeDeviceIds_(u.devices || u.device_ids || '#1');
}
function rt7FilterDevicesForRequest_(req, devices) {
  const u = req && (req.rt7User || rt7GetSessionUser_(req));
  if (!u) return devices;
  const allowed = new Set(rt7UserAllowedDeviceIds_(u));
  return (devices || []).filter(d => allowed.has(d.id));
}
function rt7GetVapidKeys_() {
  ensureDataDir();
  if (!webpush) return null;
  const envPub = process.env.RT7_VAPID_PUBLIC_KEY || '';
  const envPri = process.env.RT7_VAPID_PRIVATE_KEY || '';
  if (envPub && envPri) return { publicKey: envPub, privateKey: envPri, source: 'env' };
  let keys = rt7ReadJsonFile_(VAPID_FILE, null);
  if (!keys || !keys.publicKey || !keys.privateKey) {
    keys = webpush.generateVAPIDKeys();
    rt7WriteJsonFile_(VAPID_FILE, keys);
  }
  return Object.assign({ source: 'data/vapid_keys.json' }, keys);
}
function rt7SetupWebPush_() {
  if (!webpush) return { ok:false, error:'web-push package missing' };
  const keys = rt7GetVapidKeys_();
  if (!keys) return { ok:false, error:'no vapid keys' };
  try {
    webpush.setVapidDetails(process.env.RT7_VAPID_SUBJECT || 'mailto:rt7@example.com', keys.publicKey, keys.privateKey);
    return { ok:true, publicKey: keys.publicKey, source: keys.source };
  } catch (e) { return { ok:false, error:String(e && e.message || e) }; }
}
function rt7ReadPushSubs_() {
  ensureDataDir();
  const arr = rt7ReadJsonFile_(PUSH_SUBS_FILE, []);
  return Array.isArray(arr) ? arr : [];
}
function rt7SavePushSubs_(arr) {
  rt7WriteJsonFile_(PUSH_SUBS_FILE, Array.isArray(arr) ? arr : []);
}
async function rt7SendPushDoorbell_(payload) {
  const setup = rt7SetupWebPush_();
  if (!setup.ok) { console.log('[RT7_PUSH][SKIP]', setup.error); return { ok:false, sent:0, error:setup.error, failures:[{error:setup.error}] }; }
  const subs = rt7ReadPushSubs_();
  const body = JSON.stringify(Object.assign({
    type: 'doorbell',
    title: '🔔 有人按門鈴',
    body: '收到門鈴：有人按門鈴',
    url: '/rt7_cloud_original_ui_doorbell',
    tag: 'rt7-doorbell',
    time: nowIso(),
    n5: true
  }, payload || {}));
  let sent = 0, removed = 0;
  const keep = [];
  const failures = [];
  for (const sub of subs) {
    const subscription = sub.subscription || sub;
    try {
      await webpush.sendNotification(subscription, body, { TTL: 60, urgency: 'high' });
      sent++;
      keep.push(sub);
    } catch (e) {
      const code = e && (e.statusCode || e.status);
      const msg = String((e && (e.body || e.message)) || e || 'send failed').slice(0, 500);
      failures.push({ statusCode: code || 0, message: msg });
      // 400/403 often means the old subscription was created with a different VAPID key.
      // Remove it so the phone can press the notify button once and register a fresh subscription.
      if (code === 400 || code === 403 || code === 404 || code === 410) removed++;
      else { console.warn('[RT7_PUSH][SEND_FAIL]', code, msg); keep.push(sub); }
    }
  }
  if (removed) rt7SavePushSubs_(keep);
  console.log('[RT7_PUSH][DOORBELL][V56N5] sent=' + sent + ' removed=' + removed + ' total=' + subs.length + ' failures=' + failures.length);
  return { ok:true, sent, removed, total:subs.length, failures };
}

const SERVER_VERSION = 'RT7_CLOUD_SERVER_V5_8E2_COMMUNITY_SCOPED_ADMIN_ACCOUNT';

function ensureDataDir() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  // V5.6B: primary file is data/devices.json.  If an older Railway deploy already
  // has data/rt7_devices.json, migrate it once so existing #1~#4 settings survive.
  if (!fs.existsSync(DEVICES_FILE)) {
    if (fs.existsSync(LEGACY_DEVICES_FILE)) fs.copyFileSync(LEGACY_DEVICES_FILE, DEVICES_FILE);
    else fs.writeFileSync(DEVICES_FILE, JSON.stringify(defaultDevices(), null, 2), 'utf8');
  }
  if (!fs.existsSync(EVENT_LOG)) fs.writeFileSync(EVENT_LOG, '', 'utf8');
  if (!fs.existsSync(PUSH_SUBS_FILE)) fs.writeFileSync(PUSH_SUBS_FILE, '[]', 'utf8');
}

function defaultDevices() {
  return [
    { id: '#1', name: 'RT7 ESP32-S3-CAM', ip: '192.168.0.179', enabled: true },
    { id: '#2', name: '影像對講', ip: '192.168.0.11', enabled: true },
    { id: '#3', name: 'RT7 S3-CAM-A', ip: '192.168.0.12', enabled: true },
    { id: '#4', name: 'RT7 S3-CAM-B', ip: '192.168.0.13', enabled: true }
  ];
}

function nowIso() { return new Date().toISOString(); }
function safeString(v) { return (v === undefined || v === null) ? '' : String(v); }
function clientIp(req) {
  const fwd = safeString(req.headers['x-forwarded-for']).split(',')[0].trim();
  return fwd || req.socket.remoteAddress || '';
}


// ---------------- V5.7A Auth helpers ----------------
function rt7ReadUsers_() {
  ensureDataDir();
  const arr = rt7ReadJsonFile_(USERS_FILE, []);
  return Array.isArray(arr) ? arr : [];
}
function rt7SaveUsers_(arr) { rt7WriteJsonFile_(USERS_FILE, Array.isArray(arr) ? arr : []); }
function rt7ReadSessions_() {
  ensureDataDir();
  const obj = rt7ReadJsonFile_(SESSIONS_FILE, {});
  return obj && typeof obj === 'object' && !Array.isArray(obj) ? obj : {};
}
function rt7SaveSessions_(obj) { rt7WriteJsonFile_(SESSIONS_FILE, obj || {}); }
function rt7ParseCookies_(req) {
  const out = {};
  String(req.headers.cookie || '').split(';').forEach(part => {
    const i = part.indexOf('=');
    if (i > 0) out[decodeURIComponent(part.slice(0, i).trim())] = decodeURIComponent(part.slice(i + 1).trim());
  });
  return out;
}
function rt7HashPassword_(password, salt) {
  return crypto.pbkdf2Sync(String(password || ''), salt, 120000, 32, 'sha256').toString('hex');
}
function rt7NewId_(prefix) { return prefix + '_' + crypto.randomBytes(16).toString('hex'); }
function rt7PublicUser_(u) { return u ? { id:u.id, username:u.username, role:u.role || 'user', enabled:u.enabled !== false, system_enabled:u.system_enabled !== false, created_at:u.created_at, master_uid:u.master_uid || '', master_ip:u.master_ip || '', devices:rt7UserAllowedDeviceIds_(u) } : null; }
function rt7GetSessionUser_(req) {
  const sid = rt7ParseCookies_(req)[AUTH_COOKIE];
  if (!sid) return null;
  const sessions = rt7ReadSessions_();
  const ss = sessions[sid];
  if (!ss || !ss.user_id || (ss.expires_at && Date.now() > ss.expires_at)) return null;
  const user = rt7ReadUsers_().find(u => u.id === ss.user_id && u.enabled !== false);
  return user || null;
}
function rt7SetLoginCookie_(res, sid) {
  res.setHeader('Set-Cookie', AUTH_COOKIE + '=' + encodeURIComponent(sid) + '; Path=/; Max-Age=' + (60*60*24*30) + '; HttpOnly; SameSite=Lax; Secure');
}
function rt7ClearLoginCookie_(res) {
  res.setHeader('Set-Cookie', AUTH_COOKIE + '=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax; Secure');
}
const RT7_FORCE_LOGIN_COOKIE = 'rt7_main_gate_once';
function rt7SetMainGateCookie_(res) {
  // V5.7E2: one-time ticket issued only after an explicit login.
  // It lets the browser enter the main page once, then is cleared by the main-page gate.
  res.append('Set-Cookie', RT7_FORCE_LOGIN_COOKIE + '=1; Path=/rt7_cloud_original_ui_doorbell; Max-Age=120; SameSite=Lax; Secure');
}
function rt7ClearMainGateCookie_(res) {
  res.append('Set-Cookie', RT7_FORCE_LOGIN_COOKIE + '=; Path=/rt7_cloud_original_ui_doorbell; Max-Age=0; SameSite=Lax; Secure');
}
function rt7HasMainGateCookie_(req) {
  const c = rt7ParseCookies_(req);
  return c[RT7_FORCE_LOGIN_COOKIE] === '1';
}
function rt7CreateSession_(req, res, user) {
  const sid = rt7NewId_('sid');
  const sessions = rt7ReadSessions_();
  sessions[sid] = { user_id:user.id, created_at:Date.now(), expires_at:Date.now()+60*60*24*30*1000, ip:clientIp(req), user_agent:safeString(req.headers['user-agent']) };
  rt7SaveSessions_(sessions);
  rt7SetLoginCookie_(res, sid);
  return sid;
}
function rt7AuthPage_(mode, message, nextUrl) {
  const isReg = mode === 'register';
  const escHtml = (v) => String(v == null ? '' : v).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c] || c));
  const nextHidden = (!isReg && nextUrl) ? '<input type="hidden" name="next" value="' + escHtml(nextUrl) + '">' : '';
  const title = isReg ? 'RT7 註冊' : 'RT7 登入';
  const masters = isReg ? rt7KnownMastersArray_().filter(m => m && m.uid) : [];
  const usedMap = {};
  if (isReg) {
    rt7ReadUsers_().forEach(u => {
      const uid = rt7NormalizeMasterUid_(u && u.master_uid || '');
      if (uid) usedMap[uid] = rt7CommunityName_(u);
    });
  }
  const masterOptions = masters.map(m => {
    const uid = rt7NormalizeMasterUid_(m.uid || m.master_uid || '');
    const used = usedMap[uid] || '';
    const label = uid + ' / ' + (m.ip || '-') + (used ? ' / 已綁定：' + used : ' / 可綁定') + (m.online ? ' / ONLINE' : ' / OFFLINE');
    return `<option value="${escHtml(uid)}" data-ip="${escHtml(m.ip || '')}" data-used="${escHtml(used)}">${escHtml(label)}</option>`;
  }).join('');
  const loginFields = !isReg ? `
<label>社區名稱</label><input name="community" placeholder="A社區 / B社區（同帳號時必填）">
<div class="hint">V5.8E2：A社區與 B社區可同時使用 admin 帳號；若帳號重複，登入時請輸入社區名稱。</div>` : '';
  const registerFields = isReg ? `
<label>社區名稱</label><input name="community" required placeholder="例如 A社區 / B社區">
<div class="hint">V5.8E1：第一次建立 admin 時，必須先輸入社區名稱，再選擇該社區的 #1 主門禁。</div>
<label>選擇在線 #1 主門禁</label>
<select id="master_select" name="master_select"><option value="">手動輸入 / 尚未看到模組</option>${masterOptions}</select>
<div class="hint" id="master_select_hint">若 ESP32 已開機並完成 heartbeat，會出現在這裡。選取後會自動帶入 UID/IP。</div>
<label>主門禁UID</label><input id="master_uid_input" name="master_uid" placeholder="例如 RT7-MASTER-68F2299FC114">
<div class="hint">#1 RT7 主門禁唯一編號。不可與其他社區重複綁定。</div>
<label>#1 主門禁 IP</label><input id="master_ip_input" name="master_ip" placeholder="例如 192.168.0.179">
<div class="hint">選擇在線 Master 後會自動填入目前 heartbeat IP。</div>
<label>設備配對碼</label><input name="device_pair" placeholder="#1 / #2 / #3 / #4，預設 #1">
<label>註冊碼</label><input name="register_code" placeholder="預設 rt7，可由環境變數修改">
<div class="hint">每個社區第一個帳號會成為該社區 admin；同一帳號名稱可在不同社區重複使用，例如 A社區 admin、B社區 admin。</div>
<script>
(function(){
  function q(id){return document.getElementById(id)}
  function applySelection(){
    var s=q('master_select'), uid=q('master_uid_input'), ip=q('master_ip_input'), hint=q('master_select_hint');
    if(!s || !uid || !ip) return;
    var opt=s.options[s.selectedIndex];
    if(opt && opt.value){ uid.value=opt.value; ip.value=opt.getAttribute('data-ip')||''; var used=opt.getAttribute('data-used')||''; if(hint) hint.textContent = used ? ('注意：此 UID 已被社區「'+used+'」使用，不能再註冊其他社區。') : '已自動帶入在線主門禁 UID/IP。'; }
  }
  var s=q('master_select'); if(s){ s.addEventListener('change', applySelection); }
})();
</script>` : '';
  return `<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1"><title>${title}</title><style>
body{margin:0;background:#071f25;color:#10212b;font-family:system-ui,-apple-system,BlinkMacSystemFont,'Noto Sans TC',sans-serif}.wrap{max-width:430px;margin:0 auto;padding:28px 18px}.card{background:#fff;border-radius:22px;padding:22px;box-shadow:0 12px 40px #0005}.logo{color:white;text-align:center;font-weight:900;font-size:26px;margin:20px 0 26px}.sub{color:#cde6ee;text-align:center;margin-top:-18px;margin-bottom:20px}h1{margin:0 0 16px;font-size:28px}label{font-weight:800;margin-top:12px;display:block}input,select{box-sizing:border-box;width:100%;font-size:18px;padding:14px;border-radius:13px;border:1px solid #cbd6df;margin-top:7px;background:#fff}button,.btn{display:block;width:100%;box-sizing:border-box;text-align:center;border:0;border-radius:14px;background:#1197d5;color:#fff;font-size:18px;font-weight:900;padding:14px;margin-top:18px;text-decoration:none}.btn.gray{background:#41506a}.msg{background:#fff1c2;color:#5b3a00;padding:10px;border-radius:12px;margin-bottom:12px;font-weight:800}.hint{font-size:13px;color:#6b7c88;margin-top:10px;line-height:1.5}.row{display:flex;gap:10px}.row .btn{margin-top:12px}</style></head><body><div class="wrap"><div class="logo">RT7 CLOUD AI DOORBELL</div><div class="sub">使用者登入 / 註冊 / 權限保護</div><div class="card"><h1>${title}</h1>${message?`<div class="msg">${escHtml(message)}</div>`:''}<form method="post" action="${isReg?'/api/auth/register':'/api/auth/login'}">${nextHidden}<label>帳號</label><input name="username" autocomplete="username" required placeholder="例如 gwansyan"><label>密碼</label><input name="password" type="password" autocomplete="${isReg?'new-password':'current-password'}" required placeholder="至少 4 碼">${loginFields}${registerFields}<button type="submit">${isReg?'建立帳號':'登入'}</button></form><div class="row"><a class="btn gray" href="${isReg?'/rt7_login':'/rt7_register'}">${isReg?'已有帳號，去登入':'註冊新帳號'}</a></div><div class="hint">登入後才能進入主頁、GPIO、人臉資料庫、通知設定與管理頁。</div></div></div></body></html>`;
}
function rt7RequireLogin_(req, res, next) {
  const u = rt7GetSessionUser_(req);
  if (u) {
    if (!rt7UserSystemEnabled_(u)) {
      if (req.path.startsWith('/api/')) return res.status(403).json({ ok:false, error:'system_not_activated', login:'/rt7_login' });
      return res.status(403).type('html').send('<!doctype html><meta charset="utf-8"><body style="font-family:system-ui;padding:24px"><h2>RT7 帳號尚未開通或已解除綁定</h2><p>請聯絡管理者於「使用者管理」勾選開通，並綁定主門禁 UID。</p><p><a href="/api/auth/logout">登出</a></p></body>');
    }
    req.rt7User = u; return next();
  }
  if (req.path.startsWith('/api/')) return res.status(401).json({ ok:false, error:'login_required', login:'/rt7_login' });
  return res.redirect('/rt7_login?next=' + encodeURIComponent(req.originalUrl || '/rt7_cloud_original_ui_doorbell'));
}


// ---------------- V5.7E Railway Platform Admin helpers ----------------
function rt7PlatformUsername_() { return safeString(process.env.RT7_PLATFORM_ADMIN_USER || process.env.RT7_CLOUD_ADMIN_USER || 'rt7cloud'); }
function rt7PlatformPassword_() { return safeString(process.env.RT7_PLATFORM_ADMIN_PASS || process.env.RT7_CLOUD_ADMIN_PASS || 'rt7cloud'); }
function rt7PlatformSecret_() { return safeString(process.env.RT7_PLATFORM_SECRET || process.env.SESSION_SECRET || 'rt7-platform-local-secret'); }
function rt7PlatformToken_() {
  return crypto.createHmac('sha256', rt7PlatformSecret_()).update(rt7PlatformUsername_() + ':' + rt7PlatformPassword_()).digest('hex');
}
function rt7IsPlatformAdmin_(req) {
  const c = rt7ParseCookies_(req);
  return c[PLATFORM_COOKIE] && c[PLATFORM_COOKIE] === rt7PlatformToken_();
}
function rt7SetPlatformCookie_(res) {
  res.setHeader('Set-Cookie', PLATFORM_COOKIE + '=' + encodeURIComponent(rt7PlatformToken_()) + '; Path=/; Max-Age=' + (30*24*3600) + '; SameSite=Lax; HttpOnly');
}
function rt7ClearPlatformCookie_(res) {
  res.setHeader('Set-Cookie', PLATFORM_COOKIE + '=; Path=/; Max-Age=0; SameSite=Lax; HttpOnly');
}
function rt7RequirePlatformAdmin_(req, res, next) {
  if (!rt7IsPlatformAdmin_(req)) return res.redirect('/rt7_platform_login?msg=' + encodeURIComponent('請先登入 Railway 雲端管理平台'));
  next();
}
function rt7CommunityName_(u) {
  return safeString(u && (u.community || u.community_name || u.site || '')).trim() || '未分類社區';
}


// ---------------- V5.8A Community Manager helpers ----------------
function rt7CommunityKey_(name) {
  const n = safeString(name || '').trim() || '未分類社區';
  return n.toLowerCase().replace(/\s+/g,'_').replace(/[^a-z0-9_\-\u4e00-\u9fff]/gi,'').slice(0,80) || 'community';
}
function rt7BuildCommunities_() {
  const users = rt7ReadUsers_();
  const groups = {};
  users.forEach(u => {
    const name = rt7CommunityName_(u);
    const key = rt7CommunityKey_(name);
    if (!groups[key]) groups[key] = { community_id:key, community_name:name, users:[], admins:[], enabled_users:0, master_uids:new Set(), ips:new Set(), devices:new Set() };
    const g = groups[key];
    g.users.push(u.username);
    if ((u.role || 'user') === 'admin') g.admins.push(u.username);
    if (u.enabled !== false && u.system_enabled !== false) g.enabled_users++;
    if (rt7NormalizeMasterUid_(u.master_uid || '')) g.master_uids.add(rt7NormalizeMasterUid_(u.master_uid));
    if (safeString(u.master_ip || '').trim()) g.ips.add(safeString(u.master_ip).trim());
    (Array.isArray(u.devices) ? u.devices : rt7NormalizeDeviceIds_(u.devices || '')).forEach(d => g.devices.add(d));
  });
  return Object.values(groups).map(g => ({
    community_id:g.community_id,
    community_name:g.community_name,
    users:g.users,
    admins:g.admins,
    enabled_users:g.enabled_users,
    master_uids:Array.from(g.master_uids),
    master_uid:Array.from(g.master_uids)[0] || '',
    master_ips:Array.from(g.ips),
    master_ip:Array.from(g.ips)[0] || '',
    master_online: !!rt7GetMasterInfo_(Array.from(g.master_uids)[0] || ''),
    devices:Array.from(g.devices).sort(),
    user_count:g.users.length,
    admin_count:g.admins.length
  })).sort((a,b)=>String(a.community_name).localeCompare(String(b.community_name)));
}
function rt7CommunityManagerPage_(req, message) {
  const esc = (v) => String(v === undefined || v === null ? '' : v).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const communities = rt7BuildCommunities_();
  const rows = communities.map(c => `<tr><td><b>${esc(c.community_name)}</b><div class="small">${esc(c.community_id)}</div></td><td>${esc(c.admins.join(', ') || '-')}</td><td>${esc(c.users.join(', '))}</td><td>${c.enabled_users}/${c.user_count}</td><td><input name="community_name" value="${esc(c.community_name)}"></td><td><input name="master_uid" value="${esc(c.master_uid)}" placeholder="RT7-MASTER-..."></td><td><input name="master_ip" value="${esc(c.master_ip)}" placeholder="192.168.0.179"></td><td><input name="devices" value="${esc(c.devices.join(', ') || '#1,#2,#3,#4')}"></td><td><form method="post" action="/api/platform/community/update"><input type="hidden" name="community_id" value="${esc(c.community_id)}"><input type="hidden" name="old_community" value="${esc(c.community_name)}"><input type="hidden" name="community_name" value=""><input type="hidden" name="master_uid" value=""><input type="hidden" name="master_ip" value=""><input type="hidden" name="devices" value=""><button onclick="rt7CopyCommunityRow_(this.form);return true;">更新社區</button></form></td></tr>`).join('') || '<tr><td colspan="9">尚無社區資料</td></tr>';
  return `<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>RT7 社區管理</title><style>
body{margin:0;background:#eef4f7;color:#10212b;font-family:system-ui,-apple-system,'Noto Sans TC',sans-serif}.top{background:#071f25;color:white;padding:14px;display:flex;gap:10px;align-items:center}.top h1{margin:0;font-size:22px;flex:1}.top a{color:white;text-decoration:none;background:#41546b;border-radius:10px;padding:9px 12px;font-weight:900}.wrap{max-width:1250px;margin:0 auto;padding:16px}.card{background:white;border-radius:18px;padding:16px;box-shadow:0 4px 18px #0001;margin-bottom:14px;overflow:auto}.msg{background:#fff1c2;color:#5b3a00;padding:10px;border-radius:12px;margin-bottom:12px;font-weight:800}.hint{color:#64748b;font-size:13px;line-height:1.6}.small{font-size:12px;color:#64748b}table{width:100%;border-collapse:collapse;min-width:1100px}th,td{border-bottom:1px solid #e5edf2;padding:10px;text-align:left;vertical-align:top}th{background:#f6fafc}input{box-sizing:border-box;width:100%;padding:8px;border:1px solid #cbd5e1;border-radius:8px}button{border:0;border-radius:9px;background:#0ea5e9;color:white;font-weight:900;padding:9px 12px}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}.stat{background:#f6fafc;border-radius:14px;padding:12px}.value{font-size:24px;font-weight:900}.label{font-size:13px;color:#64748b;font-weight:900}@media(max-width:760px){.grid{grid-template-columns:1fr 1fr}.top h1{font-size:18px}}</style><script>
function rt7CopyCommunityRow_(form){ const tr=form.closest('tr'); ['community_name','master_uid','master_ip','devices'].forEach(n=>{ form.querySelector('input[name="'+n+'"]').value = tr.querySelector('input[name="'+n+'"]').value; }); }
</script></head><body><div class="top"><h1>RT7 社區管理</h1><a href="/rt7_platform_admin">平台首頁</a><a href="/rt7_user_manager">使用者管理</a><a href="/rt7_platform_logout">登出平台</a></div><div class="wrap">${message?`<div class="msg">${esc(message)}</div>`:''}<div class="card"><div class="grid"><div class="stat"><div class="label">社區數</div><div class="value">${communities.length}</div></div><div class="stat"><div class="label">帳號數</div><div class="value">${communities.reduce((a,c)=>a+c.user_count,0)}</div></div><div class="stat"><div class="label">已開通</div><div class="value">${communities.reduce((a,c)=>a+c.enabled_users,0)}</div></div><div class="stat"><div class="label">主門禁 UID 數</div><div class="value">${new Set(communities.flatMap(c=>c.master_uids)).size}</div></div></div><div class="hint">V5.8A：Railway 雲端服務作為獨立管理平台，以「社區」為單位管理 A社區/B社區。每個社區可有自己的 admin/user、主門禁 UID、#1 IP 與 #1~#4 設備綁定。</div></div><div class="card"><h2>社區清單</h2><table><thead><tr><th>社區</th><th>社區 admin</th><th>帳號</th><th>開通</th><th>社區名稱</th><th>主門禁 UID</th><th>#1 IP</th><th>設備</th><th>操作</th></tr></thead><tbody>${rows}</tbody></table></div><div class="card"><h2>建立新社區管理員</h2><form method="post" action="/api/platform/community/create_admin" style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr auto;gap:10px;align-items:end"><label><div class="label">社區名稱</div><input name="community" placeholder="A社區" required></label><label><div class="label">admin 帳號</div><input name="username" placeholder="admin_A" required></label><label><div class="label">密碼</div><input name="password" placeholder="至少4碼" required></label><label><div class="label">主門禁 UID</div><input name="master_uid" placeholder="RT7-MASTER-..."></label><button>建立社區 admin</button></form></div></div></body></html>`;
}

function rt7MasterCandidateFromUsers_() {
  const users = rt7ReadUsers_();
  const master = rt7ReadMasterRegistry_();
  const list = users.filter(u => u && u.enabled !== false && u.system_enabled !== false && rt7NormalizeMasterUid_(u.master_uid || ''));
  const preferred = list.find(u => (u.role || 'user') === 'admin') || list[0] || null;
  return {
    master_uid: rt7NormalizeMasterUid_((preferred && preferred.master_uid) || master.master_uid || ''),
    ip: safeString((preferred && (preferred.master_ip || preferred.masterIp || preferred.ip_hint)) || master.registered_ip || master.heartbeat_ip || '').trim(),
    username: preferred && preferred.username || ''
  };
}
function rt7PlatformLoginPage_(message) {
  const esc = (v) => String(v === undefined || v === null ? '' : v).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  return `<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>RT7 Railway 雲端管理登入</title><style>
body{margin:0;background:#071f25;color:#10212b;font-family:system-ui,-apple-system,'Noto Sans TC',sans-serif}.wrap{max-width:420px;margin:0 auto;padding:42px 18px}.logo{color:white;text-align:center;font-size:26px;font-weight:900;margin:28px 0 8px}.sub{color:#cde6ee;text-align:center;margin-bottom:24px}.card{background:white;border-radius:22px;padding:22px;box-shadow:0 12px 40px #0005}label{display:block;font-weight:900;margin-top:12px}input{box-sizing:border-box;width:100%;font-size:18px;padding:14px;border:1px solid #cbd5e1;border-radius:13px;margin-top:7px}button{width:100%;border:0;border-radius:14px;background:#0ea5e9;color:white;font-weight:900;font-size:18px;padding:14px;margin-top:18px}.msg{background:#fff1c2;color:#5b3a00;padding:10px;border-radius:12px;margin-bottom:12px;font-weight:800}.hint{font-size:13px;color:#64748b;line-height:1.6;margin-top:12px}</style></head><body><div class="wrap"><div class="logo">RT7 Railway 雲端管理平台</div><div class="sub">唯一管理帳密 / 社區與設備開通管理</div><div class="card">${message?`<div class="msg">${esc(message)}</div>`:''}<form method="post" action="/api/platform/login"><label>平台管理帳號</label><input name="username" autocomplete="username" required placeholder="rt7cloud"><label>平台管理密碼</label><input name="password" type="password" autocomplete="current-password" required><button>登入平台管理</button></form><div class="hint">此帳號獨立於 A社區/B社區住戶帳號。正式部署請在 Railway Variables 設定 RT7_PLATFORM_ADMIN_USER / RT7_PLATFORM_ADMIN_PASS。</div></div></div></body></html>`;
}
function rt7PlatformAdminPage_(req, message) {
  const users = rt7ReadUsers_().sort((a,b)=> (rt7CommunityName_(a)+String(a.username)).localeCompare(rt7CommunityName_(b)+String(b.username)));
  const devices = Array.isArray(readDevices()) ? readDevices() : [];
  const master = rt7ReadMasterRegistry_();
  const candidate = rt7MasterCandidateFromUsers_();
  const knownMasters = rt7KnownMastersArray_();
  const esc = (v) => String(v === undefined || v === null ? '' : v).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const enabledUsers = users.filter(u => u.enabled !== false && u.system_enabled !== false).length;
  const communities = Array.from(new Set(users.map(rt7CommunityName_)));
  const masterOnline = !!(master.last_master_heartbeat && (Date.now() - Date.parse(master.last_master_heartbeat)) < 120000);
  const masterVerified = !!(master.real_master_uid && master.real_master_uid === master.master_uid && masterOnline);
  const masterRows = knownMasters.map(m => `<tr><td><code>${esc(m.uid)}</code></td><td>${esc(m.ip || '-')}</td><td>${esc(m.mac || '-')}</td><td>${m.online?'<span class="ok">ONLINE</span>':'<span class="bad">OFFLINE</span>'}</td><td>${esc(m.last_heartbeat || '-')}</td></tr>`).join('') || '<tr><td colspan="5">尚未收到任何 ESP32 #1 heartbeat</td></tr>';
  const rows = users.map(u => {
    const sys = u.enabled !== false && u.system_enabled !== false && rt7NormalizeMasterUid_(u.master_uid||'');
    const devs = (u.devices || []).join(', ');
    return `<tr><td><b>${esc(u.username)}</b><div class="small">${esc(u.id)}</div></td><td><input name="community" value="${esc(rt7CommunityName_(u))}"></td><td>${esc(u.role||'user')}</td><td>${sys?'<span class="ok">開通</span>':'<span class="bad">解除</span>'}</td><td><input name="master_uid" value="${esc(u.master_uid||'')}" placeholder="RT7-MASTER-..."></td><td><input name="master_ip" value="${esc(u.master_ip || '')}" placeholder="192.168.0.179"></td><td><input name="devices" value="${esc(devs || '#1')}" placeholder="#1,#2,#3,#4"></td><td class="ops"><form method="post" action="/api/platform/user/binding"><input type="hidden" name="id" value="${esc(u.id)}"><input type="hidden" name="community" value=""><input type="hidden" name="master_uid" value=""><input type="hidden" name="master_ip" value=""><input type="hidden" name="devices" value=""><button onclick="rt7CopyRowInputs_(this.form);return true;">更新綁定</button></form><form method="post" action="/api/platform/user/system_enabled"><input type="hidden" name="id" value="${esc(u.id)}"><input type="hidden" name="system_enabled" value="${sys?'0':'1'}"><button class="${sys?'red':'green'}">${sys?'解除':'開通'}</button></form></td></tr>`;
  }).join('');
  const devRows = devices.map(d => `<tr><td>${esc(d.id)}</td><td>${esc(d.name)}</td><td>${esc(d.ip)}</td><td>${d.enabled===false?'<span class="bad">停用</span>':'<span class="ok">啟用</span>'}</td></tr>`).join('');
  return `<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>RT7 Railway 雲端管理平台</title><style>
body{margin:0;background:#eef4f7;color:#10212b;font-family:system-ui,-apple-system,'Noto Sans TC',sans-serif}.top{background:#071f25;color:white;padding:16px;display:flex;align-items:center;gap:12px}.top h1{margin:0;font-size:22px;flex:1}.top a{color:white;text-decoration:none;background:#41546b;border-radius:10px;padding:9px 12px;font-weight:900}.wrap{max-width:1250px;margin:0 auto;padding:16px}.card{background:white;border-radius:18px;padding:16px;box-shadow:0 4px 18px #0001;margin-bottom:14px;overflow:auto}.msg{background:#fff1c2;color:#5b3a00;padding:10px;border-radius:12px;margin-bottom:12px;font-weight:800}.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}.stat{background:#f6fafc;border-radius:14px;padding:12px}.label{font-size:13px;color:#64748b;font-weight:900}.value{font-size:22px;font-weight:900}.ok{color:#0a8f45;font-weight:900}.bad{color:#c62828;font-weight:900}table{width:100%;border-collapse:collapse;min-width:1050px}th,td{border-bottom:1px solid #e5edf2;padding:10px;text-align:left;vertical-align:top}th{background:#f6fafc}.small{font-size:12px;color:#64748b}input{box-sizing:border-box;width:100%;padding:8px;border:1px solid #cbd5e1;border-radius:8px}.ops{display:flex;gap:8px;flex-wrap:wrap}.ops form{display:inline}.ops button,button{border:0;border-radius:9px;background:#0ea5e9;color:white;font-weight:900;padding:9px 10px}.ops button.red{background:#d12f2f}.ops button.green{background:#0eaa5b}.hint{font-size:13px;color:#5d6b76;line-height:1.6}@media(max-width:760px){.stats{grid-template-columns:1fr 1fr}.top h1{font-size:18px}table{min-width:950px}}</style><script>
function rt7CopyRowInputs_(form){ const tr=form.closest('tr'); ['community','master_uid','master_ip','devices'].forEach(n=>{ form.querySelector('input[name="'+n+'"]').value = tr.querySelector('input[name="'+n+'"]').value; }); }
</script></head><body><div class="top"><h1>RT7 Railway 雲端管理平台</h1><a href="/rt7_community_manager">社區管理</a><a href="/rt7_user_manager">使用者管理</a><a href="/rt7_platform_logout">登出平台</a></div><div class="wrap">${message?`<div class="msg">${esc(message)}</div>`:''}<div class="card"><div class="stats"><div class="stat"><div class="label">管理平台</div><div class="value">ONLINE</div></div><div class="stat"><div class="label">社區數</div><div class="value">${communities.length}</div></div><div class="stat"><div class="label">帳號數</div><div class="value">${users.length}</div></div><div class="stat"><div class="label">已開通帳號</div><div class="value">${enabledUsers}</div></div></div><div class="hint">本頁是 Railway 雲端服務唯一管理平台，可查詢 A社區/B社區帳號、主門禁 UID、設備綁定，並可決定開通/解除。社區 admin 仍只能管理自己系統。<br>最新 heartbeat UID：${esc(master.real_master_uid || '尚未回報')} / 在線模組數：${knownMasters.filter(m=>m.online).length}/${knownMasters.length} / 最後 heartbeat：${esc(master.last_master_heartbeat || '-')} / IP：${esc(master.heartbeat_ip || '-')}</div></div>
<div class="card"><h2>#1 主門禁 UID/IP 手動取得與比對</h2><div class="hint">正常正式版應由 #1 ESP32 開機後自動 heartbeat。本區是平台管理測試/維護用：會自動帶入手機註冊/社區帳號填寫的主門禁 UID 與 #1 IP；按下後寫入 heartbeat 並立即比對綁定 UID，減少手動輸入錯誤。</div><form method="post" action="/api/platform/master/manual_verify" style="display:grid;grid-template-columns:1.3fr 1fr auto;gap:10px;align-items:end"><label><div class="label">#1 真實 Master UID</div><input name="master_uid" value="${esc(master.real_master_uid || candidate.master_uid || master.master_uid || '')}" placeholder="RT7-MASTER-68F2299FC114" required></label><label><div class="label">#1 IP</div><input name="ip" value="${esc(master.heartbeat_ip || candidate.ip || '192.168.0.179')}" placeholder="192.168.0.179"></label><button class="green" style="background:#0eaa5b">取得/比對 #1 UID</button><input type="hidden" name="device_id" value="#1"></form><div class="hint">V5.8E：比對規則改為『各社區綁定 UID 必須存在於多筆 Master Registry』，不再用單一 real_master_uid 覆蓋全部社區。</div></div>
<div class="card"><h2>多社區 Master Registry（ESP32 heartbeat 多筆清單）</h2><table><thead><tr><th>Master UID</th><th>IP</th><th>MAC</th><th>狀態</th><th>最後 heartbeat</th></tr></thead><tbody>${masterRows}</tbody></table></div>
<div class="card"><h2>社區帳號與設備綁定</h2><table><thead><tr><th>帳號</th><th>社區</th><th>角色</th><th>狀態</th><th>主門禁 UID</th><th>#1 IP</th><th>設備</th><th>平台操作</th></tr></thead><tbody>${rows || '<tr><td colspan="8">尚無帳號</td></tr>'}</tbody></table></div><div class="card"><h2>全域設備清單</h2><table><thead><tr><th>代號</th><th>名稱</th><th>IP</th><th>狀態</th></tr></thead><tbody>${devRows}</tbody></table></div><div class="card"><h2>目前預設 Master Registry</h2><pre>${esc(JSON.stringify(master,null,2))}</pre></div></div></body></html>`;
}

function rt7RequireAdmin_(req, res, next) {
  const u = rt7GetSessionUser_(req);
  if (!u) return rt7RequireLogin_(req, res, next);
  if (!rt7UserSystemEnabled_(u)) return rt7RequireLogin_(req, res, next);
  if ((u.role || 'user') !== 'admin') return res.status(403).send('需要 admin 權限');
  req.rt7User = u; return next();
}
function rt7AuthNav_(req) {
  const u = rt7GetSessionUser_(req);
  if (!u) return '<a href="/rt7_login">登入</a>';
  return '<span style="font-weight:900;color:#0a2">' + u.username + '</span> <a href="/api/auth/logout">登出</a>';
}

function rt7InvalidateUserSessions_(userId) {
  const sessions = rt7ReadSessions_();
  let changed = false;
  Object.keys(sessions).forEach(sid => {
    if (sessions[sid] && sessions[sid].user_id === userId) { delete sessions[sid]; changed = true; }
  });
  if (changed) rt7SaveSessions_(sessions);
}
function rt7CountAdmins_(users) {
  return (Array.isArray(users) ? users : []).filter(u => u && u.enabled !== false && (u.role || 'user') === 'admin').length;
}

function rt7DeviceBindStatusPage_(req, message) {
  const current = rt7GetSessionUser_(req);
  const master = rt7ReadMasterRegistry_();
  const devices = rt7FilterDevicesForRequest_(req, readDevices());
  const allDevices = readDevices();
  const users = rt7ReadUsers_();
  const esc = (v) => String(v === undefined || v === null ? '' : v).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const isAdmin = current && (current.role || 'user') === 'admin';
  const userDevices = rt7UserAllowedDeviceIds_(current);
  const onlineCut = Date.now() - 120000;
  const parseTime = (t) => { const n = Date.parse(t || ''); return Number.isFinite(n) ? n : 0; };
  const rows = (isAdmin ? allDevices : devices).map(d => {
    const bound = userDevices.includes(d.id);
    const t = parseTime(d.last_online);
    const online = t && t >= onlineCut;
    const role = d.id === '#1' ? '主門禁' : '附屬門禁';
    return `<tr class="${bound?'bound':'unbound'}">
<td><b>${esc(d.id)}</b><div class="small">${esc(role)}</div></td>
<td>${esc(d.name || '')}</td>
<td><code>${esc(d.ip || '-')}</code></td>
<td>${online?'<span class="ok">ONLINE</span>':'<span class="bad">OFFLINE</span>'}<div class="small">${esc(d.last_online || '尚無上線紀錄')}</div></td>
<td>${bound?'<span class="pill okp">已綁定</span>':'<span class="pill badp">未綁定</span>'}</td>
</tr>`;
  }).join('');
  const userRows = users.map(u => `<tr><td>${esc(u.username)}</td><td>${esc(u.role||'user')}</td><td>${u.system_enabled!==false && rt7NormalizeMasterUid_(u.master_uid||'')?'<span class="ok">開通</span>':'<span class="bad">解除</span>'}</td><td><code>${esc(u.master_uid||'')}</code></td><td>${esc((u.devices||[]).join(', '))}</td></tr>`).join('');
  return `<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1"><title>RT7 設備綁定狀態</title><style>
body{margin:0;background:#eef4f7;color:#10212b;font-family:system-ui,-apple-system,BlinkMacSystemFont,'Noto Sans TC',sans-serif}.top{background:#071f25;color:white;padding:16px 14px;display:flex;align-items:center;gap:12px}.top h1{font-size:22px;margin:0;flex:1}.top a{color:white;text-decoration:none;background:#41546b;border-radius:10px;padding:9px 12px;font-weight:900}.wrap{max-width:1100px;margin:0 auto;padding:16px}.grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}.card{background:white;border-radius:18px;padding:16px;box-shadow:0 4px 18px #0001;overflow:auto;margin-bottom:14px}.msg{background:#fff1c2;color:#5b3a00;padding:10px;border-radius:12px;margin-bottom:12px;font-weight:800}.label{font-size:13px;color:#64748b;font-weight:800}.value{font-size:18px;font-weight:900;margin:4px 0 10px}.uid{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;background:#f1f5f9;border:1px solid #d9e2ec;border-radius:10px;padding:8px;display:inline-block}.ok{color:#0a8f45;font-weight:900}.bad{color:#c62828;font-weight:900}.pill{display:inline-block;border-radius:999px;padding:4px 9px;font-weight:900}.okp{background:#e8ffe8;color:#097b35}.badp{background:#ffe8e8;color:#b42318}.small{font-size:12px;color:#64748b;margin-top:4px}table{width:100%;border-collapse:collapse;min-width:780px}th,td{border-bottom:1px solid #e5edf2;padding:10px;text-align:left;vertical-align:top}th{background:#f6fafc}.actions{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}.btn{display:inline-block;text-decoration:none;border:0;border-radius:10px;background:#159bd7;color:#fff;font-weight:900;padding:10px 12px}.btn.gray{background:#475569}.btn.green{background:#0eaa5b}input{padding:10px;border:1px solid #cbd5e1;border-radius:10px;min-width:260px}form{display:flex;gap:8px;flex-wrap:wrap;align-items:center}@media(max-width:760px){.grid{grid-template-columns:1fr}.top h1{font-size:18px}table{min-width:700px}}</style></head><body>
<div class="top">${rt7IsPlatformAdmin_(req) ? '<a href="/rt7_platform_admin">← 平台首頁</a>' : '<a href="/rt7_cloud_original_ui_doorbell">← 主頁</a>'}<h1>RT7 設備綁定狀態</h1>${rt7IsPlatformAdmin_(req) ? '<a href="/rt7_community_manager">社區管理</a>' : ''}<a href="/rt7_user_manager">使用者管理</a><a href="/api/auth/logout">登出</a></div>
<div class="wrap">${message?`<div class="msg">${esc(message)}</div>`:''}
<div class="grid"><div class="card"><div class="label">主門禁 UID</div><div class="value"><span class="uid">${esc(master.master_uid)}</span></div><div class="label">Owner</div><div class="value">${esc(master.owner || '尚未綁定')}</div><div class="label">目前登入</div><div class="value">${esc(current && current.username || '')} <span class="small">${esc(current && current.role || '')}</span></div></div>
<div class="card"><div class="label">你的設備</div><div class="value">${esc(userDevices.join(', '))}</div><div class="label">綁定說明</div><div class="small">#1 是 RT7 主門禁；#2~#4 是附屬影像門禁。admin 可查看全部設備，一般使用者只可查看綁定設備。</div>${isAdmin?`<div class="actions"><form method="post" action="/api/rt7/master/bind"><input name="master_uid" value="${esc(master.master_uid)}" placeholder="RT7-MASTER-XXXXXXXXXXXX"><button class="btn green">重新綁定主門禁</button></form></div>`:''}</div></div>
<div class="card"><h2>#1~#4 設備狀態</h2><table><thead><tr><th>代號</th><th>名稱</th><th>IP</th><th>狀態 / 最後上線</th><th>綁定</th></tr></thead><tbody>${rows || '<tr><td colspan="5">尚無設備資料</td></tr>'}</tbody></table></div>
${isAdmin?`<div class="card"><h2>使用者綁定清單</h2><table><thead><tr><th>帳號</th><th>角色</th><th>開通</th><th>主門禁 UID</th><th>設備</th></tr></thead><tbody>${userRows || '<tr><td colspan="4">尚無使用者</td></tr>'}</tbody></table></div>`:''}
<div class="actions"><a class="btn" href="/api/rt7/master/status">Master JSON</a><a class="btn" href="/api/rt7/master/devices">Devices JSON</a><a class="btn gray" href="/rt7_user_manager">使用者管理</a>
<a class="btn gray" href="/rt7_platform_login">Railway 雲端管理平台</a><a class="btn gray" href="/rt7_device_transfer_owner">轉移Owner</a></div>
</div></body></html>`;
}


function rt7DeviceTransferOwnerPage_(req, message) {
  const current = rt7GetSessionUser_(req);
  const master = rt7ReadMasterRegistry_();
  const users = rt7ReadUsers_().sort((a,b)=>String(a.username||'').localeCompare(String(b.username||'')));
  const esc = (v) => String(v === undefined || v === null ? '' : v).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const options = users.map(u => `<option value="${esc(u.id)}">${esc(u.username)} / ${esc(u.role||'user')} / ${u.enabled!==false?'帳號啟用':'帳號停用'} / ${u.system_enabled!==false?'系統開通':'系統解除'}</option>`).join('');
  const rows = users.map(u => `<tr><td><b>${esc(u.username)}</b></td><td>${esc(u.role||'user')}</td><td>${u.enabled!==false?'<span class="ok">帳號啟用</span>':'<span class="bad">帳號停用</span>'}<br>${u.system_enabled!==false?'<span class="ok">系統開通</span>':'<span class="bad">系統解除</span>'}</td><td><code>${esc(u.master_uid||'')}</code></td><td>${esc((u.devices||[]).join(', ')||'-')}</td><td>${master.owner===u.username?'<span class="owner">目前 Owner</span>':''}</td></tr>`).join('');
  return `<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1"><title>RT7 轉移主門禁 Owner</title><style>
body{margin:0;background:#eef4f7;color:#10212b;font-family:system-ui,-apple-system,BlinkMacSystemFont,'Noto Sans TC',sans-serif}.top{background:#071f25;color:white;padding:16px 14px;display:flex;align-items:center;gap:12px}.top h1{font-size:22px;margin:0;flex:1}.top a{color:white;text-decoration:none;background:#41546b;border-radius:10px;padding:9px 12px;font-weight:900}.wrap{max-width:1050px;margin:0 auto;padding:16px}.card{background:white;border-radius:18px;padding:16px;box-shadow:0 4px 18px #0001;overflow:auto;margin-bottom:14px}.msg{background:#fff1c2;color:#5b3a00;padding:10px;border-radius:12px;margin-bottom:12px;font-weight:800}.hint{font-size:13px;color:#5d6b76;line-height:1.6;margin:8px 0 16px}.uid{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;background:#f1f5f9;border:1px solid #d9e2ec;border-radius:10px;padding:8px;display:inline-block;font-weight:900}.ok{color:#0a8f45;font-weight:900}.bad{color:#c62828;font-weight:900}.owner{display:inline-block;background:#fff1c2;color:#8a4b00;border-radius:999px;padding:4px 10px;font-weight:900}select,input{box-sizing:border-box;width:100%;font-size:16px;padding:12px;border:1px solid #cbd5e1;border-radius:10px;margin:8px 0 12px}button,.btn{display:inline-block;text-decoration:none;border:0;border-radius:10px;background:#159bd7;color:#fff;font-weight:900;padding:12px 14px}.btn.gray{background:#475569}button.red{background:#d12f2f}table{width:100%;border-collapse:collapse;min-width:840px}th,td{border-bottom:1px solid #e5edf2;padding:10px;text-align:left;vertical-align:top}th{background:#f6fafc}.grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}@media(max-width:760px){.grid{grid-template-columns:1fr}.top h1{font-size:18px}table{min-width:760px}}</style></head><body>
<div class="top"><a href="/rt7_device_bind_status">← 設備綁定</a><h1>RT7 轉移主門禁 Owner</h1><a href="/rt7_user_manager">使用者管理</a><a href="/api/auth/logout">登出</a></div>
<div class="wrap">${message?`<div class="msg">${esc(message)}</div>`:''}
<div class="grid"><div class="card"><h2>目前主門禁</h2><div class="hint">主門禁 UID</div><div class="uid">${esc(master.master_uid)}</div><div class="hint">目前 Owner：<b>${esc(master.owner||'尚未綁定')}</b><br>目前登入：admin ${esc(current && current.username || '')}</div></div>
<div class="card"><h2>轉移 Owner</h2><form method="post" action="/api/rt7/master/transfer_owner" onsubmit="return confirm('確定轉移主門禁 Owner？新 Owner 會自動開通並綁定 #1~#4。')"><label>選擇新 Owner 帳號</label><select name="target_user_id" required>${options}</select><label><input type="checkbox" name="promote_admin" value="1" checked> 新 Owner 設為 admin</label><label><input type="checkbox" name="release_old" value="1"> 轉移後解除舊 Owner 的系統綁定</label><button class="red">轉移主門禁 Owner</button></form><div class="hint">此功能用於主門禁所有權轉移。轉移後，新 Owner 會綁定同一個 master_uid，並取得 #1~#4 設備權限。</div></div></div>
<div class="card"><h2>使用者清單</h2><table><thead><tr><th>帳號</th><th>角色</th><th>狀態</th><th>主門禁 UID</th><th>設備</th><th>Owner</th></tr></thead><tbody>${rows || '<tr><td colspan="6">尚無使用者</td></tr>'}</tbody></table></div>
</div></body></html>`;
}

function rt7UserManagerPage_(req, message) {
  const current = rt7GetSessionUser_(req);
  const users = rt7ReadUsers_().sort((a,b) => String(a.created_at||'').localeCompare(String(b.created_at||'')));
  const esc = (v) => String(v === undefined || v === null ? '' : v).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const activeAdminCount = users.filter(x => x.enabled !== false && x.system_enabled !== false && (x.role || 'user') === 'admin').length;
  const rows = users.map(u => {
    const isSelf = current && u.id === current.id;
    const enabled = u.enabled !== false;
    const sysEnabled = u.system_enabled !== false && !!rt7NormalizeMasterUid_(u.master_uid || '');
    const isLastActiveAdmin = sysEnabled && enabled && (u.role || 'user') === 'admin' && activeAdminCount <= 1;
    const sysDisabled = isLastActiveAdmin ? 'disabled title="至少保留一個已開通 admin"' : '';
    const sysNote = isLastActiveAdmin ? '<div class="small warn">最後一個已開通 admin 不可解除</div>' : '<div class="small">勾選立即開通；取消勾選立即解除主門禁 UID 與設備綁定。</div>';
    return `<tr>
<td><b>${esc(u.username)}</b>${isSelf?'<div class="tag self">目前登入</div>':''}<div class="small"><code>${esc(u.master_uid || '未綁定')}</code></div><div class="small">設備：${esc((u.devices||[]).join(', ') || '-')}</div></td>
<td><span class="tag ${u.role==='admin'?'admin':'user'}">${esc(u.role || 'user')}</span></td>
<td>${enabled?'<span class="ok">登入啟用</span>':'<span class="bad">帳號停用</span>'}<br>${sysEnabled?'<span class="ok">系統開通</span>':'<span class="bad">系統解除</span>'}</td>
<td class="small">${esc(u.created_at || '')}</td>
<td class="ops">
<form method="post" action="/api/auth/users/role"><input type="hidden" name="id" value="${esc(u.id)}"><select name="role"><option value="user" ${u.role!=='admin'?'selected':''}>user</option><option value="admin" ${u.role==='admin'?'selected':''}>admin</option></select><button>改角色</button></form>
<form method="post" action="/api/auth/users/enabled"><input type="hidden" name="id" value="${esc(u.id)}"><input type="hidden" name="enabled" value="${enabled?'0':'1'}"><button class="gray">${enabled?'停用帳號':'啟用帳號'}</button></form>
<form class="sysForm" method="post" action="/api/auth/users/system_enabled"><input type="hidden" name="id" value="${esc(u.id)}"><input type="hidden" name="system_enabled" value="${sysEnabled?'1':'0'}"><label class="switchLabel ${sysEnabled?'on':'off'}"><input type="checkbox" ${sysEnabled?'checked':''} ${sysDisabled} onchange="rt7SubmitSystemCheckbox_(this)"> 系統開通</label>${sysNote}</form>
<form method="post" action="/api/auth/users/password"><input type="hidden" name="id" value="${esc(u.id)}"><input name="password" placeholder="新密碼" minlength="4"><button class="blue">改密碼</button></form>
<form method="post" action="/api/auth/users/delete" onsubmit="return confirm('確定刪除帳號 ${esc(u.username)}？此動作無法復原。')"><input type="hidden" name="id" value="${esc(u.id)}"><button class="red" ${isSelf?'disabled title="不能刪除目前登入帳號"':''}>刪除</button></form>
</td>
</tr>`;
  }).join('');
  return `<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1"><title>RT7 使用者管理</title><style>
body{margin:0;background:#eef4f7;color:#10212b;font-family:system-ui,-apple-system,BlinkMacSystemFont,'Noto Sans TC',sans-serif}.top{background:#071f25;color:white;padding:16px 14px;display:flex;align-items:center;gap:12px}.top h1{font-size:22px;margin:0;flex:1}.top a{color:white;text-decoration:none;background:#41546b;border-radius:10px;padding:9px 12px;font-weight:900}.wrap{max-width:1050px;margin:0 auto;padding:16px}.card{background:white;border-radius:18px;padding:16px;box-shadow:0 4px 18px #0001;overflow:auto}.msg{background:#fff1c2;color:#5b3a00;padding:10px;border-radius:12px;margin-bottom:12px;font-weight:800}.hint{font-size:13px;color:#5d6b76;line-height:1.5;margin:8px 0 16px}table{width:100%;border-collapse:collapse;min-width:850px}th,td{border-bottom:1px solid #e5edf2;padding:10px;text-align:left;vertical-align:top}th{background:#f6fafc}.small{font-size:12px;color:#64748b}.ok{color:#0a8f45;font-weight:900}.bad{color:#c62828;font-weight:900}.tag{display:inline-block;border-radius:999px;padding:3px 8px;font-size:12px;font-weight:900;background:#e8eef4;color:#40516a;margin-top:4px}.tag.admin{background:#ffe5b5;color:#8a4b00}.tag.user{background:#dff3ff;color:#075985}.tag.self{background:#e8ffe8;color:#097b35}.ops{display:grid;grid-template-columns:repeat(5,minmax(120px,1fr));gap:8px}.ops form{display:flex;gap:5px;align-items:center}.ops input,.ops select{min-width:0;width:100%;padding:7px;border:1px solid #cbd5e1;border-radius:8px}.ops button{white-space:nowrap;border:0;border-radius:8px;background:#13a85a;color:#fff;font-weight:900;padding:8px 10px}.ops button.gray{background:#475569}.ops button.blue{background:#0b88d8}.ops button.red{background:#d12f2f}.ops button.green{background:#0eaa5b}.ops button:disabled{opacity:.45}.sysForm{align-items:center}.switchLabel{display:inline-flex;align-items:center;gap:8px;border-radius:999px;padding:8px 12px;font-weight:900;cursor:pointer;white-space:nowrap}.switchLabel input{width:20px;height:20px;min-width:20px;accent-color:#16a34a}.switchLabel input:disabled{cursor:not-allowed}.switchLabel.on{background:#dcfce7;color:#087336}.switchLabel.off{background:#fee2e2;color:#b91c1c}.warn{color:#b45309;font-weight:900}@media(max-width:720px){.ops{grid-template-columns:1fr}.top h1{font-size:18px}}</style><script>
function rt7SubmitSystemCheckbox_(cb){
  const form = cb.closest('form');
  const hidden = form.querySelector('input[name="system_enabled"]');
  const next = cb.checked ? '1' : '0';
  const msg = cb.checked ? '確定開通此帳號使用 RT7 系統？' : '確定解除此帳號的主門禁 UID、#1~#4 設備綁定與系統使用權？';
  if(!confirm(msg)) { cb.checked = !cb.checked; return false; }
  hidden.value = next;
  cb.disabled = true;
  form.submit();
  return true;
}
</script></head><body><div class="top">${rt7IsPlatformAdmin_(req) ? '<a href="/rt7_platform_admin">← 平台首頁</a>' : '<a href="/rt7_cloud_original_ui_doorbell">← 主頁</a>'}<h1>RT7 使用者管理</h1>${rt7IsPlatformAdmin_(req) ? '<a href="/rt7_community_manager">社區管理</a>' : ''}<a href="/rt7_device_bind_status">設備綁定</a><a href="/rt7_register">新增帳號</a><a href="/rt7_device_transfer_owner">轉移Owner</a><a href="/api/auth/logout">登出</a></div><div class="wrap"><div class="card">${message?`<div class="msg">${esc(message)}</div>`:''}<div class="hint">只有 admin 可進入本頁。可按右上「新增帳號」建立第二個帳號；第一個帳號自動 admin，後續帳號預設 user。可刪除帳號、停用帳號、修改角色、重設密碼，也可用「系統開通」勾選框控制帳號是否可使用本系統。解除後該帳號無法進入主頁、GPIO、人臉資料庫與通知設定；ESP32 裝置 API 不受登入保護，避免影響設備連線。</div><div class="hint"><b>帳號數：</b>${users.length}　<b>已開通 admin：</b>${activeAdminCount}　<b>目前登入：</b>${esc(current && current.username || '-')}</div><table><thead><tr><th>帳號</th><th>角色</th><th>狀態</th><th>建立時間</th><th>管理</th></tr></thead><tbody>${rows || '<tr><td colspan="5">尚無使用者</td></tr>'}</tbody></table></div></div></body></html>`;
}

function readDevices() {
  ensureDataDir();
  try {
    const raw = fs.readFileSync(DEVICES_FILE, 'utf8');
    const data = JSON.parse(raw || '[]');
    return normalizeDeviceList_(Array.isArray(data) ? data : (data.devices || []));
  } catch (e) {
    return defaultDevices();
  }
}

function normalizeDeviceList_(devices) {
  const input = Array.isArray(devices) ? devices : [];
  const byId = new Map(input.map(d => [safeString(d && d.id).trim(), d || {}]));
  return ['#1','#2','#3','#4'].map((id, idx) => {
    const d = byId.get(id) || input[idx] || {};
    const ip = safeString(d.ip || d.device_ip || d.esp_ip).trim().replace(/^https?:\/\//i,'').replace(/\/.*$/,'');
    return {
      id,
      name: safeString(d.name || d.device_name || (id === '#1' ? 'RT7 ESP32-S3-CAM' : id)).trim() || id,
      ip,
      enabled: d.enabled !== false,
      note: safeString(d.note || '').trim(),
      version: safeString(d.version || d.firmware || '').trim(),
      last_online: safeString(d.last_online || '').trim()
    };
  });
}

function saveDevices(devices) {
  ensureDataDir();
  const arr = normalizeDeviceList_(devices);
  fs.writeFileSync(DEVICES_FILE, JSON.stringify(arr, null, 2), 'utf8');
  // Keep the old filename in sync for backward compatibility with older support tools.
  try { fs.writeFileSync(LEGACY_DEVICES_FILE, JSON.stringify(arr, null, 2), 'utf8'); } catch (_) {}
  return arr;
}

function appendEvent(event) {
  ensureDataDir();
  const row = Object.assign({
    time: nowIso(),
    server: SERVER_VERSION
  }, event || {});
  fs.appendFileSync(EVENT_LOG, JSON.stringify(row) + '\n', 'utf8');
  return row;
}

function readEvents(limit = 500) {
  ensureDataDir();
  try {
    const lines = fs.readFileSync(EVENT_LOG, 'utf8').split(/\r?\n/).filter(Boolean);
    const tail = lines.slice(Math.max(0, lines.length - limit));
    return tail.map(line => {
      try { return JSON.parse(line); }
      catch (e) { return { type: 'parse_error', message: line }; }
    });
  } catch (e) {
    return [];
  }
}

function broadcast(type, payload) {
  const msg = JSON.stringify({ ok: true, type, payload, time: nowIso() });
  for (const ws of wss.clients) {
    if (ws.readyState === WebSocket.OPEN) ws.send(msg);
  }
}

function broadcastBinaryToViewers(buf) {
  for (const ws of wss.clients) {
    if (ws.readyState === WebSocket.OPEN && ws.rt7Role === 'viewer') {
      try { ws.send(buf, { binary: true }); } catch (_) {}
    }
  }
}


// V5.0I: WebSocket binary intercom relay. Phone sends PCM16 binary frames to Railway;
// Railway forwards them to ESP32 persistent esp32_pcm client over /ws.
function rt7IsPhonePcmRole_(role) {
  return role === 'phone_pcm' || role === 'intercom_phone' || role === 'phone' || role === 'webrtc_phone';
}
function rt7IsEspPcmRole_(role) {
  return role === 'esp32_pcm' || role === 'esp32_intercom' || role === 'esp32' || role === 'esp32_frame_upload' || role === 'esp32_pcm_client';
}
function rt7IsEspPcmClient_(c) {
  return !!c && (rt7IsEspPcmRole_(c.rt7Role) || rt7IsEspPcmRole_(c.rt7PcmRole) || c.rt7PcmClient === true);
}
function rt7SendToEspIntercom_(payload, opts) {
  let n = 0;
  for (const c of wss.clients) {
    if (c.readyState !== WebSocket.OPEN) continue;
    if (rt7IsEspPcmClient_(c)) {
      try { c.send(payload, opts || {}); n++; } catch (_) {}
    }
  }
  return n;
}

// V5.0N: Relay ESP32 mic PCM back only to phone/intercom clients.
// This is required for release-to-listen duplex mode.
function rt7SendToPhoneIntercom_(payload, opts) {
  let n = 0;
  for (const c of wss.clients) {
    if (c.readyState !== WebSocket.OPEN) continue;
    if (rt7IsPhonePcmRole_(c.rt7Role)) {
      try { c.send(payload, opts || {}); n++; } catch (_) {}
    }
  }
  return n;
}
const rt7WsTrace = {
  phonePcmPackets: 0,
  phonePcmBytes: 0,
  relayPcmPackets: 0,
  relayPcmBytes: 0,
  espPcmPackets: 0,
  espPcmBytes: 0,
  phoneRxPackets: 0,
  phoneRxBytes: 0,
  lastEspPcmTime: null,
  lastPhoneRxTime: null,
  lastPhonePcmTime: null,
  lastRelayTime: null
};
function rt7IntercomWsState_() {
  let phones=0, esp=0;
  for (const c of wss.clients) {
    if (c.readyState !== WebSocket.OPEN) continue;
    if (rt7IsPhonePcmRole_(c.rt7Role)) phones++;
    if (rt7IsEspPcmClient_(c)) esp++;
  }
  return {
    phones, esp,
    phone_pcm_packets: rt7WsTrace.phonePcmPackets,
    phone_pcm_bytes: rt7WsTrace.phonePcmBytes,
    relay_pcm_packets: rt7WsTrace.relayPcmPackets,
    relay_pcm_bytes: rt7WsTrace.relayPcmBytes,
    esp_pcm_packets: rt7WsTrace.espPcmPackets,
    esp_pcm_bytes: rt7WsTrace.espPcmBytes,
    phone_rx_packets: rt7WsTrace.phoneRxPackets,
    phone_rx_bytes: rt7WsTrace.phoneRxBytes,
    last_esp_pcm_time: rt7WsTrace.lastEspPcmTime,
    last_phone_rx_time: rt7WsTrace.lastPhoneRxTime,
    last_phone_pcm_time: rt7WsTrace.lastPhonePcmTime,
    last_relay_time: rt7WsTrace.lastRelayTime
  };
}

function normalizeDevice(body, req) {
  const ip = body.ip || body.device_ip || body.esp_ip || clientIp(req);
  return {
    id: body.device_id || body.device || body.id || '#1',
    name: body.device_name || body.name || 'RT7 ESP32-S3-CAM',
    ip,
    version: body.version || body.firmware || '',
    last_online: nowIso(),
    enabled: body.enabled !== false
  };
}

let doorbellState = {
  ok: true,
  count: 0,
  last: null
};

// ---------- V4.8F7 restored shared runtime state ----------
const SNAPSHOT_FILE = path.join(DATA_DIR, 'latest.jpg');
const STREAM_FRAME_FILE = path.join(DATA_DIR, 'latest_stream_frame.jpg');
let latestStreamFrame = null;
let rt7MjpegCongestUntilMs = 0;
// V5.0G: while phone PCM is active, skip Railway JPEG work to reduce audio jitter.
let rt7AudioActiveUntilMs = 0;
function rt7AudioHold_(ms) { rt7AudioActiveUntilMs = Math.max(rt7AudioActiveUntilMs, Date.now() + ms); }
function rt7AudioActive_() { return Date.now() < rt7AudioActiveUntilMs; }
const RT7_STREAM_FAST_MS = 100;
const RT7_STREAM_STABLE_MS = 140;
const RT7_STREAM_IDLE_MS = 1000;
const RT7_VIEWER_ACTIVE_TTL_MS = 12000;
const streamViewers = new Map();
let liveStreamState = {
  ok: true,
  enabled: true,
  transport: 'auto_lan_cloud',
  fps_mode: 'idle',
  adaptive_mode: 'idle_1fps',
  desired_interval_ms: RT7_STREAM_IDLE_MS,
  seq: 0,
  bytes: 0,
  time: null,
  viewer_count: 0,
  clients: 0
};
let cloudState = {
  current_device_id: '#1',
  ai_enabled: false,
  plugins: { motion:true, face:true, doorbell:true, intercom:true },
  last_snapshot: null,
  last_vision: null,
  face_gate_enabled: false,
  face_gate_auto_enabled: false,
  face_gate_auto_busy: false,
  face_gate_auto_last_ms: 0,
  face_gate_auto_cooldown_ms: 8000,
  last_face_gate: null
};
function getCurrentDevice(req) {
  const devices = readDevices();
  const qid = safeString(req?.query?.device_id || req?.query?.device || '').trim();
  const id = qid || cloudState.current_device_id || '#1';
  let dev = devices.find(d => d.id === id) || devices.find(d => d.id === '#1') || devices[0] || { id:'#1', name:'RT7 ESP32-S3-CAM', ip:'192.168.0.179' };
  if (!dev.ip) dev = Object.assign({}, dev, { ip:'192.168.0.179' });
  dev.base_url = /^https?:\/\//i.test(dev.ip) ? dev.ip.replace(/\/$/,'') : 'http://' + dev.ip;
  return dev;
}
async function proxyToEsp(req, res, espPath, method='GET') {
  const dev = getCurrentDevice(req);
  const url = dev.base_url + espPath + (req.url.includes('?') ? req.url.slice(req.url.indexOf('?')) : '');
  try {
    const opt = { method, headers: { 'Content-Type': req.headers['content-type'] || 'text/plain' } };
    if (method !== 'GET' && method !== 'HEAD') opt.body = typeof req.body === 'string' ? req.body : (Buffer.isBuffer(req.body) ? req.body : JSON.stringify(req.body || {}));
    const r = await fetch(url, opt);
    const buf = Buffer.from(await r.arrayBuffer());
    res.status(r.status);
    const ct = r.headers.get('content-type');
    if (ct) res.setHeader('content-type', ct);
    res.send(buf);
  } catch (e) {
    res.status(502).json({ ok:false, error:'ESP_PROXY_FAILED', url, message:String(e && e.message || e) });
  }
}


function registerOrUpdateDevice(dev) {
  const devices = readDevices();
  const idx = devices.findIndex(d => (d.id && d.id === dev.id) || (dev.ip && d.ip === dev.ip));
  if (idx >= 0) devices[idx] = Object.assign({}, devices[idx], dev);
  else devices.push(dev);
  saveDevices(devices);
  return dev;
}

function htmlShell(title, body, extraHead = '') {
  return `<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no"><title>${title}</title><link rel="manifest" href="/manifest.webmanifest"><meta name="theme-color" content="#071f25">${extraHead}</head><body>${body}
<script id="rt7-v48f4-button-layout-fix-js">
(function(){
  function bindTextButton(label, fn){
    Array.prototype.forEach.call(document.querySelectorAll('button'), function(b){
      if((b.textContent||'').trim() === label){
        b.style.pointerEvents='auto'; b.style.position='relative'; b.style.zIndex='2147483000';
        b.addEventListener('click', function(ev){ ev.preventDefault(); ev.stopPropagation(); try{ fn(); }catch(e){ console.log(e); } }, true);
      }
    });
  }
  function install(){
    var kill=function(el){ try{ el.style.pointerEvents='none'; el.style.zIndex='0'; }catch(e){} };
    Array.prototype.forEach.call(document.querySelectorAll('.audioOverlay,.modal,.modal-backdrop,.overlay,.backdrop,.mask,.loading,.blocker'), kill);
    bindTextButton('啟用 AI', function(){ if(window.enableAi) window.enableAi(); });
    bindTextButton('關閉 AI', function(){ if(window.disableAi) window.disableAi(); });
    bindTextButton('開始影像', function(){ if(window.startVideo) window.startVideo(); });
    bindTextButton('停止影像', function(){ if(window.stopVideo) window.stopVideo(); });
  }
  if(document.readyState==='loading') document.addEventListener('DOMContentLoaded', install); else install();
})();
</script>

<script id="rt7-v56n2-pwa-push-js">
(function(){
  if(window.__rt7PwaPushInstalled) return; window.__rt7PwaPushInstalled=true;
  function log(msg){ try{ console.log('[RT7_PWA_N2]', msg); }catch(e){} }
  // V5.8D8B: 通知鍵外框改成「全域 Service Worker registration 掃描 + inline 強制套色」。
  // 原因：Android Chrome 可能收到推播，但目前頁面 register('/sw.js') 取得的 registration 不是實際持有 subscription 的 registration，
  // 或舊 CSS class/inline border 把綠框覆蓋。因此 D8B 會掃 navigator.serviceWorker.getRegistrations()，只要任一 registration 有 subscription 就綠框，
  // 並用 inline style.setProperty(...,'important') 強制覆蓋黃框。
  function rt7SetPushLocal_(yes){ try{ localStorage.setItem('rt7_push_enabled', yes?'1':'0'); sessionStorage.setItem('rt7_push_enabled', yes?'1':'0'); }catch(_){} }
  function rt7ApplyPushButtonStyle_(mode,msg){
    try{
      var btn=document.getElementById('btnPushNotify')||document.getElementById('btnPushNotifyFloat');
      var lab=document.getElementById('lblPushNotify'); if(lab) lab.textContent='通知';
      if(!btn) return;
      btn.classList.remove('pushEnabled','pushWarn','pushErr');
      if(mode==='green'){
        btn.classList.add('pushEnabled');
        btn.style.setProperty('border-color','#22c55e','important');
        btn.style.setProperty('background','#ecfdf5','important');
        btn.style.setProperty('box-shadow','0 0 0 4px rgba(34,197,94,.24),0 2px 10px rgba(0,0,0,.1)','important');
      }else if(mode==='red'){
        btn.classList.add('pushErr');
        btn.style.setProperty('border-color','#dc2626','important');
        btn.style.setProperty('background','#fff1f2','important');
        btn.style.setProperty('box-shadow','0 2px 10px rgba(0,0,0,.1)','important');
      }else{
        btn.classList.add('pushWarn');
        btn.style.setProperty('border-color','#f59e0b','important');
        btn.style.setProperty('background','#fffbeb','important');
        btn.style.setProperty('box-shadow','0 2px 10px rgba(0,0,0,.1)','important');
      }
      btn.title = msg || '通知';
      btn.setAttribute('data-rt7-push-ui', mode);
    }catch(e){}
  }
  async function rt7GetRealPushSubscription_(){
    if(!('serviceWorker' in navigator)) throw new Error('此瀏覽器不支援 Service Worker');
    if(!('PushManager' in window)) throw new Error('此瀏覽器不支援 PushManager');
    try{ await navigator.serviceWorker.register('/sw.js',{scope:'/'}); }catch(_){ }
    try{ await navigator.serviceWorker.ready; }catch(_){ }
    var regs=[];
    try{ regs=await navigator.serviceWorker.getRegistrations(); }catch(_){ regs=[]; }
    if(!regs || !regs.length){
      try{ regs=[await navigator.serviceWorker.ready]; }catch(_){ regs=[]; }
    }
    for(var i=0;i<regs.length;i++){
      var r=regs[i];
      if(r && r.pushManager){
        try{ var sub=await r.pushManager.getSubscription(); if(sub) return {reg:r,subscription:sub,scope:r.scope||''}; }catch(_){ }
      }
    }
    var reg=null; try{ reg=await navigator.serviceWorker.ready; }catch(_){ }
    var sub=null; try{ if(reg&&reg.pushManager) sub=await reg.pushManager.getSubscription(); }catch(_){ }
    return { reg:reg, subscription:sub, scope:(reg&&reg.scope)||'' };
  }
  function state(msg, err){
    try{
      var el=document.getElementById('rt7PushState');
      if(el){ el.textContent=msg; el.style.color=err?'#dc2626':'#16a34a'; }
      var ok = /已訂閱|已啟用|已允許|伺服器已有訂閱/.test(String(msg||'')) && !err;
      rt7ApplyPushButtonStyle_(err?'red':(ok?'green':'yellow'), msg);
      log(msg);
    }catch(e){}
  }
  function b64ToUint8Array(base64String){
    var padding='='.repeat((4-base64String.length%4)%4);
    var base64=(base64String+padding).replace(/-/g,'+').replace(/_/g,'/');
    var raw=atob(base64); var out=new Uint8Array(raw.length);
    for(var i=0;i<raw.length;i++) out[i]=raw.charCodeAt(i);
    return out;
  }
  async function enableRt7Push(){
    state('推播：檢查瀏覽器支援');
    if(!('serviceWorker' in navigator)){ throw new Error('此瀏覽器不支援 Service Worker'); }
    if(!('PushManager' in window)){ throw new Error('此瀏覽器不支援 PushManager'); }
    if(!('Notification' in window)){ throw new Error('此瀏覽器不支援通知'); }
    state('推播：註冊 Service Worker');
    var reg=await navigator.serviceWorker.register('/sw.js',{scope:'/'});
    try{ await navigator.serviceWorker.ready; }catch(e){}
    state('推播：要求通知權限');
    var perm=Notification.permission;
    if(perm!=='granted') perm=await Notification.requestPermission();
    if(perm!=='granted') throw new Error('通知權限未允許：'+perm);
    state('推播：取得 VAPID key');
    var keyRes=await fetch('/api/push/vapid-public-key?_='+Date.now(),{cache:'no-store'});
    var key=await keyRes.json();
    if(!key.ok || !key.publicKey) throw new Error('推播 key 無效：'+(key.error||'no key'));
    state('推播：同步 VAPID key');
    var appKey=b64ToUint8Array(key.publicKey);
    var sub=await reg.pushManager.getSubscription();
    if(sub){
      try{
        var oldKey=sub.options&&sub.options.applicationServerKey ? new Uint8Array(sub.options.applicationServerKey) : null;
        var same=!!oldKey && oldKey.length===appKey.length;
        if(same){ for(var ki=0; ki<oldKey.length; ki++){ if(oldKey[ki]!==appKey[ki]){ same=false; break; } } }
        if(!same){ state('推播：舊訂閱 key 不同，重新訂閱'); await sub.unsubscribe(); sub=null; }
      }catch(_){ try{ await sub.unsubscribe(); }catch(__){} sub=null; }
    }
    state('推播：建立訂閱');
    if(!sub){
      sub=await reg.pushManager.subscribe({ userVisibleOnly:true, applicationServerKey:appKey });
    }
    state('推播：送出訂閱');
    var save=await fetch('/api/push/subscribe',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(sub)});
    var sj=await save.json().catch(function(){return {ok:false,error:'bad json'};});
    if(!save.ok || !sj.ok) throw new Error('訂閱儲存失敗：'+(sj.error||save.status));
    rt7SetPushLocal_(true);
    state('推播：已訂閱');
    alert('RT7 門鈴背景通知已啟用');
    return true;
  }
  async function refreshPushState(){
    try{
      // V5.8D8B：先掃描所有 SW registration 的真實 subscription。
      // 任一 registration 有 subscription => 主頁通知鍵立即強制綠框。
      if(!('Notification' in window)){ state('推播：此瀏覽器不支援通知', true); return false; }
      if(Notification.permission==='denied'){ state('推播：通知權限已封鎖', true); return false; }
      var real=await rt7GetRealPushSubscription_();
      if(real && real.subscription){
        rt7SetPushLocal_(true);
        state('推播：已訂閱');
        fetch('/api/push/subscribe',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(real.subscription)}).catch(function(){});
        log('real subscription yes scope='+(real.scope||''));
        return true;
      }
      // 保險：如果此手機已允許通知，且 Railway 目前保存至少一筆 subscription，代表剛才啟用頁已成功送出訂閱；主頁先顯示綠框，避免 false yellow。
      // 這不是 localStorage；是伺服器目前真實保存的 subscription count。
      try{
        var sr=await fetch('/api/push/state?_='+Date.now(),{cache:'no-store'});
        var sj=await sr.json();
        if(sj && sj.ok && Number(sj.subscriptions||0)>0 && Notification.permission==='granted'){
          state('推播：伺服器已有訂閱');
          log('server subscription fallback count='+sj.subscriptions);
          return true;
        }
      }catch(_){ }
      if(Notification.permission==='granted') state('推播：權限已允許，尚未訂閱');
      else state('推播：未啟用');
      log('real subscription no');
      return false;
    }catch(e){
      // 若 getRegistrations 在某些 Chrome 狀態失敗，但權限已允許且伺服器有訂閱，仍不要讓主頁錯誤黃框。
      try{
        var fr=await fetch('/api/push/state?_='+Date.now(),{cache:'no-store'});
        var fj=await fr.json();
        if(fj && fj.ok && Number(fj.subscriptions||0)>0 && window.Notification && Notification.permission==='granted'){
          state('推播：伺服器已有訂閱');
          return true;
        }
      }catch(_){ }
      state('推播：狀態讀取失敗 '+(e.message||e), true);
      return false;
    }
  }
  function onPushClick(ev){
    if(ev){ ev.preventDefault(); ev.stopPropagation(); }
    state('推播：按鍵已觸發');
    try{
      var onSetupPage = location.pathname.indexOf('/rt7_push_enable') === 0;
      if(!onSetupPage){
        setTimeout(function(){ location.href='/rt7_push_enable'; }, 30);
        return false;
      }
    }catch(_){ }
    enableRt7Push().then(refreshPushState).catch(function(e){ state('推播錯誤：'+(e.message||e), true); alert('啟用門鈴通知失敗：'+(e.message||e)); });
    return false;
  }
  window.rt7EnablePwaPush=enableRt7Push;
  window.rt7PushNotifyClick=onPushClick;
  function installPushButton(){
    var btn=document.getElementById('btnPushNotify')||document.getElementById('btnPushNotifyFloat');
    if(!btn){
      btn=document.createElement('button');
      btn.id='btnPushNotifyFloat';
      btn.textContent='🔔 啟用門鈴通知';
      btn.style.cssText='position:fixed;right:10px;bottom:92px;z-index:2147483647;background:#16a34a;color:#fff;border:0;border-radius:999px;padding:10px 12px;font-weight:900;box-shadow:0 3px 12px rgba(0,0,0,.25);';
      document.body.appendChild(btn);
    }
    btn.style.pointerEvents='auto'; btn.style.zIndex='2147483647'; btn.setAttribute('onclick','return window.rt7PushNotifyClick&&window.rt7PushNotifyClick(event)');
    btn.onclick=onPushClick;
    btn.addEventListener('click', onPushClick, true);
    btn.addEventListener('touchend', onPushClick, {capture:true, passive:false});
    if('serviceWorker' in navigator){ navigator.serviceWorker.register('/sw.js',{scope:'/'}).then(function(){log('sw registered'); refreshPushState();}).catch(function(e){state('推播：SW註冊失敗 '+e.message,true);}); }
    refreshPushState();
    setTimeout(refreshPushState, 800);
    setTimeout(refreshPushState, 2000);
  }
  function schedulePushRefresh(delay){ setTimeout(function(){ refreshPushState(); }, delay||0); }
  if(document.readyState==='loading') document.addEventListener('DOMContentLoaded', installPushButton); else installPushButton();
  window.addEventListener('pageshow', function(){ schedulePushRefresh(50); schedulePushRefresh(600); });
  window.addEventListener('focus', function(){ schedulePushRefresh(50); });
  document.addEventListener('visibilitychange', function(){ if(!document.hidden) schedulePushRefresh(80); });
})();
</script>
</body></html>`;
}

const baseCss = `
<style>
:root{--dark:#0b252b;--blue:#0b84d8;--green:#16a34a;--red:#dc2626;--gray:#475569;--line:#d8e0e8;--bg:#f6f8fb;--text:#17262a}
*{box-sizing:border-box}body{font-family:system-ui,-apple-system,"Noto Sans TC","Microsoft JhengHei",Arial,sans-serif;margin:0;background:var(--bg);color:var(--text)}
.top{background:linear-gradient(90deg,#092228,#0d2c32);color:#fff;padding:20px;text-align:center;font-weight:900}.top h1{margin:0;font-size:22px}.top p{margin:6px 0 0;color:#cdebf0}
.wrap{max-width:980px;margin:0 auto;padding:14px}.card{background:#fff;border:1px solid var(--line);border-radius:14px;padding:16px;margin:12px 0;box-shadow:0 2px 14px rgba(10,30,40,.06)}
.btn{display:inline-flex;align-items:center;justify-content:center;min-height:46px;margin:6px 8px 6px 0;padding:12px 16px;border:0;border-radius:11px;background:var(--blue);color:white;text-decoration:none;font-weight:900;font-size:16px;cursor:pointer}.green{background:var(--green)}.red{background:var(--red)}.gray{background:var(--gray)}
code{background:#eef4f8;padding:2px 6px;border-radius:6px}.status{background:#08101f;color:#d8f2ff;border-radius:10px;padding:12px;white-space:pre-wrap;font-family:ui-monospace,Consolas,monospace;font-size:13px;max-height:260px;overflow:auto}.big{font-size:44px;font-weight:900}.muted{color:#64748b}.ok{color:#16a34a;font-weight:900}.warn{color:#b45309;font-weight:900}
table{width:100%;border-collapse:collapse;background:white}th,td{border-bottom:1px solid #e6edf3;padding:9px;text-align:left;vertical-align:top;font-size:14px}th{background:#edf6ff}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px}
@media(max-width:640px){.btn{width:100%;margin:6px 0}.top h1{font-size:19px}th,td{font-size:12px;padding:7px}.wrap{padding:10px}}
</style>`;


// ---------- V5.6N PWA + Doorbell Push Notification ----------
function rt7SendPwaManifest_(req, res) {
  res.type('application/manifest+json').send(JSON.stringify({
    name: 'RT7 Cloud AI Doorbell',
    short_name: 'RT7 Doorbell',
    start_url: '/rt7_cloud_original_ui_doorbell',
    scope: '/',
    display: 'standalone',
    background_color: '#071f25',
    theme_color: '#071f25',
    description: 'RT7 Cloud AI Doorbell / GPIO / Face / Music',
    icons: [
      { src: '/rt7-icon.svg', sizes: 'any', type: 'image/svg+xml', purpose: 'any maskable' }
    ]
  }));
}
app.get('/manifest.webmanifest', rt7SendPwaManifest_);
app.get('/manifest.json', rt7SendPwaManifest_);
app.get('/rt7-icon.svg', (req, res) => { res.type('image/svg+xml').send('<svg xmlns="http://www.w3.org/2000/svg" width="512" height="512" viewBox="0 0 512 512"><rect width="512" height="512" rx="90" fill="#071f25"/><text x="256" y="302" text-anchor="middle" font-size="145" fill="white" font-family="Arial,sans-serif" font-weight="900">RT7</text></svg>'); });
app.get('/sw.js', (req, res) => {
  res.type('application/javascript').send(`
const RT7_CACHE='rt7-v56n5-pwa-cache-v1';
self.addEventListener('install', e=>{ self.skipWaiting(); e.waitUntil(caches.open(RT7_CACHE).then(c=>c.addAll(['/rt7_cloud_original_ui_doorbell','/manifest.webmanifest','/manifest.json']).catch(()=>{}))); });
self.addEventListener('activate', e=>{ e.waitUntil(self.clients.claim()); });
self.addEventListener('push', event=>{
  let data={title:'🔔 有人按門鈴', body:'收到門鈴事件', url:'/rt7_cloud_original_ui_doorbell', tag:'rt7-doorbell'};
  try{ if(event.data) data=Object.assign(data,event.data.json()); }catch(e){}
  // N5: conservative notification options. Avoid complex actions that can break some Android Chrome versions.
  const opt={
    body:data.body||'收到門鈴事件',
    tag:data.tag||'rt7-doorbell',
    renotify:true,
    requireInteraction:true,
    vibrate:[500,200,500,200,800],
    icon:'/rt7-icon.svg',
    badge:'/rt7-icon.svg',
    data:{url:data.url||'/rt7_cloud_original_ui_doorbell'}
  };
  event.waitUntil(self.registration.showNotification(data.title||'🔔 有人按門鈴', opt));
});
self.addEventListener('notificationclick', event=>{
  event.notification.close();
  const url=(event.notification.data&&event.notification.data.url)||'/rt7_cloud_original_ui_doorbell';
  event.waitUntil(clients.matchAll({type:'window',includeUncontrolled:true}).then(list=>{
    for(const c of list){ if(c.url.includes(location.origin) && 'focus' in c){ c.navigate(url); return c.focus(); } }
    return clients.openWindow(url);
  }));
});
`);
});
app.get('/api/push/vapid-public-key', (req, res) => {
  res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  const setup = rt7SetupWebPush_();
  res.json(Object.assign({ version: SERVER_VERSION, supported: !!webpush }, setup));
});
app.post('/api/push/subscribe', (req, res) => {
  const sub = req.body && req.body.subscription ? req.body.subscription : req.body;
  if (!sub || !sub.endpoint) return res.status(400).json({ ok:false, error:'missing subscription endpoint' });
  const arr = rt7ReadPushSubs_();
  const idx = arr.findIndex(x => (x.subscription || x).endpoint === sub.endpoint);
  const setup = rt7SetupWebPush_();
  const row = { subscription: sub, user_agent: safeString(req.headers['user-agent']), time: nowIso(), ip: clientIp(req), vapid_source: setup.source || '', server_public_key: setup.publicKey || '' };
  if (idx >= 0) arr[idx] = row; else arr.push(row);
  rt7SavePushSubs_(arr);
  res.json({ ok:true, count: arr.length, version: SERVER_VERSION });
});
app.get('/api/push/state', (req, res) => {
  const setup = rt7SetupWebPush_();
  res.json({ ok:true, version:SERVER_VERSION, webpush:setup, subscriptions:rt7ReadPushSubs_().length });
});
app.get('/api/push/reset', (req, res) => {
  rt7SavePushSubs_([]);
  res.set('Cache-Control', 'no-store');
  res.json({ ok:true, version:SERVER_VERSION, reset:true, subscriptions:0, note:'Server subscriptions cleared. Open /rt7_push_enable on phone and subscribe again.' });
});
app.post('/api/push/reset', (req, res) => {
  rt7SavePushSubs_([]);
  res.json({ ok:true, version:SERVER_VERSION, reset:true, subscriptions:0 });
});
app.post('/api/push/test', async (req, res) => {
  const r = await rt7SendPushDoorbell_({ title:'🔔 有人按門鈴', body:'收到門鈴：有人按門鈴', url:'/rt7_cloud_original_ui_doorbell' });
  res.json(Object.assign({ version: SERVER_VERSION }, r));
});
app.get('/api/push/test', async (req, res) => {
  const r = await rt7SendPushDoorbell_({ title:'🔔 有人按門鈴', body:'收到門鈴：有人按門鈴', url:'/rt7_cloud_original_ui_doorbell' });
  res.json(Object.assign({ version: SERVER_VERSION }, r));
});

app.get('/', (req, res) => {
  res.type('html').send(htmlShell('RT7 Cloud Server V4.3', `${baseCss}
<header class="top"><h1>RT7 CLOUD SERVER V4.3</h1><p>Doorbell + Snapshot + Event Logger + Device Registry + WebSocket</p></header>
<div class="alert" id="doorbellAlert"><div class="alertTitle">🔔 有人按門鈴</div><div class="alertMsg" id="doorbellMsg">播放音樂中收到門鈴事件。</div><div class="alertBtns"><button class="white" onclick="goBack()">返回門禁</button><button class="dark" onclick="dismissDoorbell()">繼續播放</button></div></div>
<main class="wrap">
<section class="card"><h2 class="ok">Server OK</h2><p>Railway Node.js Server is running.</p>
<a class="btn" href="/rt7_cloud_original_ui_doorbell">原始 UI 雲端門鈴</a>
<a class="btn" href="/rt7_login">登入 / 註冊</a>
<a class="btn gray" href="/rt7_user_manager">使用者管理</a>
<a class="btn gray" href="/rt7_platform_login">Railway 雲端管理平台</a>
<a class="btn green" href="/rt7_cloud_phase10_no_nodered">Phase10 雲端影像/對講/AI（無 Node-RED）</a>
<a class="btn" href="/rt7_cloud_doorbell_player">雲端門鈴播放器</a>
<a class="btn" href="/rt7_cloud_admin">雲端管理頁</a>
<a class="btn green" href="/rt7_snapshot_bridge_test">V4.2 Snapshot Bridge 測試頁</a>
<a class="btn" href="/api/rt7/camera/state">Snapshot 狀態 JSON</a>
<a class="btn" href="/api/rt7/doorbell/state">門鈴狀態 JSON</a>
<a class="btn" href="/api/events/latest">事件紀錄 JSON</a>
</section>
<section class="card"><h3>部署策略</h3><p>V4.3 採「原始手機 UI + 最新 Snapshot」：保留原始 UI 風格，只把 ESP32 主動上傳的最新照片整合到手機畫面；不混入對講或 Face Match。</p></section>
</main>`));
});



// ---------------- V5.7E Railway Platform Admin routes ----------------
app.get('/rt7_platform_login', (req, res) => res.type('html').send(rt7PlatformLoginPage_(req.query.msg || '')));
app.post('/api/platform/login', (req, res) => {
  const username = safeString(req.body.username).trim();
  const password = safeString(req.body.password);
  if (username !== rt7PlatformUsername_() || password !== rt7PlatformPassword_()) return res.status(401).type('html').send(rt7PlatformLoginPage_('平台管理帳號或密碼錯誤'));
  rt7SetPlatformCookie_(res);
  appendEvent({ type:'platform_login', username, ip:clientIp(req) });
  res.redirect('/rt7_platform_admin');
});
app.get('/rt7_platform_logout', (req, res) => { rt7ClearPlatformCookie_(res); res.redirect('/rt7_platform_login?msg=' + encodeURIComponent('已登出平台管理')); });
app.get('/rt7_platform_admin', rt7RequirePlatformAdmin_, (req, res) => {
  try {
    res.type('html').send(rt7PlatformAdminPage_(req, req.query.msg || ''));
  } catch (e) {
    console.error('[RT7_PLATFORM_ADMIN][ERROR]', e && e.stack || e);
    res.status(500).type('text/plain').send('RT7 platform admin error: ' + String(e && e.message || e));
  }
});

app.get('/rt7_community_manager', rt7RequirePlatformAdmin_, (req, res) => {
  try { res.type('html').send(rt7CommunityManagerPage_(req, req.query.msg || '')); }
  catch(e) { console.error('[RT7_COMMUNITY_MANAGER][ERROR]', e && e.stack || e); res.status(500).type('text/plain').send('RT7 community manager error: ' + String(e && e.message || e)); }
});
app.get('/api/platform/communities', rt7RequirePlatformAdmin_, (req,res)=>{
  res.json({ ok:true, version:SERVER_VERSION, communities:rt7BuildCommunities_() });
});
app.post('/api/platform/community/update', rt7RequirePlatformAdmin_, (req,res)=>{
  const oldCommunity = safeString(req.body.old_community || '').trim() || '未分類社區';
  const newCommunity = safeString(req.body.community_name || oldCommunity).trim().slice(0,80) || oldCommunity;
  const masterUid = rt7NormalizeMasterUid_(req.body.master_uid || '');
  const masterIp = safeString(req.body.master_ip || '').trim().replace(/^https?:\/\//i,'').replace(/\/.*$/,'').slice(0,80);
  const devices = rt7NormalizeDeviceIds_(req.body.devices || '#1,#2,#3,#4');
  const users = rt7ReadUsers_();
  let changed = 0;
  users.forEach(u=>{
    if (rt7CommunityName_(u) === oldCommunity) {
      u.community = newCommunity;
      if (masterUid) u.master_uid = masterUid;
      if (masterIp) u.master_ip = masterIp;
      if (devices.length) u.devices = devices;
      if (masterUid) { u.enabled = true; u.system_enabled = true; }
      changed++;
    }
  });
  rt7SaveUsers_(users);
  appendEvent({ type:'platform_community_update', old_community:oldCommunity, community:newCommunity, master_uid:masterUid, master_ip:masterIp, devices, changed, ip:clientIp(req) });
  res.redirect('/rt7_community_manager?msg=' + encodeURIComponent('已更新社區：' + newCommunity + '，帳號數：' + changed));
});
app.post('/api/platform/community/create_admin', rt7RequirePlatformAdmin_, (req,res)=>{
  const community = safeString(req.body.community || '').trim().slice(0,80) || '未分類社區';
  const username = safeString(req.body.username).trim().replace(/[^a-zA-Z0-9_@.\-]/g, '').slice(0,40);
  const password = safeString(req.body.password || '');
  const masterUid = rt7NormalizeMasterUid_(req.body.master_uid || '');
  if (!username || password.length < 4) return res.redirect('/rt7_community_manager?msg=' + encodeURIComponent('帳號或密碼太短'));
  const users = rt7ReadUsers_();
  if (users.some(u=>String(u.username).toLowerCase() === username.toLowerCase())) return res.redirect('/rt7_community_manager?msg=' + encodeURIComponent('帳號已存在：' + username));
  const usedBy = rt7MasterUidUsedByOtherCommunity_(masterUid, community);
  if (usedBy) return res.redirect('/rt7_community_manager?msg=' + encodeURIComponent('此 Master UID 已被其他社區綁定：' + usedBy));
  const onlineInfo = rt7GetMasterInfo_(masterUid);
  const salt = crypto.randomBytes(16).toString('hex');
  const u = { id:rt7NewId_('u'), username, salt, password_hash:rt7HashPassword_(password, salt), role:'admin', enabled:true, system_enabled:!!masterUid && !!onlineInfo, community, master_uid:masterUid, master_ip:(onlineInfo && onlineInfo.ip || ''), devices:['#1','#2','#3','#4'], created_at:nowIso(), ip:clientIp(req) };
  users.push(u); rt7SaveUsers_(users);
  appendEvent({ type:'platform_community_create_admin', community, username, master_uid:masterUid, ip:clientIp(req) });
  res.redirect('/rt7_community_manager?msg=' + encodeURIComponent('已建立社區 admin：' + community + ' / ' + username + (masterUid ? (onlineInfo ? '，UID 已在線並開通' : '，UID 尚未 heartbeat，暫不開通') : '，尚未綁定 UID')));
});
app.post('/api/platform/user/system_enabled', rt7RequirePlatformAdmin_, (req, res) => {
  const id = safeString(req.body.id).trim();
  const systemEnabled = safeString(req.body.system_enabled) === '1';
  const master = rt7ReadMasterRegistry_();
  const users = rt7ReadUsers_();
  const target = users.find(u => u.id === id);
  if (!target) return res.redirect('/rt7_platform_admin?msg=' + encodeURIComponent('找不到帳號'));
  target.enabled = true;
  target.system_enabled = systemEnabled;
  if (systemEnabled) {
    target.master_uid = rt7NormalizeMasterUid_(target.master_uid || master.master_uid || rt7DefaultMasterUid_());
    target.devices = (target.role || 'user') === 'admin' ? ['#1','#2','#3','#4'] : rt7NormalizeDeviceIds_(target.devices || '#1');
  } else {
    target.master_uid = '';
    target.devices = [];
    rt7InvalidateUserSessions_(id);
  }
  rt7SaveUsers_(users);
  appendEvent({ type:'platform_user_system_enabled', username:target.username, system_enabled:systemEnabled, master_uid:target.master_uid, devices:target.devices, ip:clientIp(req) });
  res.redirect('/rt7_platform_admin?msg=' + encodeURIComponent((systemEnabled?'已由平台開通：':'已由平台解除：') + target.username));
});
app.post('/api/platform/user/binding', rt7RequirePlatformAdmin_, (req, res) => {
  const id = safeString(req.body.id).trim();
  const users = rt7ReadUsers_();
  const target = users.find(u => u.id === id);
  if (!target) return res.redirect('/rt7_platform_admin?msg=' + encodeURIComponent('找不到帳號'));
  target.community = safeString(req.body.community || '').trim().slice(0, 80) || '未分類社區';
  target.master_uid = rt7NormalizeMasterUid_(req.body.master_uid || target.master_uid || rt7DefaultMasterUid_());
  target.master_ip = safeString(req.body.master_ip || target.master_ip || '').trim().replace(/^https?:\/\//i,'').replace(/\/.*$/,'').slice(0,80);
  target.devices = rt7NormalizeDeviceIds_(req.body.devices || target.devices || '#1');
  target.system_enabled = !!target.master_uid;
  target.enabled = true;
  rt7SaveUsers_(users);
  appendEvent({ type:'platform_user_binding_update', username:target.username, community:target.community, master_uid:target.master_uid, master_ip:target.master_ip, devices:target.devices, ip:clientIp(req) });
  res.redirect('/rt7_platform_admin?msg=' + encodeURIComponent('已更新平台綁定：' + target.username));
});

app.post('/api/platform/master/manual_verify', rt7RequirePlatformAdmin_, (req, res) => {
  const uid = rt7NormalizeMasterUid_(req.body.master_uid || req.body.uid || '');
  const ip = safeString(req.body.ip || '').trim() || clientIp(req);
  const deviceId = safeString(req.body.device_id || '#1').trim() || '#1';
  if (!uid) return res.redirect('/rt7_platform_admin?msg=' + encodeURIComponent('請輸入 #1 主門禁 UID'));
  const info = rt7UpdateMasterHeartbeat_(uid, { ip, request_ip:clientIp(req), device_id:deviceId, source:'manual_verify' });
  try { registerOrUpdateDevice({ id:'#1', name:'RT7 ESP32-S3-CAM', ip, last_online:nowIso(), enabled:true }); } catch(_) {}
  appendEvent({ type:'platform_master_manual_verify', master_uid:uid, device_id:deviceId, ip:clientIp(req), heartbeat_ip:ip });
  const msg = '#1 UID 已加入多社區 Master Registry：' + uid + ' / IP=' + (info && info.ip || ip);
  res.redirect('/rt7_platform_admin?msg=' + encodeURIComponent(msg));
});
app.get('/api/platform/master/manual_verify', rt7RequirePlatformAdmin_, (req, res) => {
  const uid = rt7NormalizeMasterUid_(req.query.master_uid || req.query.uid || '');
  const ip = safeString(req.query.ip || '').trim() || clientIp(req);
  if (!uid) return res.status(400).json({ ok:false, version:SERVER_VERSION, error:'MISSING_MASTER_UID' });
  const info = rt7UpdateMasterHeartbeat_(uid, { ip, request_ip:clientIp(req), device_id:safeString(req.query.device_id || '#1').trim() || '#1', source:'manual_verify_get' });
  try { registerOrUpdateDevice({ id:'#1', name:'RT7 ESP32-S3-CAM', ip, last_online:nowIso(), enabled:true }); } catch(_) {}
  res.json({ ok:true, version:SERVER_VERSION, master_uid:uid, real_master_uid:uid, verified:true, online:true, heartbeat_ip:ip, last_master_heartbeat:info && info.last_heartbeat || '', known_masters:rt7KnownMastersArray_() });
});

// V5.8E1 public selector for phone registration. Only sanitized UID/IP/status are exposed.
app.get('/api/rt7/master/public_masters', (req, res) => {
  const users = rt7ReadUsers_();
  const used = {};
  users.forEach(u => {
    const uid = rt7NormalizeMasterUid_(u && u.master_uid || '');
    if (uid && !used[uid]) used[uid] = rt7CommunityName_(u);
  });
  const masters = rt7KnownMastersArray_().map(m => ({
    uid: rt7NormalizeMasterUid_(m.uid || m.master_uid || ''),
    master_uid: rt7NormalizeMasterUid_(m.uid || m.master_uid || ''),
    ip: safeString(m.ip || ''),
    mac: safeString(m.mac || ''),
    online: !!m.online,
    last_heartbeat: safeString(m.last_heartbeat || ''),
    used_by_community: used[rt7NormalizeMasterUid_(m.uid || m.master_uid || '')] || ''
  })).filter(m => m.uid);
  res.json({ ok:true, version:SERVER_VERSION, masters });
});

// ---------------- V5.7A Auth routes ----------------
app.get('/rt7_login', (req, res) => { res.set('Cache-Control','no-store'); res.type('html').send(rt7AuthPage_('login', req.query.msg || '', req.query.next || '')); });
app.get('/rt7_register', (req, res) => { res.set('Cache-Control','no-store'); res.type('html').send(rt7AuthPage_('register', req.query.msg || '')); });
app.post('/api/auth/login', (req, res) => {
  const username = safeString(req.body.username).trim();
  const password = safeString(req.body.password);
  const loginCommunity = safeString(req.body.community || req.body.community_name || '').trim().slice(0,80);
  const users = rt7ReadUsers_();
  const matches = users.filter(x => String(x.username).toLowerCase() === username.toLowerCase() && x.enabled !== false);
  let u = null;
  if (loginCommunity) {
    const ck = rt7CommunityKey_(loginCommunity);
    u = matches.find(x => rt7CommunityKey_(rt7CommunityName_(x)) === ck) || null;
  } else if (matches.length === 1) {
    u = matches[0];
  } else if (matches.length > 1) {
    return res.status(409).type('html').send(rt7AuthPage_('login', '同一帳號存在多個社區，請輸入社區名稱後登入。'));
  }
  if (!u || rt7HashPassword_(password, u.salt) !== u.password_hash) return res.status(401).type('html').send(rt7AuthPage_('login', '帳號、密碼或社區名稱錯誤'));
  // V5.7E1: 主頁登入閘門必須每次登入都檢查系統開通、主門禁 UID、設備綁定。
  // 未開通帳號不可建立 session 進入主頁，避免未綁定者直接觀看/操作門禁。
  if (!rt7UserSystemEnabled_(u)) {
    appendEvent({ type:'auth_login_blocked_not_activated', username:u.username, role:u.role, ip:clientIp(req) });
    return res.status(403).type('html').send(rt7AuthPage_('login', '帳號尚未開通或尚未綁定主門禁 UID，請聯絡 Railway 雲端管理平台或社區 admin 開通。'));
  }
  // V5.7E5: 登入前即時比對帳號綁定 UID 與 #1 主門禁 heartbeat 回報的真實 UID。
  // 只有 user.master_uid === real_master_uid，且平台曾成功比對 #1 真實 UID，才建立登入 session。
  const verify = rt7RealMasterVerifyForUser_(u);
  if (!verify.ok) {
    appendEvent({ type:'auth_login_blocked_master_uid_verify_failed', username:u.username, role:u.role, reason:verify.reason, user_master_uid:verify.user_master_uid, real_master_uid:verify.real_master_uid, bound_master_uid:verify.bound_master_uid, online:verify.online, ip:clientIp(req) });
    const msg = verify.reason === 'MASTER_OFFLINE'
      ? '無法登入：#1 主門禁尚未完成 UID 比對，請先到 Railway 雲端管理平台按「取得/比對 #1 UID」。'
      : '無法登入：帳號綁定 UID 與 #1 主門禁真實 UID 不一致，請聯絡 Railway 雲端管理平台重新綁定。';
    return res.status(403).type('html').send(rt7AuthPage_('login', msg));
  }
  rt7CreateSession_(req, res, u);
  appendEvent({ type:'auth_login', username:u.username, role:u.role, master_uid:u.master_uid, devices:u.devices, ip:clientIp(req) });
  const next = safeString(req.query.next || req.body.next || '/rt7_cloud_original_ui_doorbell');
  const safeNext = next.startsWith('/') ? next : '/rt7_cloud_original_ui_doorbell';
  if (safeNext === '/rt7_cloud_original_ui_doorbell' || safeNext.startsWith('/rt7_cloud_original_ui_doorbell?')) rt7SetMainGateCookie_(res);
  res.redirect(safeNext);
});
app.post('/api/auth/register', (req, res) => {
  const username = safeString(req.body.username).trim().replace(/[^a-zA-Z0-9_@.\-]/g, '').slice(0, 40);
  const password = safeString(req.body.password);
  const code = safeString(req.body.register_code).trim();
  const masterUidInput = rt7NormalizeMasterUid_(req.body.master_uid || '');
  const masterIpInput = safeString(req.body.master_ip || '').trim().replace(/^https?:\/\//i,'').replace(/\/.*$/,'').slice(0,80);
  const communityInput = safeString(req.body.community || '').trim().slice(0,80);
  const devicePair = safeString(req.body.device_pair || '#1').trim();
  if (!username || password.length < 4) return res.status(400).type('html').send(rt7AuthPage_('register', '帳號或密碼太短'));
  if (!communityInput) return res.status(400).type('html').send(rt7AuthPage_('register', '請輸入社區名稱'));
  const users = rt7ReadUsers_();
  const communityKey = rt7CommunityKey_(communityInput);
  if (users.some(u => String(u.username).toLowerCase() === username.toLowerCase() && rt7CommunityKey_(rt7CommunityName_(u)) === communityKey)) return res.status(409).type('html').send(rt7AuthPage_('register', '此社區帳號已存在'));
  // V5.8E2: A社區/B社區可各自建立 admin；帳號名稱只在同一社區內不可重複。
  const first = users.length === 0;
  const noAdmin = rt7CountAdmins_(users) === 0;
  const noAdminInCommunity = !users.some(u => rt7CommunityKey_(rt7CommunityName_(u)) === communityKey && String(u.role || 'user') === 'admin' && u.enabled !== false);
  const makeAdmin = first || noAdmin || noAdminInCommunity;
  const needCode = process.env.RT7_REGISTER_CODE || 'rt7';
  if (!makeAdmin && code !== needCode) return res.status(403).type('html').send(rt7AuthPage_('register', '註冊碼錯誤'));
  const masterReg = rt7ReadMasterRegistry_();
  const masterUid = masterUidInput || masterReg.master_uid || rt7DefaultMasterUid_();
  const usedBy = rt7MasterUidUsedByOtherCommunity_(masterUid, communityInput || ('社區_' + username));
  if (masterUid && usedBy) return res.status(409).type('html').send(rt7AuthPage_('register', '此主門禁 UID 已被其他社區綁定：' + usedBy));
  const onlineInfo = rt7GetMasterInfo_(masterUid);
  const userDevices = makeAdmin ? ['#1','#2','#3','#4'] : rt7NormalizeDeviceIds_(devicePair || '#1');
  const salt = crypto.randomBytes(16).toString('hex');
  const u = { id:rt7NewId_('u'), username, salt, password_hash:rt7HashPassword_(password, salt), role:makeAdmin?'admin':'user', enabled:true, system_enabled:!!masterUid && (!!onlineInfo || makeAdmin), community:(communityInput || ('社區_' + username)), master_uid:masterUid, master_ip:(masterIpInput || (onlineInfo && onlineInfo.ip || '')), devices:userDevices, created_at:nowIso(), ip:clientIp(req) };
  users.push(u); rt7SaveUsers_(users);
  if (makeAdmin) {
    masterReg.master_uid = masterUid;
    masterReg.owner = username;
    masterReg.devices = ['#1','#2','#3','#4'];
    if (masterIpInput) masterReg.registered_ip = masterIpInput;
    masterReg.bound_at = nowIso();
    rt7SaveMasterRegistry_(masterReg);
  }
  const currentUser = rt7GetSessionUser_(req);
  const currentIsAdmin = currentUser && (currentUser.role || 'user') === 'admin' && rt7UserSystemEnabled_(currentUser);
  appendEvent({ type:'auth_register', username:u.username, role:u.role, community:u.community, master_uid:u.master_uid, master_ip:u.master_ip, devices:u.devices, ip:clientIp(req), auto_admin:makeAdmin, created_by: currentUser && currentUser.username });
  // V5.7D7: 若 admin 在 /rt7_register 新增第二個帳號，不切換登入者，避免建立 user01 後失去 /rt7_user_manager 權限。
  if (currentIsAdmin && !makeAdmin) {
    return res.redirect('/rt7_user_manager?msg=' + encodeURIComponent('已新增帳號：' + u.username + '（預設 user，請在本頁改角色或開通/解除）'));
  }
  // V5.7E1: 註冊不再自動登入，也不直接進入主頁。
  // 註冊完成後必須回登入頁輸入帳密；登入時再檢查是否已開通、已綁定 UID 與設備。
  const msg = makeAdmin
    ? '註冊成功，已建立社區 admin 並綁定主門禁。請用 admin 帳密登入後進入主頁。'
    : '註冊成功，請等待 Railway 雲端管理平台或社區 admin 開通後再登入。';
  res.redirect('/rt7_login?msg=' + encodeURIComponent(msg));
});
app.get('/api/auth/logout', (req, res) => {
  const sid = rt7ParseCookies_(req)[AUTH_COOKIE];
  if (sid) { const sessions = rt7ReadSessions_(); delete sessions[sid]; rt7SaveSessions_(sessions); }
  rt7ClearLoginCookie_(res);
  res.redirect('/rt7_login?msg=' + encodeURIComponent('已登出'));
});
app.get('/api/auth/me', (req, res) => res.json({ ok:true, version:SERVER_VERSION, user:rt7PublicUser_(rt7GetSessionUser_(req)) }));
app.get('/api/auth/users', rt7RequireAdmin_, (req, res) => res.json({ ok:true, users:rt7ReadUsers_().map(rt7PublicUser_) }));
app.get('/api/rt7/master/status', (req, res) => {
  const master = rt7ReadMasterRegistry_();
  const user = rt7GetSessionUser_(req);
  const online = !!(master.last_master_heartbeat && (Date.now() - Date.parse(master.last_master_heartbeat)) < 120000);
  const verified = !!(master.real_master_uid && master.real_master_uid === master.master_uid && online);
  const userVerify = user ? rt7RealMasterVerifyForUser_(user) : null;
  res.json({ ok:true, version:SERVER_VERSION, master_uid:master.master_uid, owner:master.owner, devices:master.devices, real_master_uid:master.real_master_uid || '', master_uid_verified:verified, master_online:online, last_master_heartbeat:master.last_master_heartbeat || '', heartbeat_ip:master.heartbeat_ip || '', known_masters:rt7KnownMastersArray_(), login_uid_verify:userVerify, user:rt7PublicUser_(user) });
});

// V5.7E4: #1 主門禁 UID heartbeat / verify.
// ESP32 can call either GET or POST:
//   /api/rt7/master/heartbeat?master_uid=RT7-MASTER-XXXXXXXXXXXX&device_id=%231
// Railway records the real UID reported by #1 module and compares it with the bound UID.
function rt7HandleMasterHeartbeat_(req, res) {
  const q = Object.assign({}, req.query || {}, req.body || {});
  const uid = rt7NormalizeMasterUid_(q.master_uid || q.uid || q.masterUid || q.device_uid || '');
  const deviceId = safeString(q.device_id || q.device || '#1').trim() || '#1';
  if (!uid) return res.status(400).json({ ok:false, version:SERVER_VERSION, error:'MISSING_MASTER_UID' });
  const info = rt7UpdateMasterHeartbeat_(uid, { ip:q.ip || clientIp(req), request_ip:clientIp(req), device_id:deviceId, mac:q.mac || q.wifi_mac || q.wifiMac || '', source:'heartbeat' });
  const usedCommunity = rt7MasterUidUsedByOtherCommunity_(uid, '');
  try { registerOrUpdateDevice({ id:'#1', name:'RT7 ESP32-S3-CAM', ip:q.ip || clientIp(req), last_online:nowIso(), enabled:true }); } catch(_) {}
  appendEvent({ type:'master_heartbeat', master_uid:uid, device_id:deviceId, ip:clientIp(req), heartbeat_ip:info && info.ip || '', community:usedCommunity || '' });
  res.json({ ok:true, version:SERVER_VERSION, master_uid:uid, real_master_uid:uid, known_masters:rt7KnownMastersArray_().length, online:true, last_master_heartbeat:info && info.last_heartbeat || '' });
}
app.get('/api/rt7/master/heartbeat', rt7HandleMasterHeartbeat_);
app.post('/api/rt7/master/heartbeat', rt7HandleMasterHeartbeat_);
app.get('/api/rt7/master/verify', (req, res) => {
  const master = rt7ReadMasterRegistry_();
  const online = !!(master.last_master_heartbeat && (Date.now() - Date.parse(master.last_master_heartbeat)) < 120000);
  const verified = !!(master.real_master_uid && master.real_master_uid === master.master_uid && online);
  res.json({ ok:true, version:SERVER_VERSION, master_uid:master.master_uid, real_master_uid:master.real_master_uid || '', verified, online, last_master_heartbeat:master.last_master_heartbeat || '', heartbeat_ip:master.heartbeat_ip || '', known_masters:rt7KnownMastersArray_() });
});
app.get('/api/rt7/master/verify_user', rt7RequireLogin_, (req, res) => {
  const verify = rt7RealMasterVerifyForUser_(req.rt7User);
  res.json({ ok:true, version:SERVER_VERSION, username:req.rt7User.username, verify });
});
app.get('/api/rt7/platform/status', (req, res) => {
  const users = rt7ReadUsers_();
  const master = rt7ReadMasterRegistry_();
  res.json({ ok:true, version:SERVER_VERSION, platform:'online', communities:Array.from(new Set(users.map(rt7CommunityName_))).length, users:users.length, enabled_users:users.filter(u=>u.enabled!==false && u.system_enabled!==false).length, master });
});
app.get('/api/rt7/master_registry', (req, res) => {
  res.json({ ok:true, version:SERVER_VERSION, master:rt7ReadMasterRegistry_(), devices:readDevices(), users:rt7ReadUsers_().map(rt7PublicUser_) });
});

app.get('/api/rt7/master/devices', rt7RequireLogin_, (req, res) => {
  res.json({ ok:true, version:SERVER_VERSION, master:rt7ReadMasterRegistry_(), devices:rt7FilterDevicesForRequest_(req, readDevices()) });
});
app.post('/api/rt7/master/bind', rt7RequireAdmin_, (req, res) => {
  const master = rt7ReadMasterRegistry_();
  master.master_uid = rt7NormalizeMasterUid_(req.body.master_uid || req.query.master_uid || master.master_uid);
  master.owner = req.rt7User.username;
  master.devices = ['#1','#2','#3','#4'];
  rt7SaveMasterRegistry_(master);
  let users = rt7ReadUsers_();
  users = users.map(u => u.id === req.rt7User.id ? Object.assign({}, u, { master_uid:master.master_uid, devices:['#1','#2','#3','#4'] }) : u);
  rt7SaveUsers_(users);
  appendEvent({ type:'master_bind', master_uid:master.master_uid, owner:master.owner, ip:clientIp(req) });
  res.json({ ok:true, version:SERVER_VERSION, master });
});



app.get('/rt7_device_transfer_owner', rt7RequireAdmin_, (req, res) => res.type('html').send(rt7DeviceTransferOwnerPage_(req, req.query.msg || '')));
app.post('/api/rt7/master/transfer_owner', rt7RequireAdmin_, (req, res) => {
  const targetId = safeString(req.body.target_user_id || req.query.target_user_id).trim();
  const promoteAdmin = safeString(req.body.promote_admin || req.query.promote_admin) !== '0';
  const releaseOld = safeString(req.body.release_old || req.query.release_old) === '1';
  const current = req.rt7User;
  const master = rt7ReadMasterRegistry_();
  let users = rt7ReadUsers_();
  const target = users.find(u => u.id === targetId);
  if (!target) return res.redirect('/rt7_device_transfer_owner?msg=' + encodeURIComponent('找不到新 Owner 帳號'));
  const oldOwnerName = master.owner || (current && current.username) || '';
  target.enabled = true;
  target.system_enabled = true;
  target.master_uid = master.master_uid;
  target.devices = ['#1','#2','#3','#4'];
  if (promoteAdmin) target.role = 'admin';
  master.owner = target.username;
  master.devices = ['#1','#2','#3','#4'];
  master.transferred_at = nowIso();
  master.transferred_by = current && current.username;
  if (releaseOld && oldOwnerName && oldOwnerName !== target.username) {
    const old = users.find(u => u.username === oldOwnerName);
    if (old) {
      if ((old.role || 'user') === 'admin' && users.filter(x => x.id !== old.id && x.enabled !== false && x.system_enabled !== false && (x.role || 'user') === 'admin').length < 1) {
        return res.redirect('/rt7_device_transfer_owner?msg=' + encodeURIComponent('不能解除舊 Owner：會造成沒有已開通 admin'));
      }
      old.system_enabled = false;
      old.devices = [];
    }
  }
  rt7SaveUsers_(users);
  rt7SaveMasterRegistry_(master);
  appendEvent({ type:'master_transfer_owner', master_uid:master.master_uid, old_owner:oldOwnerName, new_owner:target.username, by:current && current.username, release_old:releaseOld, ip:clientIp(req) });
  res.redirect('/rt7_device_bind_status?msg=' + encodeURIComponent('已轉移主門禁 Owner：' + (oldOwnerName || '-') + ' → ' + target.username));
});

app.get('/rt7_device_bind_status', rt7RequireLogin_, (req, res) => res.type('html').send(rt7DeviceBindStatusPage_(req, req.query.msg || '')));
app.get('/rt7_user_manager', rt7RequireAdmin_, (req, res) => res.type('html').send(rt7UserManagerPage_(req, req.query.msg || '')));
app.post('/api/auth/users/delete', rt7RequireAdmin_, (req, res) => {
  const id = safeString(req.body.id).trim();
  const current = req.rt7User;
  let users = rt7ReadUsers_();
  const target = users.find(u => u.id === id);
  if (!target) return res.redirect('/rt7_user_manager?msg=' + encodeURIComponent('找不到帳號'));
  if (current && current.id === id) return res.redirect('/rt7_user_manager?msg=' + encodeURIComponent('不能刪除目前登入中的帳號'));
  if ((target.role || 'user') === 'admin' && rt7CountAdmins_(users) <= 1) return res.redirect('/rt7_user_manager?msg=' + encodeURIComponent('不能刪除最後一個 admin'));
  users = users.filter(u => u.id !== id);
  rt7SaveUsers_(users);
  rt7InvalidateUserSessions_(id);
  appendEvent({ type:'auth_user_delete', username:target.username, by:current && current.username, ip:clientIp(req) });
  res.redirect('/rt7_user_manager?msg=' + encodeURIComponent('已刪除帳號：' + target.username));
});
app.post('/api/auth/users/enabled', rt7RequireAdmin_, (req, res) => {
  const id = safeString(req.body.id).trim();
  const enabled = safeString(req.body.enabled) === '1';
  const current = req.rt7User;
  const users = rt7ReadUsers_();
  const target = users.find(u => u.id === id);
  if (!target) return res.redirect('/rt7_user_manager?msg=' + encodeURIComponent('找不到帳號'));
  if (current && current.id === id && !enabled) return res.redirect('/rt7_user_manager?msg=' + encodeURIComponent('不能停用目前登入中的帳號'));
  if ((target.role || 'user') === 'admin' && !enabled && rt7CountAdmins_(users) <= 1) return res.redirect('/rt7_user_manager?msg=' + encodeURIComponent('不能停用最後一個 admin'));
  target.enabled = enabled;
  rt7SaveUsers_(users);
  if (!enabled) rt7InvalidateUserSessions_(id);
  appendEvent({ type:'auth_user_enabled', username:target.username, enabled, by:current && current.username, ip:clientIp(req) });
  res.redirect('/rt7_user_manager?msg=' + encodeURIComponent((enabled?'已啟用：':'已停用：') + target.username));
});
app.post('/api/auth/users/system_enabled', rt7RequireAdmin_, (req, res) => {
  const id = safeString(req.body.id).trim();
  const systemEnabled = safeString(req.body.system_enabled) === '1';
  const current = req.rt7User;
  const master = rt7ReadMasterRegistry_();
  const users = rt7ReadUsers_();
  const target = users.find(u => u.id === id);
  if (!target) return res.redirect('/rt7_user_manager?msg=' + encodeURIComponent('找不到帳號'));
  if ((target.role || 'user') === 'admin' && !systemEnabled && rt7CountAdmins_(users) <= 1) return res.redirect('/rt7_user_manager?msg=' + encodeURIComponent('不能解除最後一個 admin'));
  target.system_enabled = systemEnabled;
  if (systemEnabled) {
    target.master_uid = rt7NormalizeMasterUid_(target.master_uid || master.master_uid || rt7DefaultMasterUid_());
    target.devices = (target.role || 'user') === 'admin' ? ['#1','#2','#3','#4'] : rt7NormalizeDeviceIds_(target.devices || '#1');
  } else {
    target.master_uid = '';
    target.devices = [];
  }
  rt7SaveUsers_(users);
  if (!systemEnabled) rt7InvalidateUserSessions_(id);
  appendEvent({ type:'auth_user_system_enabled', username:target.username, system_enabled:systemEnabled, master_uid:target.master_uid, devices:target.devices, by:current && current.username, ip:clientIp(req) });
  res.redirect('/rt7_user_manager?msg=' + encodeURIComponent((systemEnabled?'已開通系統：':'已解除系統：') + target.username));
});
app.post('/api/auth/users/role', rt7RequireAdmin_, (req, res) => {
  const id = safeString(req.body.id).trim();
  const role = safeString(req.body.role) === 'admin' ? 'admin' : 'user';
  const current = req.rt7User;
  const users = rt7ReadUsers_();
  const target = users.find(u => u.id === id);
  if (!target) return res.redirect('/rt7_user_manager?msg=' + encodeURIComponent('找不到帳號'));
  if ((target.role || 'user') === 'admin' && role !== 'admin' && rt7CountAdmins_(users) <= 1) return res.redirect('/rt7_user_manager?msg=' + encodeURIComponent('不能移除最後一個 admin 權限'));
  target.role = role;
  rt7SaveUsers_(users);
  appendEvent({ type:'auth_user_role', username:target.username, role, by:current && current.username, ip:clientIp(req) });
  res.redirect('/rt7_user_manager?msg=' + encodeURIComponent('已修改角色：' + target.username + ' → ' + role));
});
app.post('/api/auth/users/password', rt7RequireAdmin_, (req, res) => {
  const id = safeString(req.body.id).trim();
  const password = safeString(req.body.password);
  const current = req.rt7User;
  if (password.length < 4) return res.redirect('/rt7_user_manager?msg=' + encodeURIComponent('新密碼至少 4 碼'));
  const users = rt7ReadUsers_();
  const target = users.find(u => u.id === id);
  if (!target) return res.redirect('/rt7_user_manager?msg=' + encodeURIComponent('找不到帳號'));
  const salt = crypto.randomBytes(16).toString('hex');
  target.salt = salt;
  target.password_hash = rt7HashPassword_(password, salt);
  rt7SaveUsers_(users);
  rt7InvalidateUserSessions_(id);
  appendEvent({ type:'auth_user_password_reset', username:target.username, by:current && current.username, ip:clientIp(req) });
  res.redirect('/rt7_user_manager?msg=' + encodeURIComponent('已重設密碼：' + target.username));
});


// V5.8D: Return from push-enable page to main page without forcing a new login.
// The main page still requires the one-time gate cookie, so issue it only for a logged-in user.
app.get('/rt7_return_doorbell', (req, res) => {
  const user = rt7GetSessionUser_(req);
  if (!user) {
    return res.redirect('/rt7_login?next=' + encodeURIComponent('/rt7_cloud_original_ui_doorbell') + '&msg=' + encodeURIComponent('請先登入後進入主頁'));
  }
  // V5.8D1: fix 500 Internal Server Error.
  // Previous V5.8D called undefined helpers rt7UserSystemAccess_ / rt7VerifyUserMasterUid_.
  // Use the existing access and persistent UID verification helpers instead.
  if (!rt7UserSystemEnabled_(user)) {
    return res.redirect('/rt7_login?msg=' + encodeURIComponent('帳號尚未開通或尚未綁定設備，請聯絡管理員。'));
  }
  const verify = rt7RealMasterVerifyForUser_(user);
  if (!verify.ok) {
    return res.redirect('/rt7_login?msg=' + encodeURIComponent('主門禁 UID 尚未驗證，請聯絡管理員。'));
  }
  rt7SetMainGateCookie_(res);
  res.redirect('/rt7_cloud_original_ui_doorbell');
});

// Protect human-facing pages. ESP32 device APIs remain open for device compatibility.
app.use((req, res, next) => {
  const p = req.path || '';
  const protectedPages = [
    '/rt7_cloud_original_ui_doorbell','/rt7_cloud_admin','/rt7_device_manager','/rt7_face_db_manager','/rt7_gpio_control','/rt7_push_enable','/rt7_music_player','/rt7_cloud_doorbell_player','/rt7_snapshot_bridge_test','/rt7_cloud_phase10_no_nodered','/rt7_independent_full_video_intercom','/rt7_face_guard','/rt7_user_manager','/rt7_device_bind_status','/rt7_event_log','/rt7_log_viewer','/rt7_mapping','/rt7_stream_compare_test','/rt7_auto_stream_test'
  ];
  if (protectedPages.some(x => p === x || p.startsWith(x + '/'))) {
    res.set('Cache-Control','no-store');
    // V5.7E2: opening the main doorbell page must always pass through login.
    // A valid session alone is not enough for /rt7_cloud_original_ui_doorbell;
    // only the one-time cookie issued by a successful login can open it once.
    if (p === '/rt7_cloud_original_ui_doorbell') {
      if (!rt7HasMainGateCookie_(req)) {
        return res.redirect('/rt7_login?next=' + encodeURIComponent(req.originalUrl || '/rt7_cloud_original_ui_doorbell') + '&msg=' + encodeURIComponent('請先登入後進入主頁'));
      }
      rt7ClearMainGateCookie_(res);
    }
    return rt7RequireLogin_(req, res, next);
  }
  next();
});

app.get('/health', (req, res) => res.json({ ok: true, version: SERVER_VERSION, time: nowIso() }));

// V4.9A product system status: one endpoint for user support and maintenance.
app.get('/api/rt7/system/status', (req, res) => {
  let devices = [];
  try { devices = readDevices(); } catch (_) {}
  let snapshot = null;
  try { snapshot = getSnapshotMeta_ ? getSnapshotMeta_() : null; } catch (_) {}
  let events = [];
  try { events = readEvents(10); } catch (_) {}
  streamViewerPrune_ && streamViewerPrune_();
  res.json({
    ok: true,
    version: SERVER_VERSION,
    product: 'NO_NODERED_NO_TAILSCALE',
    cleanup: 'V5.0B_CLOUD_STREAM_KEEPALIVE_FIX',
    stable_base: 'V4.8F11/V4.9A',
    time: nowIso(),
    railway: { ok: true, port: process.env.PORT || 3000 },
    dependencies: { nodered: false, tailscale: false },
    devices,
    current_device: cloudState.current_device_id || '#1',
    ai_enabled: !!cloudState.ai_enabled,
    doorbell: doorbellState,
    stream: liveStreamState,
    snapshot,
    latest_events: events
  });
});

// ---------- Doorbell API: keep legacy Node-RED endpoint compatible ----------
function handleDoorbell(req, res, endpointName) {
  const body = req.body || {};
  doorbellState.count += 1;
  const dev = normalizeDevice(body, req);
  registerOrUpdateDevice(dev);
  const isEspNow = String(body.source || '').toLowerCase() === 'espnow' || !!body.espnow_code || String(body.type || '').toLowerCase() === 'espnow_event';
  const espnowCode = safeString(body.espnow_code || body.code || '');
  const last = {
    type: isEspNow ? 'espnow_event' : 'doorbell',
    endpoint: endpointName,
    device_id: dev.id,
    device_name: dev.name,
    ip: body.ip || dev.ip,
    source: isEspNow ? 'espnow' : (body.source || 'esp32_button'),
    espnow_code: espnowCode || undefined,
    count: body.count || doorbellState.count,
    message: body.message || (isEspNow ? ('ESP-NOW事件：控制碼 ' + (espnowCode || '-')) : '有人按門鈴'),
    time: nowIso()
  };
  doorbellState.last = last;
  const event = appendEvent(last);
  // Keep websocket type as doorbell for backward-compatible UI/audio, but event.type is espnow_event.
  broadcast('doorbell', event);
  const pushTitle = isEspNow ? '📡 ESP-NOW事件' : '🔔 有人按門鈴';
  const pushBody = isEspNow ? ('收到 ESP-NOW 控制碼：' + (espnowCode || '-')) : ('收到門鈴：' + safeString(last.message || '有人按門鈴'));
  rt7SendPushDoorbell_({ title: pushTitle, body: pushBody, url: '/rt7_cloud_original_ui_doorbell', device_id: dev.id, device_name: dev.name }).catch(e => console.warn('[RT7_PUSH][ERR]', String(e && e.message || e)));
  console.log(isEspNow ? '[RT7][ESPNOW_EVENT]' : '[RT7][DOORBELL]', JSON.stringify(event));
  res.json({ ok: true, message: isEspNow ? 'espnow event received' : 'doorbell received', state: doorbellState, event });
}

app.post('/api/rt7/phase9n/doorbell/event', (req, res) => handleDoorbell(req, res, 'legacy_phase9n'));
app.get('/api/rt7/phase9n/doorbell/event', (req, res) => handleDoorbell(req, res, 'legacy_phase9n_get'));
app.post('/api/rt7/doorbell/ring', (req, res) => handleDoorbell(req, res, 'legacy_ring'));
app.get('/api/rt7/doorbell/ring', (req, res) => handleDoorbell(req, res, 'legacy_ring_get'));
app.post('/api/rt7/doorbell', (req, res) => handleDoorbell(req, res, 'compat_rt7_doorbell'));
app.get('/api/rt7/doorbell', (req, res) => handleDoorbell(req, res, 'compat_rt7_doorbell_get'));
app.post('/api/rt7/doorbell/event', (req, res) => handleDoorbell(req, res, 'compat_rt7_event'));
app.get('/api/rt7/doorbell/event', (req, res) => handleDoorbell(req, res, 'compat_rt7_event_get'));
app.post('/api/doorbell', (req, res) => handleDoorbell(req, res, 'cloud_v3'));
app.get('/api/doorbell', (req, res) => handleDoorbell(req, res, 'cloud_v3_get'));
app.get('/api/rt7/doorbell/state', (req, res) => { res.set('Cache-Control','no-store, no-cache, must-revalidate, proxy-revalidate'); res.json({ ok: true, state: doorbellState }); });
app.get('/api/doorbell/state', (req, res) => { res.set('Cache-Control','no-store, no-cache, must-revalidate, proxy-revalidate'); res.json({ ok: true, state: doorbellState }); });
app.get('/api/rt7/espnow/state', (req, res) => {
  res.set('Cache-Control','no-store, no-cache, must-revalidate, proxy-revalidate');
  const latestEspnow = readEvents(50).find(e => e && (e.type === 'espnow_event' || e.source === 'espnow'));
  res.json({ ok: true, version: SERVER_VERSION, last: latestEspnow || null, state: doorbellState });
});

// ---------- Event Logger ----------
app.post('/api/events/log', (req, res) => {
  const event = appendEvent(Object.assign({ type: req.body?.type || 'event' }, req.body || {}, { ip: req.body?.ip || clientIp(req) }));
  broadcast('event', event);
  res.json({ ok: true, event });
});

app.get('/api/events/latest', (req, res) => {
  const limit = Math.min(parseInt(req.query.limit || '200', 10) || 200, 2000);
  res.json({ ok: true, events: readEvents(limit) });
});

app.get('/api/events/clear', (req, res) => {
  ensureDataDir();
  fs.writeFileSync(EVENT_LOG, '', 'utf8');
  appendEvent({ type: 'events_clear', message: 'event log cleared' });
  res.json({ ok: true, message: 'cleared' });
});

// ---------- Device Registry ----------
app.post('/api/device/register', (req, res) => {
  const dev = registerOrUpdateDevice(normalizeDevice(req.body || {}, req));
  const event = appendEvent({ type: 'device_register', device_id: dev.id, device_name: dev.name, ip: dev.ip, version: dev.version, message: 'device registered' });
  broadcast('device_register', event);
  res.json({ ok: true, device: dev, devices: readDevices() });
});

app.get('/api/devices', (req, res) => res.json({ ok: true, version:SERVER_VERSION, devices: rt7FilterDevicesForRequest_(req, readDevices()), master:rt7ReadMasterRegistry_(), file:'data/devices.json' }));
app.post('/api/devices/save', (req, res) => {
  const devices = saveDevices(req.body?.devices || req.body || []);
  const event = appendEvent({ type: 'devices_save', device_count: devices.length, message: 'devices saved' });
  broadcast('devices_save', event);
  res.json({ ok: true, devices });
});

// ---------- Test endpoint ----------
app.get('/api/test/doorbell', (req, res) => {
  req.body = { source: 'web_test', device_id: '#1', device_name: 'RT7 ESP32-S3-CAM', ip: req.query.ip || 'web' };
  handleDoorbell(req, res, 'web_test');
});

// ---------- Phone player page ----------
app.get('/rt7_cloud_doorbell_player', (req, res) => {
  res.type('html').send(htmlShell('RT7 Cloud Doorbell Player', `${baseCss}
<header class="top"><h1>RT7 Cloud Doorbell Player</h1><p>Railway → 手機提示音</p></header>
<div class="alert" id="doorbellAlert"><div class="alertTitle">🔔 有人按門鈴</div><div class="alertMsg" id="doorbellMsg">播放音樂中收到門鈴事件。</div><div class="alertBtns"><button class="white" onclick="goBack()">返回門禁</button><button class="dark" onclick="dismissDoorbell()">繼續播放</button></div></div>
<main class="wrap">
<section class="card" style="text-align:center"><div class="big">🔔</div><h2 id="banner">等待門鈴事件</h2><p>目前 count：<b id="count">0</b></p><p class="muted">最後事件：<span id="lastTime">-</span></p></section>
<section class="card"><button class="btn green" onclick="enableAudio()">啟用提示音</button><button class="btn" onclick="playBell()">測試提示音</button><button class="btn gray" onclick="poll(true)">立即讀取</button><button class="btn red" onclick="resetLocal()">本機重設顯示</button><p class="warn">手機瀏覽器通常要先按一次「啟用提示音」，後續門鈴事件才可自動播放。</p></section>
<section class="card"><h3>狀態 JSON</h3><pre id="json" class="status">loading...</pre></section>
</main>
<script>
let audioCtx=null, audioEnabled=false, lastSeenCount=null;
function $(id){return document.getElementById(id)}
async function enableAudio(){
  try{
    audioCtx = audioCtx || new (window.AudioContext||window.webkitAudioContext)();
    await audioCtx.resume();
    audioEnabled=true;
    playBell();
    $('banner').textContent='提示音已啟用，等待門鈴';
  }catch(e){alert('啟用提示音失敗：'+e.message)}
}
function tone(freq, delay, dur){
  if(!audioCtx) return;
  setTimeout(()=>{
    const o=audioCtx.createOscillator(); const g=audioCtx.createGain();
    o.frequency.value=freq; g.gain.value=0.22;
    o.connect(g); g.connect(audioCtx.destination);
    o.start(); setTimeout(()=>{try{o.stop()}catch(e){}}, dur);
  }, delay);
}
function playBell(){
  if(!audioCtx){return;}
  tone(880,0,180); tone(660,260,220);
}
function resetLocal(){lastSeenCount=null; $('banner').textContent='本機已重設，下一次事件會提示';}
async function poll(manual){
  try{
    const r=await fetch('/api/rt7/doorbell/state?_='+Date.now(),{cache:'no-store'});
    const j=await r.json();
    $('json').textContent=JSON.stringify(j,null,2);
    const st=j.state||{}; const c=st.count||0; const last=st.last||null;
    $('count').textContent=c;
    $('lastTime').textContent=last?(new Date(last.time).toLocaleString('zh-TW',{hour12:false})):'-';
    if(lastSeenCount===null){ lastSeenCount=c; if(manual && c>0) $('banner').textContent='🔔 有人按門鈴 #'+c; return; }
    if(c>lastSeenCount){
      lastSeenCount=c;
      $('banner').textContent='🔔 有人按門鈴 #'+c;
      if(audioEnabled) playBell();
    }
  }catch(e){$('json').textContent='ERROR '+e.message;}
}
setInterval(()=>poll(false),1000);
poll(false);
try{
  const wsProto=location.protocol==='https:'?'wss':'ws';
  const ws=new WebSocket(wsProto+'://'+location.host+'/ws');
  ws.onmessage=(ev)=>{try{const m=JSON.parse(ev.data); if(m.type==='doorbell') poll(true);}catch(e){}};
}catch(e){}
</script>`));
});


// ---------- V5.6N3 standalone Push enable page ----------
app.get('/rt7_push_enable', (req, res) => {
  res.type('html').send(htmlShell('RT7 啟用門鈴通知', `${baseCss}
<header class="top"><h1>RT7 門鈴背景通知</h1><p>請在手機上按一次啟用，允許通知權限。</p></header>
<main class="wrap">
<section class="card" style="text-align:center">
  <div class="big">🔔</div>
  <h2>啟用門鈴通知</h2>
  <p class="muted">此頁是獨立測試頁，避免主頁其它按鍵或影像區影響通知註冊。</p>
  <button id="rt7PushEnableNow" class="btn green" style="font-size:22px;min-height:64px;width:100%" onclick="return rt7StandalonePushClick(event)">🔔 立即啟用門鈴通知</button>
  <a class="btn gray" href="/rt7_return_doorbell">返回門禁主頁</a>
  <button class="btn" onclick="location.href='/api/push/test'">測試通知 /api/push/test</button>
  <pre id="rt7PushState" class="status" style="text-align:left;margin-top:12px">推播：等待按下啟用</pre>
</section>
<section class="card"><h3>測試順序</h3><p>1. 按「立即啟用門鈴通知」。<br>2. Android 跳出通知權限時選「允許」。<br>3. 顯示「推播：已啟用」後，打開 /api/push/test 測試。</p></section>
</main>
<script>
function rt7StandalonePushClick(ev){
  if(ev){ev.preventDefault();ev.stopPropagation();}
  var st=document.getElementById('rt7PushState');
  if(st) st.textContent='推播：獨立頁按鍵已觸發，正在啟用...';
  if(window.rt7EnablePwaPush){
    window.rt7EnablePwaPush().then(function(){ if(st) st.textContent='推播：已啟用，請測試 /api/push/test'; }).catch(function(e){ if(st) st.textContent='推播錯誤：'+(e.message||e); alert('啟用失敗：'+(e.message||e)); });
  } else {
    if(st) st.textContent='推播錯誤：rt7EnablePwaPush 未載入，請重新整理';
  }
  return false;
}
</script>`));
});

// ---------- Admin page ----------
app.get('/rt7_cloud_admin', (req, res) => {
  res.type('html').send(htmlShell('RT7 Cloud Admin V3', `${baseCss}
<header class="top"><h1>RT7 Cloud Admin V3</h1><p>Devices / Events / Doorbell</p></header>
<div class="alert" id="doorbellAlert"><div class="alertTitle">🔔 有人按門鈴</div><div class="alertMsg" id="doorbellMsg">播放音樂中收到門鈴事件。</div><div class="alertBtns"><button class="white" onclick="goBack()">返回門禁</button><button class="dark" onclick="dismissDoorbell()">繼續播放</button></div></div>
<main class="wrap">
<section class="card"><a class="btn" href="/rt7_cloud_doorbell_player">門鈴播放器</a><button class="btn gray" onclick="loadAll()">重新讀取</button><button class="btn red" onclick="clearEvents()">清除事件</button></section>
<section class="card"><h2>設備清單</h2><div id="devices">loading...</div></section>
<section class="card"><h2>事件紀錄</h2><div style="overflow:auto"><table><thead><tr><th>時間</th><th>設備</th><th>IP</th><th>事件</th><th>內容</th></tr></thead><tbody id="events"></tbody></table></div></section>
<section class="card"><h3>Status</h3><pre id="status" class="status">ready</pre></section>
</main>
<script>
function $(id){return document.getElementById(id)}
function esc(s){return String(s??'').replace(/[&<>\"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;'}[c]));}
async function j(url,opt){const r=await fetch(url,opt);return await r.json();}
async function loadAll(){
 const [d,e]=await Promise.all([j('/api/devices'),j('/api/events/latest?limit=200')]);
 $('devices').innerHTML=(d.devices||[]).map(x=>'<div class="card"><b>'+esc(x.id)+' '+esc(x.name)+'</b><br><code>'+esc(x.ip)+'</code><br><span class="muted">'+esc(x.version||'')+' '+esc(x.last_online||'')+'</span></div>').join('')||'no devices';
 $('events').innerHTML=(e.events||[]).reverse().map(x=>'<tr><td>'+esc(new Date(x.time||Date.now()).toLocaleString('zh-TW',{hour12:false}))+'</td><td>'+esc(x.device_id||'')+' '+esc(x.device_name||'')+'</td><td>'+esc(x.ip||'')+'</td><td>'+esc(x.type||'')+'</td><td>'+esc(x.message||JSON.stringify(x))+'</td></tr>').join('')||'<tr><td colspan="5">no events</td></tr>';
 $('status').textContent='devices='+(d.devices||[]).length+' events='+(e.events||[]).length;
}
async function clearEvents(){await j('/api/events/clear');loadAll();}
loadAll();
</script>`));
});




// ---------- V4.2 Snapshot Bridge test page ----------
app.get('/rt7_snapshot_bridge_test', (req, res) => {
  res.type('html').send(htmlShell('RT7 V4.2 Snapshot Bridge Test', `${baseCss}
<header class="top"><h1>RT7 V4.2 Snapshot Bridge</h1><p>只測 ESP32 → Railway Snapshot 上傳 / 手機讀取</p></header>
<div class="alert" id="doorbellAlert"><div class="alertTitle">🔔 有人按門鈴</div><div class="alertMsg" id="doorbellMsg">播放音樂中收到門鈴事件。</div><div class="alertBtns"><button class="white" onclick="goBack()">返回門禁</button><button class="dark" onclick="dismissDoorbell()">繼續播放</button></div></div>
<main class="wrap">
<section class="card"><h2>測試目標</h2><p>本頁只驗證 Snapshot Bridge，不測對講、不測 Face Match、不測 AI Vision。</p><div class="grid"><a class="btn green" href="/api/rt7/camera/state">Snapshot 狀態</a><a class="btn" href="/api/rt7/camera/latest.jpg" target="_blank">開啟最新 JPG</a><button class="btn gray" onclick="refreshState()">重新讀取</button><button class="btn red" onclick="clearSnapshot()">清除 Snapshot</button></div></section>
<section class="card"><h2>最新 Snapshot</h2><div style="background:#000;aspect-ratio:4/3;border-radius:12px;display:flex;align-items:center;justify-content:center;overflow:hidden"><img id="img" style="max-width:100%;max-height:100%;display:none"><div id="empty" style="color:#cbd5e1;font-weight:900;text-align:center">尚無照片<br><span class="muted">請 ESP32 POST /api/rt7/camera/snapshot</span></div></div><p class="muted">圖片 URL：<code>/api/rt7/camera/latest.jpg</code></p></section>
<section class="card"><h2>ESP32 上傳方式</h2><p>方式 A：直接 POST JPEG binary：</p><pre class="status">POST https://rt7-cloud-server-production.up.railway.app/api/rt7/camera/snapshot
Content-Type: image/jpeg
Body: JPEG bytes</pre><p>方式 B：POST base64 JSON：</p><pre class="status">POST /api/rt7/camera/snapshot_json
Content-Type: application/json
{"image_b64":"...","device_id":"#1"}</pre></section>
<section class="card"><h2>目前狀態</h2><pre id="log" class="status">loading...</pre></section>
</main>
<script>
const $=id=>document.getElementById(id);
async function j(url,opt){const r=await fetch(url+(url.includes('?')?'&':'?')+'_='+Date.now(),Object.assign({cache:'no-store'},opt||{}));const t=await r.text();try{return JSON.parse(t)}catch(e){return{ok:r.ok,status:r.status,raw:t}}}
async function refreshState(){const s=await j('/api/rt7/camera/state');$('log').textContent=JSON.stringify(s,null,2);if(s.latest_url){$('img').src=s.latest_url+'?_='+Date.now();$('img').style.display='block';$('empty').style.display='none';}else{$('img').style.display='none';$('empty').style.display='block';}}
async function clearSnapshot(){const s=await j('/api/rt7/camera/clear',{method:'POST'});$('log').textContent=JSON.stringify(s,null,2);refreshState();}
setInterval(refreshState,3000);refreshState();
try{const ws=new WebSocket((location.protocol==='https:'?'wss://':'ws://')+location.host+'/ws');ws.onmessage=e=>{try{const m=JSON.parse(e.data);if(m.type==='snapshot')refreshState();}catch(_){}}}catch(e){}
</script>`));
});



// ---------- V5.2A Railway-only Face Detection + Face Match (no GPT face recognition) ----------
const FACES_FILE = path.join(DATA_DIR, 'rt7_faces.json');
const FACE_DEBUG_SNAPSHOT_FILE = path.join(DATA_DIR, 'rt7_face_last_ai_snapshot.jpg');
function rt7ReadFaces_() {
  ensureDataDir();
  try {
    const raw = fs.existsSync(FACES_FILE) ? fs.readFileSync(FACES_FILE, 'utf8') : '[]';
    const arr = JSON.parse(raw || '[]');
    return Array.isArray(arr) ? arr : [];
  } catch (_) { return []; }
}
function rt7SaveFaces_(arr) {
  ensureDataDir();
  fs.writeFileSync(FACES_FILE, JSON.stringify(Array.isArray(arr) ? arr : [], null, 2), 'utf8');
}
function rt7LatestJpegB64_() {
  ensureDataDir();
  if (!fs.existsSync(SNAPSHOT_FILE)) return null;
  const buf = fs.readFileSync(SNAPSHOT_FILE);
  if (!buf || buf.length < 800) return null;
  return { b64: buf.toString('base64'), bytes: buf.length };
}
function rt7ParseFaceJson_(txt) {
  const raw = String(txt || '').trim();
  const m = raw.match(/\{[\s\S]*\}/);
  if (m) { try { return JSON.parse(m[0]); } catch (_) {} }
  return { ok:true, known_face:false, matched_name:'', confidence:0, summary:raw.slice(0,240) };
}


function rt7GetLatestWithMeta_() {
  const latest = rt7LatestJpegB64_();
  if (!latest) return null;
  const latestMeta = getSnapshotMeta_() || {};
  latest.snap_time = latestMeta.time || nowIso();
  latest.snap_source = latestMeta.source || 'latest.jpg';
  latest.snap_hash = rt7QuickHash_(latest.b64);
  latest.snap_age_ms = latestMeta.time ? Math.max(0, Date.now() - new Date(latestMeta.time).getTime()) : null;
  latest.meta = latestMeta;
  return latest;
}
function rt7SendWsJsonToEsp_(obj) {
  // V5.2B/V5.2C: make face snapshot command delivery robust.
  // Previous build sent only to clients already tagged as ESP. In field tests the ESP32
  // persistent WS was uploading frames/keepalive, but the tag was not always visible to
  // the face-match request path, so WS_SENT stayed 0.
  // Safe fix: send command JSON to all non-phone viewer clients and all ESP-like clients.
  let n = 0;
  let seen = 0;
  try {
    const text = JSON.stringify(obj);
    for (const ws of wss.clients) {
      if (!ws || ws.readyState !== WebSocket.OPEN) continue;
      seen++;
      const role = safeString(ws.rt7Role || '').toLowerCase();
      const pcmRole = safeString(ws.rt7PcmRole || '').toLowerCase();
      const isPhone = role.includes('phone') || role.includes('viewer');
      const isEsp = role.includes('esp') || pcmRole.includes('esp') || ws.rt7PcmClient === true || role === 'esp32_frame_upload' || role === 'control' || !role;
      if (isPhone && !isEsp) continue;
      try { ws.send(text); n++; } catch (_) {}
    }
    console.log('[FACE_API][V54O][WS_CMD_RELAY] type=' + safeString(obj && obj.type) + ' sent=' + n + ' open=' + seen);
  } catch (e) { console.warn('[FACE_API][V54O][WS_CMD_RELAY_ERR] ' + String(e && e.message || e)); }
  return n;
}
function rt7Sleep_(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
async function rt7ForceRealtimeSnapshot_() {
  const startMs = Date.now();
  const beforeMeta = getSnapshotMeta_() || {};
  const beforeTimeMs = beforeMeta.time ? new Date(beforeMeta.time).getTime() : 0;
  const requestId = 'face_snap_' + startMs + '_' + Math.floor(Math.random()*1000);
  // V5.2D: use the same cloud command polling path that already works for door/open commands.
  // Queue as wildcard device_id first, because field tests showed WS_SENT may be >0 but
  // ESP32 still receives no WS control message while streaming. The ESP32 polls
  // /api/rt7/device/commands/next, so this command must be visible there.
  // V5.2S: clear stale pending face snapshot commands before creating a new one.
  // This restores the V5.2N successful face path, but prevents stacked old face commands.
  try {
    if (Array.isArray(cloudState.command_queue)) {
      cloudState.command_queue = cloudState.command_queue.filter(c => !(c && c.status === 'pending' && (c.command === 'face_snapshot_now' || c.action === 'face_snapshot_now' || c.priority === 'face_snapshot')));
      saveState();
    }
    if (Array.isArray(pendingCommands)) {
      pendingCommands = pendingCommands.filter(c => !(c && (c.command === 'face_snapshot_now' || c.action === 'face_snapshot_now' || c.priority === 'face_snapshot')));
    }
  } catch (e) { console.warn('[FACE_API][V54O][CLEAR_PENDING_WARN] ' + String(e && e.message || e)); }

  const cmd = queueCommand({
    command:'face_snapshot_now', action:'face_snapshot_now', request_id:requestId,
    device_id:'rt7-esp32-s3-cam-01', requested_device_id:'#1', target_all:true, interval_ms:100,
    priority:'face_snapshot',
    message:'V54O single-shot face snapshot trigger; duplicate face_snapshot_now suppressed'
  });
  const wsSentA = rt7SendWsJsonToEsp_({ type:'face_snapshot_now', command:'face_snapshot_now', request_id:requestId, phase:'V54O', time:nowIso() });
  const wsSentB = rt7SendToEspIntercom_(JSON.stringify({ type:'face_snapshot_now', command:'face_snapshot_now', request_id:requestId, phase:'V54O', relay:'intercom_path', time:nowIso() }));
  const wsSent = wsSentA + wsSentB;
  broadcast('face_snapshot_request', { ok:true, version:SERVER_VERSION, request_id:requestId, command:cmd, ws_sent:wsSent, time:nowIso() });
  console.log('[FACE_API][V54O][SNAPSHOT_REQUEST] request_id=' + requestId + ' ws_sent=' + wsSent + ' before_time=' + (beforeMeta.time||'') + ' before_source=' + (beforeMeta.source||''));

  let latest = null;
  for (let i=0; i<90; i++) {
    await rt7Sleep_(100);
    latest = rt7GetLatestWithMeta_();
    if (!latest) continue;
    const tMs = latest.snap_time ? new Date(latest.snap_time).getTime() : 0;
    const freshByTime = tMs >= startMs - 250;
    const newerThanBefore = !beforeTimeMs || tMs > beforeTimeMs;
    const freshByAge = latest.snap_age_ms !== null && latest.snap_age_ms <= 1800;
    const isLiveFrame = latest.snap_source === 'ws_frame' || latest.snap_source === 'live_frame' || latest.snap_source === 'raw_post' || latest.snap_source === 'json_b64';
    // V5.2C: in field tests WS_SENT may be 0 because the ESP32 stream client is not command-addressable,
    // but the live stream is still uploading a fresh frame every ~100ms. Treat a very fresh live frame as
    // realtime Snapshot, otherwise face recognition waits forever and returns NO_REALTIME_SNAPSHOT.
    if (latest.bytes >= 800 && ((freshByTime && newerThanBefore) || (freshByAge && isLiveFrame))) {
      latest.snap_request_id = requestId;
      latest.snap_request_ws_sent = wsSent;
      latest.snap_wait_ms = Date.now() - startMs;
      latest.snap_forced_realtime = true;
      latest.snap_live_frame_fallback = !(freshByTime && newerThanBefore);
      latest.snap_source = (latest.snap_source === 'raw_post' || latest.snap_source === 'json_b64') ? 'realtime_snapshot' : (latest.snap_live_frame_fallback ? 'realtime_live_ws_frame' : 'realtime_ws_frame');
      console.log('[FACE_API][V54O][SNAPSHOT_FRESH_OR_LIVE] request_id=' + requestId + ' wait_ms=' + latest.snap_wait_ms + ' bytes=' + latest.bytes + ' hash=' + latest.snap_hash + ' age_ms=' + latest.snap_age_ms + ' source=' + latest.snap_source + ' ws_sent=' + wsSent + ' fallback=' + latest.snap_live_frame_fallback);
      return latest;
    }
  }
  // V5.1C: realtime-only. Do NOT fall back to stale ws_frame/latest.jpg.
  // If ESP32 does not provide a new frame after the request, return null so face match stops before AI.
  const stale = rt7GetLatestWithMeta_();
  console.warn('[FACE_API][V54O][SNAPSHOT_REALTIME_TIMEOUT] request_id=' + requestId + ' wait_ms=' + (Date.now() - startMs) + ' ws_sent=' + wsSent + ' last_hash=' + (stale && stale.snap_hash || '') + ' last_age_ms=' + (stale && stale.snap_age_ms || '') + ' last_source=' + (stale && stale.snap_source || ''));
  return {
    realtime_failed: true,
    snap_request_id: requestId,
    snap_request_ws_sent: wsSent,
    snap_wait_ms: Date.now() - startMs,
    snap_forced_realtime: true,
    snap_stale_warning: true,
    stale_meta: stale || null
  };
}

function rt7FaceGateCheck_(latest) {
  const bytes = Number(latest?.bytes || 0);
  const ageMs = latest?.time ? Math.max(0, Date.now() - new Date(latest.time).getTime()) : 0;
  // V50W: test-mode gate. This is a safe cloud-side precheck so the known-good V5.0S recognition path remains usable.
  // Real ESP32 FACE_GATE can be reintroduced after this toggle confirms ON/OFF routing.
  const pass = bytes >= 2500;
  const gate = {
    enabled: !!cloudState.face_gate_enabled,
    source: 'V51A_CLOUD_GATE_TEST',
    pass,
    bytes,
    age_ms: ageMs,
    reason: pass ? 'GATE_PASS_BYTES_OK' : 'GATE_SKIP_JPEG_TOO_SMALL',
    time: nowIso()
  };
  cloudState.last_face_gate = gate;
  console.log('[RT7_FACE_GATE][TOGGLE][V52D] enabled=' + gate.enabled + ' pass=' + gate.pass + ' bytes=' + gate.bytes + ' age_ms=' + gate.age_ms + ' reason=' + gate.reason);
  return gate;
}

function rt7QuickHash_(bufOrB64) {
  let buf = Buffer.isBuffer(bufOrB64) ? bufOrB64 : Buffer.from(String(bufOrB64 || ''), 'base64');
  let h = 2166136261 >>> 0;
  for (let i = 0; i < buf.length; i += Math.max(1, Math.floor(buf.length / 2048))) {
    h ^= buf[i]; h = Math.imul(h, 16777619) >>> 0;
  }
  return ('00000000' + h.toString(16).toUpperCase()).slice(-8);
}

function rt7SkinLike_(r, g, b) {
  const y  =  0.299*r + 0.587*g + 0.114*b;
  const cb = 128 - 0.168736*r - 0.331264*g + 0.5*b;
  const cr = 128 + 0.5*r - 0.418688*g - 0.081312*b;
  const maxc = Math.max(r,g,b), minc = Math.min(r,g,b);
  // YCbCr + simple color spread. This avoids gray wall/curtain being counted as face.
  return y > 45 && cr >= 133 && cr <= 185 && cb >= 75 && cb <= 145 && (maxc - minc) > 12 && r > b * 0.85 && r > g * 0.72;
}

function rt7RealFaceCountDetect_(latest) {
  const empty = {
    face_found:false, face_count:0, face_box:{x:0,y:0,w:0,h:0}, face_ratio:0,
    face_quality:'NO_FACE', face_position:'UNKNOWN', reason:'NO_FACE',
    summary:'目前畫面未偵測到人臉。', raw:'LOCAL_REAL_FACE_COUNT_V52D'
  };
  let img;
  try { img = jpeg.decode(Buffer.from(latest.b64, 'base64'), { useTArray:true, maxMemoryUsageInMB:80 }); }
  catch (e) { return { ...empty, face_quality:'DECODE_FAIL', reason:'JPEG_DECODE_FAIL', summary:'JPEG 解碼失敗：' + String(e && e.message || e).slice(0,80) }; }
  const W = img.width || 0, H = img.height || 0;
  if (!W || !H || !img.data) return { ...empty, reason:'JPEG_EMPTY', summary:'JPEG 無有效影像資料。' };

  const sw = 160, sh = Math.max(1, Math.round(H * sw / W));
  const mask = new Uint8Array(sw * sh);
  const lum = new Uint8Array(sw * sh);
  let skinTotal = 0;
  for (let yy=0; yy<sh; yy++) {
    const sy = Math.min(H-1, Math.floor(yy * H / sh));
    for (let xx=0; xx<sw; xx++) {
      const sx = Math.min(W-1, Math.floor(xx * W / sw));
      const p = (sy * W + sx) * 4;
      const r = img.data[p], g = img.data[p+1], b = img.data[p+2];
      const yv = Math.max(0, Math.min(255, Math.round(0.299*r + 0.587*g + 0.114*b)));
      lum[yy*sw+xx] = yv;
      if (rt7SkinLike_(r,g,b)) { mask[yy*sw+xx] = 1; skinTotal++; }
    }
  }
  if (skinTotal < sw*sh*0.012) return { ...empty, reason:'NO_SKIN_FACE_CANDIDATE', summary:'未偵測到足夠的人臉膚色區塊。' };

  const seen = new Uint8Array(sw * sh);
  const comps = [];
  const qx = new Int16Array(sw*sh), qy = new Int16Array(sw*sh);
  for (let y=0; y<sh; y++) for (let x=0; x<sw; x++) {
    const idx=y*sw+x;
    if (!mask[idx] || seen[idx]) continue;
    let head=0, tail=0, area=0, minx=x, maxx=x, miny=y, maxy=y;
    seen[idx]=1; qx[tail]=x; qy[tail]=y; tail++;
    while (head<tail) {
      const cx=qx[head], cy=qy[head]; head++; area++;
      if (cx<minx) minx=cx; if (cx>maxx) maxx=cx; if (cy<miny) miny=cy; if (cy>maxy) maxy=cy;
      const nbs=[[cx+1,cy],[cx-1,cy],[cx,cy+1],[cx,cy-1]];
      for (const [nx,ny] of nbs) {
        if (nx<0||ny<0||nx>=sw||ny>=sh) continue;
        const ni=ny*sw+nx;
        if (mask[ni] && !seen[ni]) { seen[ni]=1; qx[tail]=nx; qy[tail]=ny; tail++; }
      }
    }
    if (area>20) comps.push({area,minx,maxx,miny,maxy,w:maxx-minx+1,h:maxy-miny+1});
  }
  comps.sort((a,b)=>b.area-a.area);

  function darkFeatureCount(c) {
    let sum=0, n=0;
    for (let y=c.miny; y<=c.maxy; y++) for (let x=c.minx; x<=c.maxx; x++) { sum += lum[y*sw+x]; n++; }
    const avg = n ? sum/n : 128;
    const dark = new Uint8Array(sw*sh);
    const y0 = c.miny + Math.floor(c.h*0.18), y1 = c.miny + Math.floor(c.h*0.72);
    const x0 = c.minx + Math.floor(c.w*0.12), x1 = c.maxx - Math.floor(c.w*0.12);
    let dtotal=0;
    for (let y=y0; y<=y1; y++) for (let x=x0; x<=x1; x++) {
      const idx=y*sw+x;
      if (lum[idx] < avg - 18) { dark[idx]=1; dtotal++; }
    }
    if (dtotal < Math.max(8, c.area*0.015)) return {count:0,dark_ratio:0};
    const seenD = new Uint8Array(sw*sh); let cnt=0;
    const qx2 = new Int16Array(sw*sh), qy2 = new Int16Array(sw*sh);
    for (let y=y0; y<=y1; y++) for (let x=x0; x<=x1; x++) {
      const idx=y*sw+x; if (!dark[idx] || seenD[idx]) continue;
      let head=0, tail=0, area=0, minx=x,maxx=x,miny=y,maxy=y;
      seenD[idx]=1; qx2[tail]=x; qy2[tail]=y; tail++;
      while(head<tail){ const cx=qx2[head],cy=qy2[head]; head++; area++; if(cx<minx)minx=cx;if(cx>maxx)maxx=cx;if(cy<miny)miny=cy;if(cy>maxy)maxy=cy;
        for (const [nx,ny] of [[cx+1,cy],[cx-1,cy],[cx,cy+1],[cx,cy-1]]) { if(nx<0||ny<0||nx>=sw||ny>=sh)continue; const ni=ny*sw+nx; if(dark[ni]&&!seenD[ni]){seenD[ni]=1;qx2[tail]=nx;qy2[tail]=ny;tail++;} }
      }
      const ww=maxx-minx+1, hh=maxy-miny+1;
      if (area>=3 && area<=c.area*0.20 && ww<=c.w*0.55 && hh<=c.h*0.45) cnt++;
    }
    return {count:cnt, dark_ratio: Math.round(dtotal * 1000 / Math.max(1,c.area))/10};
  }

  const candidates=[];
  for (const c of comps.slice(0,8)) {
    const ratioArea = c.area / (sw*sh);
    const asp = c.w / Math.max(1,c.h);
    const fill = c.area / Math.max(1, c.w*c.h);
    if (ratioArea < 0.018 || ratioArea > 0.70) continue;
    if (c.w < 18 || c.h < 18) continue;
    if (asp < 0.42 || asp > 1.45) continue;
    if (fill < 0.22) continue;
    const feat = darkFeatureCount(c);
    // Real face count rule: require skin blob AND internal dark facial features.
    if (feat.count < 2 && ratioArea < 0.22) continue;
    candidates.push({...c, ratioArea, asp, fill, features:feat.count, dark_ratio:feat.dark_ratio});
  }
  if (!candidates.length) {
    const c = comps[0];
    return { ...empty, reason:'NO_FACE_DETECTED', summary:'未偵測到符合人臉形狀與五官特徵的區塊。', local_skin_total:skinTotal, largest_skin_box:c?{x:c.minx,y:c.miny,w:c.w,h:c.h}:null };
  }

  const best = candidates[0];
  const scaleX = W / sw, scaleY = H / sh;
  const box = { x:Math.round(best.minx*scaleX), y:Math.round(best.miny*scaleY), w:Math.round(best.w*scaleX), h:Math.round(best.h*scaleY) };
  const faceRatio = Math.round((box.w * box.h) * 100 / Math.max(1, W*H));
  const cx = best.minx + best.w/2, cy = best.miny + best.h/2;
  let pos = 'CENTER';
  if (cx < sw*0.30) pos='LEFT'; else if (cx > sw*0.70) pos='RIGHT';
  if (cy < sh*0.25) pos = pos==='CENTER' ? 'TOP' : 'CORNER'; else if (cy > sh*0.78) pos = pos==='CENTER' ? 'BOTTOM' : 'CORNER';
  let quality = 'OK';
  if (faceRatio >= 18 && best.features >= 2) quality='GOOD';
  if (faceRatio < 7 || best.features < 2) quality='LOW';
  return {
    face_found:true,
    face_count:candidates.length,
    face_box:box,
    face_ratio:faceRatio,
    face_quality:quality,
    face_position:pos,
    reason:'FACE_OK',
    summary:'本機影像偵測到 ' + candidates.length + ' 個人臉候選區塊。',
    raw:'LOCAL_REAL_FACE_COUNT_V52D features=' + best.features + ' skin=' + skinTotal + ' blob=' + JSON.stringify({w:best.w,h:best.h,area:best.area,fill:best.fill,dark_ratio:best.dark_ratio}),
    local_debug:{ width:W, height:H, skin_total:skinTotal, candidates:candidates.slice(0,3).map(c=>({w:c.w,h:c.h,area:c.area,features:c.features,dark_ratio:c.dark_ratio,fill:Number(c.fill.toFixed(2))})) }
  };
}

async function rt7DetectFaceOnly_(latest) {
  // V5.0Z: real local face-count gate first. It does not see registered photos, so an empty room cannot match gwansyan.
  return rt7RealFaceCountDetect_(latest);
}


function rt7Clamp_(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }
function rt7DecodeJpeg_(b64) {
  return jpeg.decode(Buffer.from(String(b64 || ''), 'base64'), { useTArray:true, maxMemoryUsageInMB:80 });
}
function rt7ExtractFaceEmbedding_(b64, detectOpt) {
  let img;
  try { img = rt7DecodeJpeg_(b64); }
  catch (e) { return { ok:false, reason:'DECODE_FAIL', error:String(e && e.message || e) }; }
  const W = img.width || 0, H = img.height || 0;
  if (!W || !H || !img.data) return { ok:false, reason:'JPEG_EMPTY' };

  let detect = detectOpt || null;
  if (!detect || !detect.face_found) {
    detect = rt7RealFaceCountDetect_({ b64 });
  }

  let box = detect && detect.face_found && detect.face_box ? detect.face_box : null;
  if (!box || !box.w || !box.h) {
    // Enrollment fallback: use center crop so old registered photos still remain usable.
    const sideW = Math.round(W * 0.58), sideH = Math.round(H * 0.72);
    box = { x:Math.round((W-sideW)/2), y:Math.round((H-sideH)/2), w:sideW, h:sideH };
  }

  const x0 = rt7Clamp_(Math.round(box.x), 0, W-1);
  const y0 = rt7Clamp_(Math.round(box.y), 0, H-1);
  const bw = rt7Clamp_(Math.round(box.w), 8, W-x0);
  const bh = rt7Clamp_(Math.round(box.h), 8, H-y0);

  // 16x16 luma embedding + 4x4 color bins. Pure JS, deterministic, Railway-side only.
  const grid = 16;
  const luma = [];
  const crcb = [];
  let mean = 0;
  for (let gy=0; gy<grid; gy++) {
    for (let gx=0; gx<grid; gx++) {
      let rs=0, gs=0, bs=0, n=0;
      const sx0 = x0 + Math.floor(gx * bw / grid);
      const sx1 = x0 + Math.floor((gx+1) * bw / grid);
      const sy0 = y0 + Math.floor(gy * bh / grid);
      const sy1 = y0 + Math.floor((gy+1) * bh / grid);
      for (let yy=sy0; yy<Math.max(sy0+1,sy1); yy++) {
        for (let xx=sx0; xx<Math.max(sx0+1,sx1); xx++) {
          const i=(rt7Clamp_(yy,0,H-1)*W + rt7Clamp_(xx,0,W-1))*4;
          rs += img.data[i]; gs += img.data[i+1]; bs += img.data[i+2]; n++;
        }
      }
      const r=rs/Math.max(1,n), g=gs/Math.max(1,n), b=bs/Math.max(1,n);
      const y=0.299*r+0.587*g+0.114*b;
      const cb=128 - 0.168736*r - 0.331264*g + 0.5*b;
      const cr=128 + 0.5*r - 0.418688*g - 0.081312*b;
      luma.push(y); crcb.push(cr); crcb.push(cb); mean += y;
    }
  }
  mean /= luma.length;
  let variance=0;
  for (const v of luma) variance += (v-mean)*(v-mean);
  const std = Math.sqrt(variance / Math.max(1,luma.length)) || 1;
  const vec = [];
  for (const v of luma) vec.push((v-mean)/std);
  // Edge structure helps distinguish empty wall from face-like color patches.
  for (let y=1; y<grid-1; y++) {
    for (let x=1; x<grid-1; x++) {
      const i=y*grid+x;
      const dx=(luma[i+1]-luma[i-1])/255;
      const dy=(luma[i+grid]-luma[i-grid])/255;
      vec.push(dx); vec.push(dy);
    }
  }
  // Low-weight color signature.
  let crm=0, cbm=0;
  for (let i=0; i<crcb.length; i+=2) { crm += crcb[i]; cbm += crcb[i+1]; }
  crm /= (crcb.length/2); cbm /= (crcb.length/2);
  for (let i=0; i<crcb.length; i+=2) { vec.push((crcb[i]-crm)/80); vec.push((crcb[i+1]-cbm)/80); }

  return { ok:true, vector:vec, box, detect, width:W, height:H, embedding_len:vec.length, reason:'EMBED_OK' };
}

// ---------- V5.3A Face Fast Cache Match ----------
// Store Railway-local face embedding in rt7_faces.json at enrollment time.
// Matching can then reuse cached reference vectors instead of decoding all enrolled JPEGs every request.
// This does not use GPT/OpenAI; it is deterministic Railway-side face detect + embedding compare.
function rt7FaceEmbeddingCacheKey_(face) {
  return safeString((face && face.id) || '') + ':' + safeString((face && face.time) || '') + ':' + Number((face && face.bytes) || 0);
}
function rt7FaceEmbeddingToCache_(emb) {
  if (!emb || !emb.ok || !Array.isArray(emb.vector) || !emb.vector.length) return null;
  // Round to 4 decimals to keep rt7_faces.json compact while preserving similarity behavior.
  return {
    ok:true,
    model:'rt7_js_luma_edge_v1',
    vector:emb.vector.map(v => Number(Number(v || 0).toFixed(4))),
    box:emb.box || null,
    width:emb.width || 0,
    height:emb.height || 0,
    embedding_len:emb.embedding_len || emb.vector.length,
    reason:emb.reason || 'EMBED_OK',
    cache_time:nowIso()
  };
}
function rt7GetCachedRefEmbedding_(face) {
  if (!face) return { ok:false, reason:'NO_FACE_ROW' };
  const c = face.embedding_cache;
  if (c && c.ok && c.model === 'rt7_js_luma_edge_v1' && Array.isArray(c.vector) && c.vector.length > 64) {
    return { ok:true, vector:c.vector, box:c.box || null, width:c.width||0, height:c.height||0, embedding_len:c.embedding_len||c.vector.length, reason:'CACHE_HIT', cache_hit:true };
  }
  const emb = rt7ExtractFaceEmbedding_(face.image_b64, null);
  if (!emb.ok) return emb;
  try {
    face.embedding_cache = rt7FaceEmbeddingToCache_(emb);
    face.embedding_cache_key = rt7FaceEmbeddingCacheKey_(face);
  } catch(_) {}
  emb.cache_hit = false;
  return emb;
}
function rt7RefreshFaceEmbeddingCaches_(faces) {
  let changed = false;
  for (const f of faces || []) {
    if (!f || !f.image_b64) continue;
    if (f.embedding_cache && f.embedding_cache.ok && Array.isArray(f.embedding_cache.vector)) continue;
    const emb = rt7ExtractFaceEmbedding_(f.image_b64, null);
    if (emb.ok) {
      f.embedding_cache = rt7FaceEmbeddingToCache_(emb);
      f.embedding_cache_key = rt7FaceEmbeddingCacheKey_(f);
      changed = true;
    }
  }
  return changed;
}
function rt7Cosine_(a,b) {
  const n=Math.min(a.length,b.length); let dot=0, na=0, nb=0;
  for (let i=0;i<n;i++){ const x=Number(a[i]||0), y=Number(b[i]||0); dot += x*y; na += x*x; nb += y*y; }
  if (!na || !nb) return 0;
  return dot / Math.sqrt(na*nb);
}
function rt7RailwayFaceCompare_(latest, refs, detect) {
  const t0 = Date.now();
  const cur = rt7ExtractFaceEmbedding_(latest.b64, detect);
  if (!cur.ok) return { known_face:false, matched_name:'', confidence:0, reason:cur.reason || 'CURRENT_EMBED_FAIL', summary:'目前照片無法建立人臉特徵。', raw:'RAILWAY_FACE_MATCH_V54O', match_ms:Date.now()-t0, cache_mode:'FAST_CACHE' };
  let best = null;
  let cacheHits = 0, cacheMiss = 0, compared = 0;
  for (const f of refs || []) {
    const ref = rt7GetCachedRefEmbedding_(f);
    if (!ref.ok) continue;
    if (ref.cache_hit) cacheHits++; else cacheMiss++;
    compared++;
    const cos = rt7Cosine_(cur.vector, ref.vector);
    // map cosine to 0-100. This is intentionally conservative for door access.
    const conf = Math.round(rt7Clamp_(((cos + 1) / 2) * 100, 0, 100));
    const row = { name:safeString(f.name||''), confidence:conf, cosine:Number(cos.toFixed(4)), ref_box:ref.box, ref_reason:ref.reason, cache_hit:!!ref.cache_hit };
    if (!best || row.confidence > best.confidence) best = row;
  }
  const matchMs = Date.now() - t0;
  if (!best) return { known_face:false, matched_name:'', confidence:0, reason:'NO_VALID_REFERENCE_EMBEDDING', summary:'註冊照片無法建立可比對的人臉特徵。', raw:'RAILWAY_FACE_MATCH_V54O', match_ms:matchMs, cache_mode:'FAST_CACHE', cache_hits:cacheHits, cache_miss:cacheMiss, compared };
  // V5.4O threshold tuning: field tests showed GOOD+CENTER faces scoring 37-41%.
  // Use 40% as a practical single-user doorbell threshold while ESP32 FACE_GATE still filters candidates.
  const RT7_FACE_MATCH_THRESHOLD = 40;
  const pass = best.confidence >= RT7_FACE_MATCH_THRESHOLD;
  return {
    known_face: pass,
    matched_name: pass ? best.name : '',
    confidence: best.confidence,
    reason: pass ? 'FACE_OK_RAILWAY_MATCH' : 'LOW_SIMILARITY_RAILWAY',
    summary: pass ? ('Railway 快取比對通過：' + best.name + ' / ' + best.confidence + '% / threshold=40%') : ('Railway 已偵測到人臉，但與註冊名單相似度不足：' + best.confidence + '% / threshold=40%'),
    raw:'RAILWAY_FACE_MATCH_V54O cosine=' + best.cosine + ' name=' + best.name + ' match_ms=' + matchMs + ' cache_hits=' + cacheHits + ' cache_miss=' + cacheMiss,
    best,
    match_ms:matchMs,
    cache_mode:'FAST_CACHE',
    cache_hits:cacheHits,
    cache_miss:cacheMiss,
    compared
  };
}



// V5.6I: Vision liveness check after Railway Face Match PASS.
// Goal: block phone/tablet/computer screen photos and printed photos before reporting known_face=true.
async function rt7VisionLivenessCheck_(latest, detect) {
  const t0 = Date.now();
  const outBase = {
    ok:false,
    enabled:true,
    engine:'openai_vision',
    model:safeString(process.env.OPENAI_CHAT_MODEL || 'gpt-4o-mini').trim(),
    live_face:false,
    verdict:'UNKNOWN',
    confidence:0,
    reason:'INIT',
    summary:'Vision 活體檢測尚未完成。',
    liveness_ms:0
  };
  try {
    if (!latest || !latest.b64) return Object.assign(outBase, { reason:'NO_SNAPSHOT', summary:'Vision 活體檢測失敗：沒有 Snapshot。', liveness_ms:Date.now()-t0 });
    const prompt = [
      '你是 RT7 門禁系統的活體檢測器。請只判斷這張門口攝影機照片中的臉是否是真人站在鏡頭前。',
      '請特別檢查是否為：手機螢幕、平板螢幕、電腦螢幕、紙本照片、照片翻拍、海報、人臉圖片。',
      '判斷規則：只有明顯是真人且不是螢幕/紙本照片，才算 REAL。只要疑似螢幕或照片，判為 SCREEN 或 PHOTO。',
      '請只輸出 JSON，不要 Markdown：',
      '{"verdict":"REAL|SCREEN|PHOTO|UNCLEAR","live_face":true|false,"confidence":0-100,"reason":"繁體中文一句話"}'
    ].join('\n');
    const raw = await openAiChat([{ role:'user', content:[
      { type:'text', text:prompt },
      { type:'image_url', image_url:{ url:'data:image/jpeg;base64,' + latest.b64 } }
    ]}], 180);
    let text = safeString(raw).trim();
    text = text.replace(/^```json\s*/i,'').replace(/^```\s*/,'').replace(/```$/,'').trim();
    let obj = null;
    try { obj = JSON.parse(text); }
    catch (_) {
      const m = text.match(/\{[\s\S]*\}/);
      if (m) { try { obj = JSON.parse(m[0]); } catch(e) {} }
    }
    const verdict = safeString(obj && obj.verdict || '').toUpperCase();
    const conf = Math.max(0, Math.min(100, Number(obj && obj.confidence || 0)));
    const live = (verdict === 'REAL') && (obj && obj.live_face === true || /true/i.test(safeString(obj && obj.live_face))) && conf >= 60;
    return {
      ok:true,
      enabled:true,
      engine:'openai_vision',
      model:safeString(process.env.OPENAI_CHAT_MODEL || 'gpt-4o-mini').trim(),
      live_face:!!live,
      verdict:verdict || 'UNCLEAR',
      confidence:conf,
      reason:safeString(obj && obj.reason || text).slice(0,240),
      summary: live ? ('Vision 活體檢測通過：REAL / ' + conf + '%') : ('Vision 活體檢測未通過：' + (verdict || 'UNCLEAR') + ' / ' + conf + '%'),
      raw:text.slice(0,500),
      face_box: detect && detect.face_box || null,
      liveness_ms:Date.now()-t0
    };
  } catch(e) {
    return Object.assign(outBase, {
      ok:false,
      live_face:false,
      verdict:'ERROR',
      confidence:0,
      reason:String(e && e.message || e).slice(0,240),
      summary:'Vision 活體檢測失敗，已阻擋開門：' + String(e && e.message || e).slice(0,120),
      liveness_ms:Date.now()-t0
    });
  }
}

async function rt7MatchKnownFaceOnly_(latest, refs, detect) {
  // V5.3A: Railway-only fast cached face match. No GPT/OpenAI call is used for door face recognition.
  return rt7RailwayFaceCompare_(latest, refs, detect);
}

async function rt7FaceMatchLatestCore_(providedLatest, opt) {
  opt = opt || {};
  const autoMode = !!opt.auto_face_gate;
  console.log('[FACE_API][V54O] /api/rt7/face/match CORE ENTER detect_first=1 force_realtime=' + (providedLatest ? '0' : '1') + ' auto_face_gate=' + (autoMode ? '1' : '0'));
  const latest = providedLatest || await rt7ForceRealtimeSnapshot_();
  if (!latest || latest.realtime_failed || !latest.b64) {
    const stale = latest && latest.stale_meta || null;
    const fail = {
      ok:true, version:SERVER_VERSION, api_entered:true, api_path:'/api/rt7/face/match', type:'face_match', stage:'REALTIME_SNAPSHOT_REQUIRED', engine:'railway_local', gpt_used:false,
      known_face:false, matched_name:'', confidence:0, face_found:false, face_count:0, face_box:{x:0,y:0,w:0,h:0}, face_ratio:0,
      face_quality:'NO_REALTIME_SNAPSHOT', face_position:'UNKNOWN', fail_stage:'SNAPSHOT', reason:'NO_REALTIME_SNAPSHOT',
      summary:'未取得即時 Snapshot，已停止：不使用舊 ws_frame，也不做人臉比對。請確認 ESP32 有持續上傳最新影像。',
      snap_time: stale && stale.snap_time || '', snap_source: stale && stale.snap_source || 'none', snap_hash: stale && stale.snap_hash || '', snap_age_ms: stale && stale.snap_age_ms || null,
      latest_bytes: stale && stale.bytes || 0, snap_wait_ms: latest && latest.snap_wait_ms || 0, snap_forced_realtime:true, snap_stale_warning:true, snap_request_ws_sent: latest && latest.snap_request_ws_sent || 0, snap_live_frame_fallback: latest && !!latest.snap_live_frame_fallback,
      debug_text:'REALTIME_ONLY=YES NO_REALTIME_SNAPSHOT STALE_HASH=' + (stale && stale.snap_hash || '') + ' AGE=' + (stale && stale.snap_age_ms || ''),
      time:nowIso()
    };
    cloudState.last_face_match = fail; broadcast('face_match', fail);
    console.warn('[FACE_API][V54O] stop before AI: NO_REALTIME_SNAPSHOT ws_sent=' + fail.snap_request_ws_sent + ' wait_ms=' + fail.snap_wait_ms + ' stale_hash=' + fail.snap_hash + ' stale_age_ms=' + fail.snap_age_ms);
    return fail;
  }
  try { fs.writeFileSync(FACE_DEBUG_SNAPSHOT_FILE, Buffer.from(latest.b64, 'base64')); } catch (e) { console.warn('[FACE_API][V54O] save face debug snapshot failed', e && e.message || e); }

  const gate = rt7FaceGateCheck_(latest);
  if (cloudState.face_gate_enabled && !gate.pass) {
    const skip = { ok:true, version:SERVER_VERSION, api_entered:true, type:'face_match', known_face:false, face_found:false, face_count:0, face_box:{x:0,y:0,w:0,h:0}, face_ratio:0, confidence:0, face_quality:'SKIP', reason:gate.reason, fail_stage:'FACE_GATE', face_gate:gate, snap_time:latest.snap_time, snap_hash:latest.snap_hash, snap_age_ms:latest.snap_age_ms, latest_bytes:latest.bytes, snap_wait_ms:latest.snap_wait_ms, snap_forced_realtime:latest.snap_forced_realtime, snap_stale_warning:!!latest.snap_stale_warning, snap_request_ws_sent:latest.snap_request_ws_sent, snap_live_frame_fallback:!!latest.snap_live_frame_fallback, face_snapshot_url:'/api/rt7/face/last_snapshot.jpg?h='+latest.snap_hash, summary:'FACE_GATE 測試模式阻擋，未做 Railway 比對。' };
    cloudState.last_face_match = skip; broadcast('face_match', skip);
    console.log('[FACE_API][V54O] FACE_GATE_SKIP hash=' + latest.snap_hash + ' reason=' + gate.reason);
    return skip;
  }

  const detect = await rt7DetectFaceOnly_(latest);
  console.log('[FACE_API][V54O] detect face_found=' + detect.face_found + ' count=' + detect.face_count + ' box=' + JSON.stringify(detect.face_box) + ' ratio=' + detect.face_ratio + ' reason=' + detect.reason + ' hash=' + latest.snap_hash);
  if (!detect.face_found || detect.face_count <= 0) {
    const noface = {
      ok:true, version:SERVER_VERSION, api_entered:true, api_path:'/api/rt7/face/match', type:'face_match', stage:'DETECT_ONLY', engine:'railway_local', gpt_used:false,
      face_gate:gate, face_found:false, face_count:0, face_box:detect.face_box, face_ratio:detect.face_ratio,
      known_face:false, matched_name:'', confidence:0, backlight_tolerant:true, pass_threshold:40,
      face_quality:detect.face_quality, face_position:detect.face_position, fail_stage:'DETECT', reason:detect.reason || 'NO_FACE',
      summary:detect.summary || '即時 Snapshot 未偵測到清楚人臉，已直接結束，未做人臉比對。', count:rt7ReadFaces_().length,
      latest_bytes:latest.bytes, snap_time:latest.snap_time, snap_source:latest.snap_source, snap_hash:latest.snap_hash, snap_age_ms:latest.snap_age_ms, snap_wait_ms:latest.snap_wait_ms, snap_forced_realtime:latest.snap_forced_realtime, snap_stale_warning:!!latest.snap_stale_warning, snap_request_ws_sent:latest.snap_request_ws_sent, snap_live_frame_fallback:!!latest.snap_live_frame_fallback, face_snapshot_url:'/api/rt7/face/last_snapshot.jpg?h='+latest.snap_hash,
      debug_text:'RAILWAY_FACE=YES SNAP=' + latest.snap_time + ' HASH=' + latest.snap_hash + ' FACE_FOUND=NO COUNT=0 REASON=' + (detect.reason || 'NO_FACE'),
      time:nowIso()
    };
    cloudState.last_face_match = noface; broadcast('face_match', noface);
    appendEvent({ type:'face_match_detect_no_face', known_face:false, confidence:0, message:noface.summary });
    return noface;
  }

  const faces = rt7ReadFaces_();
  if (!faces.length) return { ok:false, version:SERVER_VERSION, error:'NO_ENROLLED_FACE', answer:'尚未註冊人臉，請先輸入姓名後按「註冊」。', count:0 };
  try { if (rt7RefreshFaceEmbeddingCaches_(faces)) rt7SaveFaces_(faces); } catch(e) { console.warn('[FACE_API][V54O][CACHE_REFRESH_WARN] ' + String(e && e.message || e)); }
  const refs = faces.slice(0, 6);
  const match = await rt7MatchKnownFaceOnly_(latest, refs, detect);
  const confidence = Number(match.confidence || 0);
  const faceKnownByMatch = !!match.known_face && confidence >= 40;
  let liveness = { enabled:true, skipped:true, live_face:false, verdict:'SKIPPED', confidence:0, reason:'FACE_MATCH_NOT_PASS', summary:'Face Match 未通過，略過 Vision 活體檢測。' };
  if (faceKnownByMatch) {
    console.log('[FACE_LIVENESS][V56I] Face Match PASS, start Vision liveness hash=' + latest.snap_hash + ' name=' + (match.matched_name || ''));
    liveness = await rt7VisionLivenessCheck_(latest, detect);
    console.log('[FACE_LIVENESS][V56I] result live=' + (!!liveness.live_face) + ' verdict=' + liveness.verdict + ' conf=' + liveness.confidence + ' reason=' + liveness.reason);
  }
  const known = faceKnownByMatch && !!liveness.live_face;
  const faceMatchName = faceKnownByMatch ? safeString(match.matched_name || refs[0]?.name || '') : 'unknown';
  const livenessLabel = safeString(liveness && liveness.verdict || (liveness && liveness.live_face ? 'REAL' : 'UNKNOWN')).toUpperCase() || 'UNKNOWN';
  const doorDecision = known ? 'ALLOW' : 'DENY';
  const displayText = 'FACE_FOUND=' + (detect.face_found ? 'YES' : 'NO') + '\n' +
    'FACE_MATCH=' + faceMatchName + '\n' +
    'MATCH=' + confidence + '%\n\n' +
    'LIVENESS=' + livenessLabel + '\n\n' +
    'DOOR=' + doorDecision;
  const result = {
    ok:true, version:SERVER_VERSION, api_entered:true, api_path:'/api/rt7/face/match', type:'face_match', stage:'RAILWAY_MATCH_PLUS_VISION_LIVENESS', engine:'railway_local_plus_openai_vision', gpt_used:faceKnownByMatch,
    face_gate:gate,
    liveness,
    face_found:true,
    face_count:detect.face_count,
    face_box:detect.face_box,
    face_ratio:detect.face_ratio,
    face_match_pass:faceKnownByMatch,
    face_match:faceMatchName,
    match_score:confidence,
    liveness_label:livenessLabel,
    door:doorDecision,
    door_allow:known,
    display_text:displayText,
    known_face:known,
    matched_name: faceKnownByMatch ? faceMatchName : '',
    confidence,
    backlight_tolerant:true,
    pass_threshold:40,
    liveness_required:true,
    liveness_pass:!!liveness.live_face,
    face_quality:detect.face_quality,
    face_position:detect.face_position,
    fail_stage:known ? 'NONE' : (faceKnownByMatch ? 'LIVENESS' : 'MATCH'),
    reason: known ? 'FACE_OK_PLUS_LIVENESS' : (faceKnownByMatch ? ('LIVENESS_FAIL_' + (liveness.verdict || 'UNKNOWN')) : (match.reason || 'LOW_SIMILARITY')),
    summary: known ? ('人臉 + 活體檢測通過：' + (match.matched_name || refs[0]?.name || '已註冊') + ' / Face=' + confidence + '% / Live=' + (liveness.confidence || 0) + '%') : (faceKnownByMatch ? ('Face Match 通過，但 Vision 活體檢測未通過，已阻擋開門：' + (liveness.summary || liveness.reason || 'UNKNOWN')) : (match.summary || '已偵測到人臉，但與註冊名單相似度不足。')),
    count:faces.length,
    latest_bytes:latest.bytes,
    snap_time:latest.snap_time,
    snap_source:latest.snap_source,
    snap_hash:latest.snap_hash,
    snap_age_ms:latest.snap_age_ms,
    face_snapshot_url:'/api/rt7/face/last_snapshot.jpg?h='+latest.snap_hash,
    match_ms:match.match_ms || 0,
    cache_mode:match.cache_mode || 'FAST_CACHE',
    cache_hits:match.cache_hits || 0,
    cache_miss:match.cache_miss || 0,
    compared:match.compared || refs.length,
    ref_names:refs.map(f => safeString(f.name || '')),
    debug_text:'RAILWAY_FACE=YES SNAP=' + latest.snap_time + ' HASH=' + latest.snap_hash + ' FACE_FOUND=YES COUNT=' + detect.face_count + ' BOX=' + JSON.stringify(detect.face_box) + ' RATIO=' + detect.face_ratio + '% FACE_MATCH=' + faceKnownByMatch + ' LIVENESS=' + (!!liveness.live_face) + ' VERDICT=' + (liveness.verdict || '') + ' NAME=' + (match.matched_name || '') + ' CONF=' + confidence + ' LIVE_CONF=' + (liveness.confidence || 0) + ' DOOR=' + doorDecision + ' QUALITY=' + detect.face_quality + ' MATCH_MS=' + (match.match_ms || 0) + ' CACHE_HITS=' + (match.cache_hits || 0) + ' POS=' + detect.face_position + ' REASON=' + (known ? 'FACE_OK_PLUS_LIVENESS' : (faceKnownByMatch ? ('LIVENESS_FAIL_' + (liveness.verdict || 'UNKNOWN')) : (match.reason || 'LOW_SIMILARITY'))),
    time:nowIso()
  };
  cloudState.last_face_match = result;
  appendEvent({ type:'face_match', name:result.matched_name, known_face:result.known_face, confidence:result.confidence, message:result.summary });
  console.log('[FACE_API][V56I] result stage=' + result.stage + ' hash=' + result.snap_hash + ' face_found=' + result.face_found + ' count=' + result.face_count + ' box=' + JSON.stringify(result.face_box) + ' ratio=' + result.face_ratio + '% face_match=' + result.face_match_pass + ' liveness=' + result.liveness_pass + ' verdict=' + (result.liveness && result.liveness.verdict || '') + ' known=' + result.known_face + ' name=' + result.matched_name + ' confidence=' + result.confidence + ' quality=' + result.face_quality + ' pos=' + result.face_position + ' reason=' + result.reason + ' fail_stage=' + result.fail_stage);
  broadcast('face_match', result);
  return result;
}

// V5.3B: server-side single-shot guard.  The phone UI can accidentally send
// multiple face requests while the stream is being stopped/restarted.  Do not
// queue multiple face_snapshot_now commands; reuse the in-flight/recent result.
let rt7FaceMatchServerBusy_ = false;
let rt7FaceMatchServerStartedMs_ = 0;
let rt7FaceMatchServerLastResult_ = null;
async function rt7FaceMatchLatest_() {
  const now = Date.now();
  if (rt7FaceMatchServerBusy_) {
    const base = cloudState.last_face_match || rt7FaceMatchServerLastResult_ || {};
    const out = Object.assign({}, base, {
      ok: true,
      version: SERVER_VERSION,
      type: 'face_match',
      face_single_shot: 'busy_reuse',
      duplicate_suppressed: true,
      answer: '人臉辨識進行中，請稍候，不重複拍照。',
      summary: base.summary || '人臉辨識進行中，已阻擋重複 face_snapshot_now。',
      time: nowIso()
    });
    console.log('[FACE_API][V54O][SINGLE_SHOT_BUSY] reuse last result, age_ms=' + (now - rt7FaceMatchServerStartedMs_));
    return out;
  }
  if (rt7FaceMatchServerLastResult_ && (now - rt7FaceMatchServerStartedMs_) < 2200) {
    const out = Object.assign({}, rt7FaceMatchServerLastResult_, {
      version: SERVER_VERSION,
      face_single_shot: 'cooldown_reuse',
      duplicate_suppressed: true,
      time: nowIso()
    });
    console.log('[FACE_API][V54O][SINGLE_SHOT_COOLDOWN] reuse recent result, age_ms=' + (now - rt7FaceMatchServerStartedMs_));
    return out;
  }
  rt7FaceMatchServerBusy_ = true;
  rt7FaceMatchServerStartedMs_ = now;
  try {
    const result = await rt7FaceMatchLatestCore_();
    rt7FaceMatchServerLastResult_ = result;
    return result;
  } finally {
    setTimeout(() => { rt7FaceMatchServerBusy_ = false; }, 900);
  }
}

app.get('/api/rt7/face/last_snapshot.jpg', (req,res) => {
  ensureDataDir();
  if (!fs.existsSync(FACE_DEBUG_SNAPSHOT_FILE)) return res.status(404).json({ok:false, version:SERVER_VERSION, error:'NO_FACE_DEBUG_SNAPSHOT'});
  res.set('Cache-Control','no-store, no-cache, must-revalidate, max-age=0');
  res.type('image/jpeg').send(fs.readFileSync(FACE_DEBUG_SNAPSHOT_FILE));
});

app.get('/api/rt7/faces', (req,res) => {
  const faces = rt7ReadFaces_().map(f => ({ id:f.id, name:f.name, time:f.time, bytes:f.bytes, device_id:f.device_id || '#1' }));
  res.json({ ok:true, version:SERVER_VERSION, count:faces.length, faces, last_face_match:cloudState.last_face_match || null });
});
app.post('/api/rt7/faces/cache/rebuild', (req,res) => {
  const faces = rt7ReadFaces_();
  const changed = rt7RefreshFaceEmbeddingCaches_(faces);
  if (changed) rt7SaveFaces_(faces);
  res.json({ ok:true, version:SERVER_VERSION, changed, count:faces.length, cached:faces.filter(f => f.embedding_cache && f.embedding_cache.ok).length, engine:'railway_local_fast_cache' });
});
app.get('/api/rt7/phase6c3_plugin/faces', (req,res) => {
  const faces = rt7ReadFaces_().map(f => ({ id:f.id, name:f.name, time:f.time, bytes:f.bytes, device_id:f.device_id || '#1' }));
  res.json({ ok:true, version:SERVER_VERSION, count:faces.length, faces });
});
function rt7EnrollFaceB64_(name, b64, meta) {
  meta = meta || {};
  name = safeString(name || '').trim() || '未命名';
  b64 = safeString(b64 || '').replace(/^data:image\/\w+;base64,/i, '').trim();
  if (!b64) return { ok:false, error:'NO_IMAGE_B64', answer:'沒有取得手機鏡頭照片。' };
  let bytes = 0;
  try { bytes = Buffer.from(b64, 'base64').length; } catch (e) { return { ok:false, error:'BAD_IMAGE_B64', answer:'照片格式錯誤，請重新拍照。' }; }
  if (bytes < 3000) return { ok:false, error:'IMAGE_TOO_SMALL', answer:'照片資料太小，請重新拍照。' };
  const faces = rt7ReadFaces_();
  const id = 'face_' + Date.now();
  const face = { id, name, image_b64:b64, bytes, time:nowIso(), device_id:safeString(meta.device_id || '#mobile'), source:safeString(meta.source || 'mobile_selfie') };
  try {
    const emb = rt7ExtractFaceEmbedding_(b64, null);
    if (emb && emb.ok) { face.embedding_cache = rt7FaceEmbeddingToCache_(emb); face.embedding_cache_key = rt7FaceEmbeddingCacheKey_(face); }
    else return { ok:false, error:'NO_FACE_IN_SELFIE', answer:'手機照片未偵測到清楚人臉，請靠近鏡頭並重新拍照。' };
  } catch(e) { console.warn('[FACE_API][V56J][MOBILE_ENROLL_CACHE_WARN] ' + String(e && e.message || e)); }
  faces.unshift(face);
  rt7SaveFaces_(faces.slice(0, 40));
  const ev = appendEvent({ type:'face_enroll_mobile', id, name, bytes, source:face.source, message:'mobile selfie enrolled face '+name });
  broadcast('face_enroll', { id, name, bytes, source:face.source, time:face.time });
  return { ok:true, version:SERVER_VERSION, enrolled:{ id, name, bytes, time:face.time, source:face.source }, count:faces.length, event:ev, answer:'已用手機前鏡頭註冊：' + name };
}

app.post('/api/rt7/face/enroll_mobile', (req,res) => {
  const name = safeString(req.body?.name || req.query.name || '').trim() || '未命名';
  const image = safeString(req.body?.image || req.body?.image_b64 || req.body?.jpeg_b64 || '');
  const out = rt7EnrollFaceB64_(name, image, { device_id:req.body?.device_id || '#mobile', source:'mobile_selfie' });
  res.status(200).json(out);
});

app.post('/api/rt7/face/enroll', rt7EnrollHandler_);
function rt7EnrollHandler_(req,res){
  const name = safeString(req.body?.name || req.query.name || req.query.face_id || req.query.id || '').trim() || '未命名';
  const image = safeString(req.body?.image || req.body?.image_b64 || req.body?.jpeg_b64 || '');
  if (image) return res.status(200).json(rt7EnrollFaceB64_(name, image, { device_id:req.body?.device_id || '#mobile', source:req.body?.source || 'mobile_selfie' }));
  const latest = rt7LatestJpegB64_();
  if (!latest) return res.status(200).json({ ok:false, version:SERVER_VERSION, error:'NO_LATEST_SNAPSHOT', answer:'尚無最新照片，請先開始影像或使用手機前鏡頭註冊。' });
  const faces = rt7ReadFaces_();
  const id = 'face_' + Date.now();
  const face = { id, name, image_b64:latest.b64, bytes:latest.bytes, time:nowIso(), device_id:safeString(req.body?.device_id || req.query.device_id || '#1'), source:'esp32_snapshot' };
  try {
    const emb = rt7ExtractFaceEmbedding_(latest.b64, null);
    if (emb && emb.ok) { face.embedding_cache = rt7FaceEmbeddingToCache_(emb); face.embedding_cache_key = rt7FaceEmbeddingCacheKey_(face); }
  } catch(e) { console.warn('[FACE_API][V54O][ENROLL_CACHE_WARN] ' + String(e && e.message || e)); }
  faces.unshift(face);
  rt7SaveFaces_(faces.slice(0, 40));
  const ev = appendEvent({ type:'face_enroll', id, name, bytes:latest.bytes, source:'esp32_snapshot', message:'enrolled face '+name });
  broadcast('face_enroll', { id, name, bytes:latest.bytes, source:'esp32_snapshot', time:face.time });
  res.json({ ok:true, version:SERVER_VERSION, enrolled:{ id, name, bytes:latest.bytes, time:face.time, source:'esp32_snapshot' }, count:faces.length, event:ev, answer:'已註冊：' + name });
}
app.get('/api/rt7/phase6c3_plugin/face/enroll_now', rt7EnrollHandler_);
app.post('/api/rt7/phase6c3_plugin/face/enroll_now', rt7EnrollHandler_);
app.post('/api/rt7/phase6c3_plugin/face/enroll', rt7EnrollHandler_);
app.post('/api/rt7/face/match', async (req,res) => {
  try { res.json(await rt7FaceMatchLatest_()); }
  catch(e) { res.status(200).json({ ok:false, version:SERVER_VERSION, error:String(e.message || e), answer:'人臉辨識失敗，請確認 Railway 已設定 OPENAI_API_KEY。' }); }
});
app.get('/api/rt7/face/match', async (req,res) => {
  try { res.json(await rt7FaceMatchLatest_()); }
  catch(e) { res.status(200).json({ ok:false, version:SERVER_VERSION, error:String(e.message || e), answer:'人臉辨識失敗，請確認 Railway 已設定 OPENAI_API_KEY。' }); }
});
app.get('/api/rt7/phase6c3_plugin/face/match', async (req,res) => {
  try { res.json(await rt7FaceMatchLatest_()); }
  catch(e) { res.status(200).json({ ok:false, version:SERVER_VERSION, error:String(e.message || e), answer:'人臉辨識失敗，請確認 Railway 已設定 OPENAI_API_KEY。' }); }
});
app.post('/api/face/recognize', async (req,res) => {
  try { res.json(await rt7FaceMatchLatest_()); }
  catch(e) { res.status(200).json({ ok:false, version:SERVER_VERSION, error:String(e.message || e), answer:'人臉辨識失敗，請確認 Railway 已設定 OPENAI_API_KEY。' }); }
});
app.get('/api/face/recognize', async (req,res) => {
  try { res.json(await rt7FaceMatchLatest_()); }
  catch(e) { res.status(200).json({ ok:false, version:SERVER_VERSION, error:String(e.message || e), answer:'人臉辨識失敗，請確認 Railway 已設定 OPENAI_API_KEY。' }); }
});

app.get('/api/rt7/face_gate/state', (req,res) => {
  res.json({ ok:true, version:SERVER_VERSION, enabled:!!cloudState.face_gate_enabled, auto_enabled:!!cloudState.face_gate_auto_enabled, auto_busy:!!cloudState.face_gate_auto_busy, auto_cooldown_ms:cloudState.face_gate_auto_cooldown_ms, last_face_gate:cloudState.last_face_gate || null, last_face_match:cloudState.last_face_match || null });
});
app.get('/api/rt7/face_gate/serial_result', (req,res) => {
  const m = cloudState.last_face_match || {};
  const g = cloudState.last_face_gate || {};
  res.json({
    ok:true,
    version:SERVER_VERSION,
    time:nowIso(),
    has_result:!!(cloudState.last_face_match),
    auto_face_gate:!!m.auto_face_gate,
    trigger_source:safeString(m.trigger_source || ''),
    known_face:!!m.known_face,
    matched_name:safeString(m.matched_name || ''),
    confidence:Number(m.confidence || 0),
    face_match:safeString(m.face_match || m.matched_name || ''),
    match_score:Number(m.match_score || m.confidence || 0),
    liveness_label:safeString(m.liveness_label || (m.liveness && m.liveness.verdict) || ''),
    liveness_pass:!!m.liveness_pass,
    door:safeString(m.door || (m.known_face ? 'ALLOW' : 'DENY')),
    door_allow:!!m.door_allow,
    display_text:safeString(m.display_text || ''),
    face_found:!!m.face_found,
    face_count:Number(m.face_count || 0),
    face_box:m.face_box || {x:0,y:0,w:0,h:0},
    face_ratio:Number(m.face_ratio || 0),
    face_quality:safeString(m.face_quality || ''),
    face_position:safeString(m.face_position || ''),
    fail_stage:safeString(m.fail_stage || ''),
    reason:safeString(m.reason || ''),
    summary:safeString(m.summary || m.answer || '').slice(0,220),
    snap_hash:safeString(m.snap_hash || ''),
    snap_age_ms:Number(m.snap_age_ms || 0),
    latest_bytes:Number(m.latest_bytes || 0),
    gate_pass:!!g.pass,
    gate_reason:safeString(g.reason || ''),
    gate_score:Number(g.candidate || g.score || 0)
  });
});
app.post('/api/rt7/face_gate/auto', (req,res) => {
  const mode = safeString(req.body?.mode || req.query.mode || '');
  if (/^(on|1|true|enable)$/i.test(mode)) cloudState.face_gate_auto_enabled = true;
  else if (/^(off|0|false|disable)$/i.test(mode)) cloudState.face_gate_auto_enabled = false;
  else cloudState.face_gate_auto_enabled = !cloudState.face_gate_auto_enabled;
  res.json({ ok:true, version:SERVER_VERSION, auto_enabled:!!cloudState.face_gate_auto_enabled, auto_busy:!!cloudState.face_gate_auto_busy });
});
app.post('/api/rt7/face_gate/toggle', (req,res) => {
  const mode = safeString(req.body?.mode || req.query.mode || '');
  if (/^(on|1|true|enable)$/i.test(mode)) cloudState.face_gate_enabled = true;
  else if (/^(off|0|false|disable)$/i.test(mode)) cloudState.face_gate_enabled = false;
  else cloudState.face_gate_enabled = !cloudState.face_gate_enabled;
  console.log('[RT7_FACE_GATE][TOGGLE][V52D] set enabled=' + cloudState.face_gate_enabled);
  res.json({ ok:true, version:SERVER_VERSION, enabled:!!cloudState.face_gate_enabled, auto_enabled:!!cloudState.face_gate_auto_enabled, last_face_gate:cloudState.last_face_gate || null });
});
app.get('/api/rt7/face/state', (req,res) => {
  res.json({ ok:true, version:SERVER_VERSION, api:'/api/rt7/face/match', alias:'/api/face/recognize', faces:rt7ReadFaces_().length, last_face_match:cloudState.last_face_match || null, latest_snapshot:getSnapshotMeta_(), face_gate_enabled:!!cloudState.face_gate_enabled, last_face_gate:cloudState.last_face_gate || null });
});

app.get('/api/rt7/faces/reset', (req,res) => {
  rt7SaveFaces_([]);
  const ev = appendEvent({ type:'faces_reset', message:'face list reset' });
  broadcast('faces_reset', ev);
  res.json({ ok:true, version:SERVER_VERSION, count:0, event:ev });
});
app.get('/api/rt7/phase6c3_plugin/faces/reset', (req,res) => {
  rt7SaveFaces_([]);
  res.json({ ok:true, version:SERVER_VERSION, count:0 });
});



// ---------- V5.6K Face DB Manager / backup / restore ----------
function rt7FacePublic_(f) {
  return {
    id: safeString(f && f.id),
    name: safeString(f && f.name),
    time: safeString(f && f.time),
    bytes: Number(f && f.bytes || 0),
    device_id: safeString(f && f.device_id || ''),
    source: safeString(f && f.source || ''),
    image_url: '/api/rt7/face/image/' + encodeURIComponent(safeString(f && f.id)) + '.jpg'
  };
}
function rt7FaceStats_() {
  const faces = rt7ReadFaces_();
  const bytes = faces.reduce((n,f)=>n+Number(f && f.bytes || 0),0);
  const names = Array.from(new Set(faces.map(f=>safeString(f && f.name).trim()).filter(Boolean)));
  return { persons:names.length, photos:faces.length, bytes, mb:Math.round(bytes/1024/1024*100)/100, names };
}
function rt7SanitizeFaceImport_(arr) {
  const out = [];
  for (const x of (Array.isArray(arr) ? arr : [])) {
    const name = safeString(x && x.name).trim();
    const b64 = safeString(x && (x.image_b64 || x.image || x.jpeg_b64)).replace(/^data:image\/\w+;base64,/i,'').trim();
    if (!name || !b64) continue;
    let bytes = 0;
    try { bytes = Buffer.from(b64, 'base64').length; } catch (_) { continue; }
    if (bytes < 1000) continue;
    const id = safeString(x && x.id).trim() || ('face_' + Date.now() + '_' + Math.floor(Math.random()*100000));
    const f = {
      id, name, image_b64:b64, bytes,
      time:safeString(x && x.time).trim() || nowIso(),
      device_id:safeString(x && x.device_id).trim() || '#import',
      source:safeString(x && x.source).trim() || 'restore_import'
    };
    try {
      const emb = rt7ExtractFaceEmbedding_(b64, null);
      if (emb && emb.ok) { f.embedding_cache = rt7FaceEmbeddingToCache_(emb); f.embedding_cache_key = rt7FaceEmbeddingCacheKey_(f); }
      else if (x && x.embedding_cache) f.embedding_cache = x.embedding_cache;
    } catch (_) { if (x && x.embedding_cache) f.embedding_cache = x.embedding_cache; }
    out.push(f);
  }
  return out;
}
function rt7ZipStore_(files) {
  // Minimal ZIP writer: STORE method only, no external npm dependency.
  const chunks = [];
  const central = [];
  let offset = 0;
  function dosTime(d) {
    d = d || new Date();
    const time = ((d.getHours() & 31) << 11) | ((d.getMinutes() & 63) << 5) | (Math.floor(d.getSeconds()/2) & 31);
    const date = (((d.getFullYear()-1980) & 127) << 9) | (((d.getMonth()+1) & 15) << 5) | (d.getDate() & 31);
    return { time, date };
  }
  function crc32(buf) {
    let c = ~0;
    if (!crc32.table) {
      crc32.table = Array.from({length:256}, (_,n)=>{let x=n; for(let k=0;k<8;k++) x=(x&1)?(0xedb88320^(x>>>1)):(x>>>1); return x>>>0;});
    }
    for (const b of buf) c = crc32.table[(c ^ b) & 255] ^ (c >>> 8);
    return (~c) >>> 0;
  }
  for (const file of files) {
    const nameBuf = Buffer.from(file.name.replace(/^\/+/, ''), 'utf8');
    const data = Buffer.isBuffer(file.data) ? file.data : Buffer.from(String(file.data || ''), 'utf8');
    const crc = crc32(data);
    const dt = dosTime(new Date());
    const local = Buffer.alloc(30);
    local.writeUInt32LE(0x04034b50,0); local.writeUInt16LE(20,4); local.writeUInt16LE(0,6); local.writeUInt16LE(0,8);
    local.writeUInt16LE(dt.time,10); local.writeUInt16LE(dt.date,12); local.writeUInt32LE(crc,14);
    local.writeUInt32LE(data.length,18); local.writeUInt32LE(data.length,22); local.writeUInt16LE(nameBuf.length,26); local.writeUInt16LE(0,28);
    chunks.push(local, nameBuf, data);
    const cent = Buffer.alloc(46);
    cent.writeUInt32LE(0x02014b50,0); cent.writeUInt16LE(20,4); cent.writeUInt16LE(20,6); cent.writeUInt16LE(0,8); cent.writeUInt16LE(0,10);
    cent.writeUInt16LE(dt.time,12); cent.writeUInt16LE(dt.date,14); cent.writeUInt32LE(crc,16);
    cent.writeUInt32LE(data.length,20); cent.writeUInt32LE(data.length,24); cent.writeUInt16LE(nameBuf.length,28); cent.writeUInt16LE(0,30); cent.writeUInt16LE(0,32);
    cent.writeUInt16LE(0,34); cent.writeUInt16LE(0,36); cent.writeUInt32LE(0,38); cent.writeUInt32LE(offset,42);
    central.push(cent, nameBuf);
    offset += local.length + nameBuf.length + data.length;
  }
  const centralStart = offset;
  const centralBuf = Buffer.concat(central);
  const end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50,0); end.writeUInt16LE(0,4); end.writeUInt16LE(0,6); end.writeUInt16LE(files.length,8); end.writeUInt16LE(files.length,10);
  end.writeUInt32LE(centralBuf.length,12); end.writeUInt32LE(centralStart,16); end.writeUInt16LE(0,20);
  return Buffer.concat([...chunks, centralBuf, end]);
}
function rt7FaceBackupObject_() {
  const faces = rt7ReadFaces_();
  return { ok:true, version:SERVER_VERSION, exported_at:nowIso(), count:faces.length, stats:rt7FaceStats_(), faces };
}

async function rt7FaceSqliteBuffer_() {
  // Build a real SQLite .sqlite file using sql.js (pure JS + WASM). The photos stay in ZIP/JPG;
  // SQLite records names, photo metadata, image paths, and image URL for readable management.
  let initSqlJs;
  try { initSqlJs = require('sql.js'); }
  catch (e) {
    const msg = 'SQLITE_EXPORT_NEEDS_SQLJS: please npm install sql.js';
    const err = new Error(msg); err.cause = e; throw err;
  }
  const SQL = await initSqlJs();
  const db = new SQL.Database();
  db.run('PRAGMA user_version = 5601');
  db.run('CREATE TABLE faces (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, photo_count INTEGER DEFAULT 0, total_bytes INTEGER DEFAULT 0, first_created_at TEXT, last_created_at TEXT)');
  db.run('CREATE TABLE face_photos (id TEXT PRIMARY KEY, face_name TEXT NOT NULL, filename TEXT NOT NULL, path TEXT NOT NULL, image_url TEXT NOT NULL, size_bytes INTEGER DEFAULT 0, created_at TEXT, source TEXT, device_id TEXT)');
  db.run('CREATE INDEX idx_face_photos_name ON face_photos(face_name)');
  const faces = rt7ReadFaces_();
  const byName = {};
  for (const f of faces) {
    const name = safeString(f && f.name || '未命名').trim() || '未命名';
    const id = safeString(f && f.id || ('face_'+Date.now()));
    const safeName = name.replace(/[\\/:*?"<>|]/g,'_').slice(0,60) || 'unknown';
    const filename = id + '.jpg';
    const path = 'faces/' + safeName + '/' + filename;
    const imageUrl = '/api/rt7/face/image/' + encodeURIComponent(id) + '.jpg';
    const bytes = Number(f && f.bytes || 0);
    const created = safeString(f && f.time || '');
    const row = byName[name] || (byName[name] = { name, count:0, bytes:0, first:'', last:'' });
    row.count++; row.bytes += bytes;
    if (created && (!row.first || created < row.first)) row.first = created;
    if (created && (!row.last || created > row.last)) row.last = created;
    const st = db.prepare('INSERT OR REPLACE INTO face_photos (id, face_name, filename, path, image_url, size_bytes, created_at, source, device_id) VALUES (?,?,?,?,?,?,?,?,?)');
    st.run([id, name, filename, path, imageUrl, bytes, created, safeString(f && f.source || ''), safeString(f && f.device_id || '')]);
    st.free();
  }
  const stFace = db.prepare('INSERT INTO faces (name, photo_count, total_bytes, first_created_at, last_created_at) VALUES (?,?,?,?,?)');
  Object.keys(byName).sort().forEach(k => { const r = byName[k]; stFace.run([r.name, r.count, r.bytes, r.first, r.last]); });
  stFace.free();
  const data = db.export();
  db.close();
  return Buffer.from(data);
}

app.get('/api/rt7/face/image/:id.jpg', (req,res) => {
  const id = safeString(req.params.id).replace(/\.jpg$/,'');
  const f = rt7ReadFaces_().find(x => safeString(x.id) === id);
  if (!f || !f.image_b64) return res.status(404).json({ ok:false, version:SERVER_VERSION, error:'FACE_IMAGE_NOT_FOUND' });
  try {
    res.set('Cache-Control','no-store');
    res.type('image/jpeg').send(Buffer.from(safeString(f.image_b64).replace(/^data:image\/\w+;base64,/i,''), 'base64'));
  } catch (e) { res.status(500).json({ ok:false, version:SERVER_VERSION, error:String(e.message || e) }); }
});
app.get('/api/rt7/faces/full', (req,res) => {
  const faces = rt7ReadFaces_();
  res.json({ ok:true, version:SERVER_VERSION, stats:rt7FaceStats_(), count:faces.length, faces:faces.map(rt7FacePublic_), last_face_match:cloudState.last_face_match || null });
});
app.post('/api/rt7/face/delete', (req,res) => {
  const id = safeString(req.body && req.body.id || req.query.id).trim();
  const before = rt7ReadFaces_();
  const after = before.filter(f => safeString(f.id) !== id);
  rt7SaveFaces_(after);
  const ev = appendEvent({ type:'face_delete', id, message:'deleted face photo '+id });
  broadcast('faces_changed', { action:'delete', id, count:after.length });
  res.json({ ok:true, version:SERVER_VERSION, deleted:before.length-after.length, count:after.length, event:ev });
});

app.post('/api/rt7/face/delete_person', (req,res) => {
  const name = safeString(req.body && req.body.name || req.query.name).trim();
  if (!name) return res.status(200).json({ ok:false, version:SERVER_VERSION, error:'NAME_REQUIRED' });
  const before = rt7ReadFaces_();
  const after = before.filter(f => safeString(f.name).trim() !== name);
  rt7SaveFaces_(after);
  const deleted = before.length - after.length;
  const ev = appendEvent({ type:'face_delete_person', name, deleted, message:'deleted all face photos for '+name });
  broadcast('faces_changed', { action:'delete_person', name, deleted, count:after.length });
  res.json({ ok:true, version:SERVER_VERSION, name, deleted, count:after.length, event:ev });
});
app.post('/api/rt7/face/rename', (req,res) => {
  const oldName = safeString(req.body && req.body.old_name || req.body && req.body.oldName || req.query.old_name).trim();
  const newName = safeString(req.body && req.body.new_name || req.body && req.body.newName || req.query.new_name).trim();
  if (!oldName || !newName) return res.status(200).json({ ok:false, version:SERVER_VERSION, error:'NAME_REQUIRED' });
  const faces = rt7ReadFaces_();
  let n = 0;
  for (const f of faces) if (safeString(f.name).trim() === oldName) { f.name = newName; n++; }
  rt7SaveFaces_(faces);
  const ev = appendEvent({ type:'face_rename', old_name:oldName, new_name:newName, count:n, message:'renamed face person '+oldName+' to '+newName });
  broadcast('faces_changed', { action:'rename', old_name:oldName, new_name:newName, count:n });
  res.json({ ok:true, version:SERVER_VERSION, renamed:n, count:faces.length, event:ev });
});
app.get('/api/rt7/face/backup.json', (req,res) => {
  res.set('Cache-Control','no-store');
  res.set('Content-Disposition','attachment; filename="rt7_face_backup.json"');
  res.json(rt7FaceBackupObject_());
});
app.get('/api/rt7/face/backup.sqlite', async (req,res) => {
  try {
    const db = await rt7FaceSqliteBuffer_();
    res.set('Cache-Control','no-store');
    res.set('Content-Type','application/vnd.sqlite3');
    res.set('Content-Disposition','attachment; filename="rt7_face_backup.sqlite"');
    res.send(db);
  } catch (e) {
    res.status(500).json({ ok:false, version:SERVER_VERSION, error:'SQLITE_EXPORT_FAILED', message:String(e.message || e) });
  }
});
app.get('/api/rt7/face/backup.zip', async (req,res) => {
  const faces = rt7ReadFaces_();
  const meta = rt7FaceBackupObject_();
  const files = [{ name:'face_backup.json', data:JSON.stringify(meta, null, 2) }];
  try { files.push({ name:'face_backup.sqlite', data:await rt7FaceSqliteBuffer_() }); }
  catch (e) { files.push({ name:'SQLITE_EXPORT_ERROR.txt', data:String(e.message || e) }); }
  for (const f of faces) {
    if (!f || !f.image_b64) continue;
    const safeName = safeString(f.name || 'unknown').replace(/[\\/:*?"<>|]/g,'_').slice(0,60) || 'unknown';
    const id = safeString(f.id || ('face_'+Date.now()));
    files.push({ name:'faces/'+safeName+'/'+id+'.jpg', data:Buffer.from(safeString(f.image_b64).replace(/^data:image\/\w+;base64,/i,''), 'base64') });
  }
  const zip = rt7ZipStore_(files);
  res.set('Cache-Control','no-store');
  res.set('Content-Type','application/zip');
  res.set('Content-Disposition','attachment; filename="rt7_face_backup.zip"');
  res.send(zip);
});
app.post('/api/rt7/face/restore_json', (req,res) => {
  const mode = safeString(req.body && req.body.mode || req.query.mode || 'replace').toLowerCase();
  let obj = req.body || {};
  if (typeof obj === 'string') { try { obj = JSON.parse(obj); } catch (_) { obj = {}; } }
  const inputFaces = Array.isArray(obj) ? obj : (Array.isArray(obj.faces) ? obj.faces : []);
  const imported = rt7SanitizeFaceImport_(inputFaces);
  if (!imported.length) return res.status(200).json({ ok:false, version:SERVER_VERSION, error:'NO_VALID_FACES_IN_BACKUP' });
  const current = mode === 'append' ? rt7ReadFaces_() : [];
  const ids = new Set(current.map(f=>safeString(f.id)));
  const merged = current.concat(imported.map(f => { while(ids.has(f.id)){ f.id = 'face_' + Date.now() + '_' + Math.floor(Math.random()*100000); } ids.add(f.id); return f; }));
  rt7SaveFaces_(merged.slice(0, 200));
  const ev = appendEvent({ type:'face_restore_json', mode, imported:imported.length, total:merged.length, message:'restored face backup json' });
  broadcast('faces_changed', { action:'restore', mode, imported:imported.length, count:merged.length });
  res.json({ ok:true, version:SERVER_VERSION, mode, imported:imported.length, count:merged.length, event:ev });
});

app.get('/rt7_face_db_manager', (req,res) => res.type('html').send(`<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1"><title>RT7 Face DB Manager</title><style>
body{margin:0;background:#f3f7fb;color:#10212b;font-family:system-ui,-apple-system,BlinkMacSystemFont,'Noto Sans TC',sans-serif}.top{background:#062b31;color:white;padding:18px 14px;text-align:center;font-weight:900}.top a{float:left;color:white;text-decoration:none;background:#41506a;border-radius:10px;padding:9px 12px}.wrap{max-width:980px;margin:0 auto;padding:14px}.card{background:white;border:1px solid #d5e0ea;border-radius:16px;padding:14px;margin:12px 0;box-shadow:0 4px 18px #0001}.row{display:flex;gap:10px;flex-wrap:wrap;align-items:center}button,.btn{border:0;border-radius:12px;padding:12px 14px;font-weight:900;font-size:16px;color:white;background:#148bd5;text-decoration:none;display:inline-block}button.green,.btn.green{background:#11aa58}button.red{background:#d92d2d}button.gray,.btn.gray{background:#41506a}input,textarea{border:1px solid #cad6e1;border-radius:10px;padding:10px;font-size:16px}textarea{width:100%;min-height:150px;box-sizing:border-box}.stats{font-weight:900;color:#6b2b20}.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:12px}.face{border:1px solid #d5e0ea;border-radius:12px;padding:8px;background:#fbfdff}.face img{width:100%;height:130px;object-fit:cover;border-radius:10px;background:#111;cursor:pointer}.face .row{gap:6px}.face button,.face .btn{font-size:13px;padding:8px 9px;border-radius:9px}.name{font-weight:900;margin-top:6px}.small{font-size:12px;color:#687886;word-break:break-all}.log{background:#071225;color:#dff7ff;padding:10px;border-radius:10px;white-space:pre-wrap;font-size:12px;max-height:180px;overflow:auto}.cap{font-size:13px;color:#667;margin:6px 0}.groupTitle{font-size:20px;font-weight:900;margin:12px 0 6px}.preview{width:220px;max-width:100%;border-radius:12px;background:#111}#camPanel{display:none}video{width:100%;max-height:360px;background:#111;border-radius:12px}</style></head><body><div class="top"><a href="/rt7_cloud_original_ui_doorbell">← 返回</a><div>RT7 FACE DB MANAGER</div><div style="font-size:13px;font-weight:600">照片預覽 / 本機上傳 / 手機拍照 / 單張刪除 / 刪除此人 / 改名 / SQLite / ZIP</div></div><main class="wrap">
<section class="card"><div class="row"><a class="btn green" href="/api/rt7/face/backup.zip">下載 ZIP</a><a class="btn gray" href="/api/rt7/face/backup.sqlite">下載 SQLite</a><a class="btn gray" href="/api/rt7/face/backup.json">下載 JSON</a><button class="gray" id="btnReload">重新載入</button><button class="red" id="btnReset">清空全部</button></div><p class="stats" id="stats">loading...</p></section>
<section class="card"><h2>新增照片</h2><div class="row"><input id="regName" placeholder="姓名，例如 gwansyan" value="gwansyan"><button class="green" id="btnOpenCam">開手機前鏡頭</button><button id="btnCapture">拍照加入</button><button class="gray" id="btnCloseCam">關閉鏡頭</button></div><div id="camPanel"><video id="video" playsinline autoplay muted></video><canvas id="canvas" style="display:none"></canvas></div><hr><h3>從手機/電腦選照片加入</h3><div class="row"><input id="fileName" placeholder="姓名，例如 gwansyan" value="gwansyan"><input id="filePhoto" type="file" accept="image/*"><button class="green" id="btnUploadPhoto">上傳照片加入</button></div><p class="cap">註冊可使用手機前鏡頭或本機照片；辨識仍使用 ESP32-CAM。建議每人 3～5 張不同角度照片。</p></section>
<section class="card"><h2>修改姓名</h2><div class="row"><input id="oldName" placeholder="原姓名"><input id="newName" placeholder="新姓名"><button id="btnRename">修改姓名</button></div></section>
<section class="card"><h2>還原 / 上傳備份 JSON</h2><p class="cap">可貼上 face_backup.json 內容；模式選 replace 會取代目前資料，append 會追加。</p><div class="row"><select id="restoreMode"><option value="replace">replace 取代</option><option value="append">append 追加</option></select><button id="btnRestore">還原 JSON</button></div><textarea id="backupText" placeholder="貼上 face_backup.json 內容"></textarea></section>
<section class="card"><h2>人臉照片管理</h2><p class="cap">可查看照片、下載單張照片、刪除單張照片、刪除此人全部照片。</p><div id="faces"></div></section><section class="card"><h2>狀態</h2><pre class="log" id="log">ready</pre></section></main><script>
var stream=null;function $(id){return document.getElementById(id)}function log(x){$('log').textContent=typeof x==='string'?x:JSON.stringify(x,null,2)}async function api(u,o){var r=await fetch(u+(u.indexOf('?')>=0?'&':'?')+'_='+Date.now(),Object.assign({cache:'no-store'},o||{}));var t=await r.text();try{return JSON.parse(t)}catch(e){return{ok:r.ok,status:r.status,raw:t}}}
async function load(){var j=await api('/api/rt7/faces/full');log(j);var s=j.stats||{};$('stats').textContent='人數：'+(s.persons||0)+'｜照片：'+(s.photos||0)+'｜容量：'+(s.mb||0)+' MB';var faces=j.faces||[];var by={};faces.forEach(function(f){var n=f.name||'未命名';(by[n]=by[n]||[]).push(f)});var html='';Object.keys(by).sort().forEach(function(n){html+='<div class="groupTitle">'+esc(n)+' <span class="small">('+by[n].length+'張)</span> <button class="red" data-delperson="'+esc(n)+'">刪除此人</button></div><div class="grid">';by[n].forEach(function(f){var img=f.image_url+'?_='+Date.now();html+='<div class="face"><img data-view="'+esc(f.image_url)+'" src="'+img+'"><div class="name">'+esc(f.name)+'</div><div class="small">ID：'+esc(f.id)+'</div><div class="small">來源：'+esc(f.source)+'｜'+esc(f.time)+'</div><div class="small">大小：'+(f.bytes||0)+' bytes</div><div class="row"><a class="btn gray" href="'+f.image_url+'" target="_blank">查看</a><a class="btn gray" href="'+f.image_url+'" download="'+esc(f.id)+'.jpg">下載</a><button class="red" data-del="'+esc(f.id)+'">刪除</button></div></div>'});html+='</div>'});$('faces').innerHTML=html||'<p>尚無人臉資料</p>';document.querySelectorAll('[data-del]').forEach(function(b){b.onclick=function(){del(this.getAttribute('data-del'))}});document.querySelectorAll('[data-delperson]').forEach(function(b){b.onclick=function(){delPerson(this.getAttribute('data-delperson'))}});document.querySelectorAll('[data-view]').forEach(function(img){img.onclick=function(){window.open(this.getAttribute('data-view'),'_blank')}})}
function esc(s){return String(s||'').replace(/[&<>"']/g,function(c){return{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]})}
async function del(id){if(!confirm('刪除此照片？'))return;var j=await api('/api/rt7/face/delete',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id:id})});log(j);load()}
async function delPerson(name){if(!confirm('刪除 '+name+' 的全部照片？'))return;var j=await api('/api/rt7/face/delete_person',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name:name})});log(j);load()}
async function rename(){var oldn=$('oldName').value.trim(),newn=$('newName').value.trim();if(!oldn||!newn){alert('請輸入原姓名與新姓名');return}var j=await api('/api/rt7/face/rename',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({old_name:oldn,new_name:newn})});log(j);load()}
async function openCam(){try{if(!navigator.mediaDevices){alert('瀏覽器不支援相機');return}stream=await navigator.mediaDevices.getUserMedia({video:{facingMode:'user',width:{ideal:720},height:{ideal:960}},audio:false});$('camPanel').style.display='block';$('video').srcObject=stream;try{await $('video').play()}catch(e){}}catch(e){log('相機開啟失敗：'+(e.message||e))}}
function closeCam(){try{if(stream)stream.getTracks().forEach(function(t){t.stop()})}catch(e){}stream=null;$('camPanel').style.display='none'}
async function capture(){var v=$('video'),c=$('canvas'),name=$('regName').value.trim()||'未命名';if(!v.videoWidth){alert('鏡頭尚未準備好');return}var scale=Math.min(1,720/v.videoWidth);c.width=Math.round(v.videoWidth*scale);c.height=Math.round(v.videoHeight*scale);c.getContext('2d').drawImage(v,0,0,c.width,c.height);var data=c.toDataURL('image/jpeg',0.86);var j=await api('/api/rt7/face/enroll_mobile',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name:name,source:'mobile_face_db_manager',device_id:'#mobile',image:data})});log(j);if(j.ok)load()}
async function uploadPhoto(){var f=$('filePhoto').files&&$('filePhoto').files[0];var name=$('fileName').value.trim()||$('regName').value.trim()||'未命名';if(!f){alert('請先選擇照片');return}var reader=new FileReader();reader.onload=async function(){var data=String(reader.result||'');var j=await api('/api/rt7/face/enroll_mobile',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name:name,source:'file_upload_face_db_manager',device_id:'#upload',image:data})});log(j);if(j.ok)load()};reader.readAsDataURL(f)}
async function restoreJson(){var txt=$('backupText').value.trim();if(!txt){alert('請貼上 JSON');return}var obj;try{obj=JSON.parse(txt)}catch(e){alert('JSON 格式錯誤');return}if(!confirm('確定還原？'))return;var j=await api('/api/rt7/face/restore_json?mode='+encodeURIComponent($('restoreMode').value),{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(obj)});log(j);load()}
$('btnReload').onclick=load;$('btnRename').onclick=rename;$('btnOpenCam').onclick=openCam;$('btnCloseCam').onclick=closeCam;$('btnCapture').onclick=capture;$('btnUploadPhoto').onclick=uploadPhoto;$('btnRestore').onclick=restoreJson;$('btnReset').onclick=async function(){if(!confirm('確定清空全部人臉？'))return;var j=await api('/api/rt7/faces/reset');log(j);load()};load();
</script></body></html>`));

// ---------- Original RT7 mobile-style cloud doorbell UI ----------
app.get('/rt7_cloud_original_ui_doorbell', (req, res) => {
  const q = req.query || {};
  const mode = safeString(q.mode || 'idle').toLowerCase();
  const ip = safeString(q.ip || '192.168.0.179').replace(/[^0-9.]/g, '') || '192.168.0.179';
  const aiOn = q.face === '1' || cloudState.face_gate_auto_enabled === true;
  const doorLast = doorbellState.last || null;
  const doorText = doorLast && doorLast.time ? ('最後：' + new Date(doorLast.time).toLocaleTimeString('zh-TW')) : '等待事件';
  let modeLabel = mode === 'lan' ? 'LAN' : (mode === 'cloud' ? 'CLOUD' : (mode === 'auto' ? 'AUTO' : 'AUTO'));
  let answer = mode === 'idle' ? '雲端門鈴待機中' : '自動判斷影像來源中';
  let hint = mode === 'idle' ? '等待影像串流' : '自動判斷：內網直連 / Railway 雲端';
  res.type('html').send(`<!doctype html><html lang="zh-Hant"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<title>RT7 Cloud Original UI V5.8D8B Push Green</title>
<style>
:root{--dark:#0b252b;--dark2:#0d2c32;--red:#ef2b24;--blue:#17a8e5;--green:#22a951;--text:#17262a;--line:#e5e7eb;--orange:#9a3b18}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent} html,body{margin:0;padding:0;background:#fff;color:var(--text);font-family:system-ui,-apple-system,"Noto Sans TC","Microsoft JhengHei",Arial,sans-serif} body{max-width:520px;margin:0 auto;min-height:100vh;padding-bottom:28px}
a,button,input,select{pointer-events:auto!important;touch-action:manipulation!important}.noTouch,.video img,.emptyVideo,.badge{pointer-events:none!important}
.top{height:66px;background:linear-gradient(90deg,var(--dark),var(--dark2));color:#fff;display:flex;align-items:center;justify-content:space-between;padding:0 16px;font-weight:900}.hamb{font-size:34px}.title{text-align:center;line-height:1.15;font-size:17px;letter-spacing:.4px}.spacer{width:34px}
.deviceBar{padding:8px 12px;background:#fff;border-bottom:1px solid var(--line)}.deviceText{height:42px;border:1px solid #334155;border-radius:8px;font-weight:900;padding:0 10px;background:#fff;font-size:17px;display:flex;align-items:center;justify-content:space-between;color:#111827}.deviceText select{border:0;background:#fff;font:inherit;font-weight:900;width:100%;outline:0}
.video{position:relative;background:#000;aspect-ratio:4/3;overflow:hidden}.video img{width:100%;height:100%;object-fit:cover;background:#000;display:block;border:0}.emptyVideo{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;text-align:center;color:#cbd5e1;font-weight:900;font-size:18px;line-height:1.45;padding:12px}.badge{position:absolute;top:12px;border-radius:7px;padding:7px 12px;color:white;font-weight:900;box-shadow:0 2px 8px rgba(0,0,0,.22)}.idle{left:14px;background:#71839d}.idle.aiOn{background:#16a34a}.live{right:14px;background:var(--red)}
.videoBtns{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:6px;background:#fff;padding:6px 8px;border-bottom:1px solid var(--line);align-items:center}.wakePanel{display:grid;grid-template-columns:1fr 1fr;gap:8px;padding:6px 8px;border-bottom:1px solid var(--line);background:#fff}.wakeBtn{border:0;border-radius:8px;color:#fff;background:#16a34a;font-weight:900;font-size:14px;height:38px}.wakeHint{font-size:12px;color:#64748b;font-weight:800;display:flex;align-items:center}.vbtn{display:flex;align-items:center;justify-content:center;border:0;border-radius:8px;color:#fff;font-weight:900;padding:8px 3px;font-size:13px;line-height:1;min-width:0;width:100%;height:38px;text-decoration:none;white-space:nowrap;overflow:hidden}.vblue{background:var(--blue)}.vred{background:var(--red)}.vdark{background:#102a31}.vorange{background:#f59e0b}
.statusLine{min-height:46px;display:grid;grid-template-columns:1fr 1fr;gap:8px;border-bottom:1px solid var(--line);align-items:center;padding:8px 12px;background:#fff;font-size:15px;font-weight:800}.faceSnapBox{display:none;border-bottom:1px solid var(--line);padding:8px 12px;background:#fff}.faceSnapTitle{font-weight:900;color:#0f172a;margin-bottom:6px}.faceSnapBox img{width:128px;max-width:40%;border:2px solid #cbd5e1;border-radius:8px;background:#000;vertical-align:top}.faceSnapMeta{display:inline-block;vertical-align:top;margin-left:10px;font-size:12px;font-weight:900;color:#5b1f14;line-height:1.5;max-width:55%;word-break:break-all}.dot{display:inline-block;width:11px;height:11px;border-radius:50%;background:var(--green);margin-right:8px}.answer{color:#5b1f14;white-space:pre-line}.door{color:#8a2f15;text-align:right}.door.bellNow{color:#9a3412;font-weight:900}.doorAlert{display:none!important}
.micZone{text-align:center;padding:18px 0 8px}.bigMic{width:128px;height:128px;border-radius:50%;border:3px solid #cbd5e1;background:#eef2f7;display:inline-flex;align-items:center;justify-content:center;font-size:72px;box-shadow:0 4px 18px rgba(20,40,60,.08);text-decoration:none;color:#24333a}
.actions{display:flex;justify-content:center;gap:6px;padding:10px 4px 4px}.act{width:60px;text-align:center;font-size:12px;font-weight:900;color:#24333a}.circle{width:52px;height:52px;border:3px solid var(--red);border-radius:50%;background:#fff;display:flex;align-items:center;justify-content:center;font-size:28px;margin:0 auto 4px;box-shadow:0 2px 10px rgba(0,0,0,.1);text-decoration:none;color:#24333a}.circle.pushEnabled{border-color:#22c55e;background:#ecfdf5;box-shadow:0 0 0 4px rgba(34,197,94,.18)}.circle.pushWarn{border-color:#f59e0b;background:#fffbeb}.circle.pushErr{border-color:#dc2626;background:#fff1f2}.circle.aiActive{border-color:#22c55e;background:#ecfdf5}.circle.talking{border-color:#ef4444;background:#fff1f2;box-shadow:0 0 0 4px rgba(239,68,68,.18)}.reg{display:flex;align-items:center;gap:10px;padding:8px 20px}.reg label{font-size:14px;font-weight:900}.reg input{flex:1;height:36px;border:1px solid #cbd5e1;border-radius:7px;padding:0 10px;font-size:16px}.small{font-size:12px;color:#64748b}.debug{display:none!important}
.selfiePanel{display:none;position:fixed;inset:0;background:rgba(0,0,0,.86);z-index:9999;padding:14px;color:#fff;overflow:auto}.selfieCard{max-width:500px;margin:0 auto;background:#0b252b;border-radius:16px;padding:14px;box-shadow:0 8px 28px rgba(0,0,0,.45)}.selfieTitle{font-size:20px;font-weight:900;margin:4px 0 10px;text-align:center}.selfieVideo{width:100%;background:#000;border-radius:12px;border:2px solid #334155;aspect-ratio:3/4;object-fit:cover}.selfieBtns{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:12px}.selfieBtns button{border:0;border-radius:10px;height:46px;font-size:17px;font-weight:900}.selfieShot{background:#22c55e;color:#fff}.selfieCancel{background:#ef4444;color:#fff}.selfieHint{font-size:13px;color:#dbeafe;line-height:1.45;margin-top:8px;text-align:center}
@media(max-height:740px){.top{height:56px}.videoBtns{gap:4px;padding:5px 6px}.vbtn{height:34px;font-size:12px;padding:7px 2px}.title{font-size:15px}.video{aspect-ratio:16/9}.bigMic{width:104px;height:104px;font-size:58px}.circle{width:50px;height:50px;font-size:24px}.act{font-size:11px}.statusLine{font-size:13px;min-height:38px}.reg{padding-top:4px}}
</style></head><body>
<header class="top"><a class="hamb" href="/rt7_gpio_control" style="color:#fff;text-decoration:none">☰</a><div class="title">RT7 PHASE10<br>AI MODE ROUTER</div><a class="spacer" href="/rt7_gpio_control" style="color:#fff;text-decoration:none;font-size:13px;font-weight:900">GPIO</a></header>
<div class="deviceBar"><div class="deviceText"><select id="deviceSel"><option value="${ip}">#1 / RT7 ESP32-S3-CAM / ${ip}</option></select></div></div>
<section class="video"><div id="emptyVideo" class="emptyVideo">${hint}<br><span class="small">網內使用 ESP32 直連；網外使用 Railway 雲端</span></div><img id="stream" alt=""><div id="aiBadge" class="badge idle ${aiOn?'aiOn':''}">${aiOn?'FACE_ENABLE':'IDLE'}</div><div id="streamModeBadge" class="badge live">${modeLabel}</div></section>
<section class="videoBtns"><button id="btnAiOn" class="vbtn vblue" type="button">啟用人臉</button><button id="btnAiOff" class="vbtn vred" type="button">關閉人臉</button><button id="btnAudio" class="vbtn vorange" type="button">啟用提示音</button><button id="btnStart" class="vbtn vdark" type="button">開始影像</button><button id="btnStop" class="vbtn vdark" type="button">停止影像</button></section>
<section class="statusLine"><div class="answer"><span class="dot"></span>回答：<span id="answerText">${answer}</span></div><div class="door">門鈴：<span id="doorText">${doorText}</span></div><div id="doorAlert" class="doorAlert">🔔 有人按門鈴</div></section>

<section class="micZone"><button id="btnVoice" class="bigMic" type="button">🎙️</button></section>
<section class="actions"><div class="act"><button id="btnOpenDoor" class="circle" type="button">🚪</button>開門</div><div class="act"><button id="btnFaceList" class="circle" type="button">👥</button>名單</div><div class="act"><button id="btnEndTalk" class="circle" type="button">◼</button>對講</div><div class="act"><button id="btnFaceEnroll" class="circle" type="button">＋</button>註冊</div><div class="act"><button id="btnWakeXiaoAi" class="circle" type="button">🎙️</button><span id="lblWakeXiaoAi">小艾</span></div><div class="act"><button id="btnPushNotify" class="circle pushWarn" type="button" title="通知設定" onclick="location.href='/rt7_push_enable'; return false;">🔔</button><span id="lblPushNotify">通知</span></div></section>
<div class="reg"><label>註冊名稱</label><input id="regName" value="gwansyan"></div>
<div id="selfiePanel" class="selfiePanel"><div class="selfieCard"><div class="selfieTitle">手機前鏡頭人臉註冊</div><video id="selfieVideo" class="selfieVideo" playsinline autoplay muted></video><canvas id="selfieCanvas" style="display:none"></canvas><div class="selfieHint">請讓臉在畫面中央，光線充足後按「拍照註冊」。辨識仍使用 ESP32-CAM，只有註冊照片改用手機前鏡頭。</div><div class="selfieBtns"><button id="btnSelfieCapture" class="selfieShot" type="button">拍照註冊</button><button id="btnSelfieCancel" class="selfieCancel" type="button">取消</button></div></div></div>
<script>
(function(){
  var ip=${JSON.stringify(ip)}; var mode=${JSON.stringify(mode)}; var selectedDeviceId='#1'; var ai=false; try{ ai=(localStorage.getItem('RT7_FACE_MODE')==='1'); selectedDeviceId=localStorage.getItem('RT7_CURRENT_DEVICE_ID')||'#1'; }catch(e){ ai=${aiOn?'true':'false'}; } var img=document.getElementById('stream'); var empty=document.getElementById('emptyVideo'); var badge=document.getElementById('streamModeBadge'); var answer=document.getElementById('answerText'); var debug=null; var audioCtx=null; var audioOK=false; var audioTried=false;
  // V5.4W: FACE_GATE is default OFF and persistent. UI state alone must never re-enable it.
  setTimeout(function(){ setAiUi(ai); rt7FaceGateEspEnable(ai, true); }, 600);
  function setAnswer(t){ if(answer) answer.textContent=t; }
  function setDoorText(t, bell){ var d=document.getElementById('doorText'); var box=d?d.closest('.door'):null; if(d)d.textContent=t; if(box){ if(bell) box.classList.add('bellNow'); else box.classList.remove('bellNow'); } }
  function showDoorbellInline(){ setDoorText('⚠️ 有人按門鈴', true); setAnswer('收到門鈴提示音'); playDingdong(); setTimeout(function(){ setDoorText('最後：'+new Date().toLocaleTimeString('zh-TW'), false); }, 8000); }
  function setDebug(t){ /* V5.0E: hidden debug; no UI repaint */ }
  function rt7Esc(s){ return String(s||'').replace(/[&<>"']/g,function(m){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m];}); }
  function rt7CleanHost(v){
    v=String(v||'').trim();
    var low=v.toLowerCase();
    if(low.indexOf('http://')===0) v=v.slice(7);
    else if(low.indexOf('https://')===0) v=v.slice(8);
    var slash=v.indexOf('/');
    if(slash>=0) v=v.slice(0,slash);
    return v.trim();
  }
  async function rt7LoadDeviceSelector(){
    try{
      // V5.6C: mobile page is bound to Device Manager. Use /api/devices as source of truth.
      var d=null;
      try{
        var r=await fetch('/api/devices?_='+Date.now(),{cache:'no-store'});
        d=await r.json();
      }catch(e1){
        var r2=await fetch('/api/rt7/devices/list?_='+Date.now(),{cache:'no-store'});
        d=await r2.json();
      }
      var list=(d.devices||[]).filter(function(x){return x && x.enabled!==false;});
      if(!list.length) list=[{id:'#1',name:'RT7 ESP32-S3-CAM',ip:ip,enabled:true}];
      var sel=document.getElementById('deviceSel'); if(!sel) return;
      sel.innerHTML=list.map(function(x){
        var id=x.id||'#1'; var name=x.name||'RT7'; var dip=rt7CleanHost(x.ip||'');
        return '<option value="'+rt7Esc(id)+'" data-ip="'+rt7Esc(dip)+'" data-name="'+rt7Esc(name)+'">'+rt7Esc(id+' / '+name+(dip?' / '+dip:''))+'</option>';
      }).join('');
      var want=selectedDeviceId||d.current_device_id||'#1'; sel.value=want; if(sel.value!==want) sel.selectedIndex=0;
      rt7ApplySelectedDevice(false);
      setDebug('devices loaded count='+list.length+' selected='+selectedDeviceId+' ip='+ip);
    }catch(e){ setDebug('device load failed '+(e.message||e)); }
  }
  function rt7ApplySelectedDevice(save){
    var sel=document.getElementById('deviceSel'); if(!sel || !sel.options.length) return;
    var opt=sel.options[sel.selectedIndex]; selectedDeviceId=sel.value||'#1';
    var nextIp=rt7CleanHost(opt&&opt.getAttribute('data-ip')||''); if(nextIp) ip=nextIp;
    var nextName=(opt&&opt.getAttribute('data-name')||'').trim();
    try{localStorage.setItem('RT7_CURRENT_DEVICE_ID',selectedDeviceId); localStorage.setItem('RT7_CURRENT_DEVICE_IP',ip); localStorage.setItem('RT7_CURRENT_DEVICE_NAME',nextName);}catch(e){}
    if(save){
      fetch('/api/rt7/device/set',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({device_id:selectedDeviceId})}).catch(function(){});
      setAnswer('已切換設備 '+selectedDeviceId+(nextName?' '+nextName:'')+(ip?' / '+ip:''));
    }
  }
  setTimeout(function(){ var sel=document.getElementById('deviceSel'); if(sel){sel.addEventListener('change',function(){rt7ApplySelectedDevice(true); stopVideo(); setAnswer('已切換設備 '+selectedDeviceId+' / '+ip+'，請按開始影像');});} rt7LoadDeviceSelector(); }, 50);
  function tone(freq, delay, dur){ if(!audioCtx) return; try{ setTimeout(function(){ var o=audioCtx.createOscillator(); var g=audioCtx.createGain(); o.frequency.value=freq; g.gain.value=0.22; o.connect(g); g.connect(audioCtx.destination); o.start(); setTimeout(function(){try{o.stop()}catch(e){}}, dur); }, delay); }catch(e){} }
  function playDingdong(){ if(!audioOK) return; tone(880,0,180); tone(660,260,220); }
  async function enableDoorbellAudio(){ try{ audioCtx = audioCtx || new (window.AudioContext||window.webkitAudioContext)(); await audioCtx.resume(); audioOK=true; audioTried=true; setAnswer('門鈴提示音已啟用'); setDebug('audio enabled'); playDingdong(); return true; }catch(e){ setAnswer('提示音啟用失敗：'+(e.message||e)); setDebug('audio failed'); return false; } }
  function tryUnlockAudioSilently(){ if(audioOK || audioTried) return; audioTried=true; try{ audioCtx = audioCtx || new (window.AudioContext||window.webkitAudioContext)(); audioCtx.resume().then(function(){ audioOK=true; setDebug('audio auto-unlocked by touch'); }).catch(function(){ audioTried=false; }); }catch(e){ audioTried=false; } }
  document.addEventListener('touchend', tryUnlockAudioSilently, {once:true, passive:true});
  document.addEventListener('click', tryUnlockAudioSilently, {once:true, passive:true});
  var videoWanted=false; var currentStreamMode='IDLE'; var lanReconnectTimer=null; var lanRetryCount=0; var lanProbeDone=false;
  function clearLanReconnect(){ if(lanReconnectTimer){ clearTimeout(lanReconnectTimer); lanReconnectTimer=null; } }
  function stopVideo(){ videoWanted=false; currentStreamMode='IDLE'; try{localStorage.setItem('RT7_V50_WANTED_VIDEO','0');localStorage.setItem('RT7_V50_STREAM_MODE','IDLE');}catch(e){} clearLanReconnect(); if(img){ img.onerror=null; img.onload=null; try{ img.src='about:blank'; }catch(e){} img.removeAttribute('src'); } if(badge) badge.textContent='AUTO'; if(empty) empty.innerHTML='等待影像串流<br><span class="small">自動判斷：內網直連 / Railway 雲端</span>'; setAnswer('雲端門鈴待機中'); setDebug('stop video'); }
  function cloud(){ videoWanted=true; currentStreamMode='CLOUD'; try{localStorage.setItem('RT7_V50_WANTED_VIDEO','1');localStorage.setItem('RT7_V50_STREAM_MODE','CLOUD');}catch(e){} clearLanReconnect(); if(badge) badge.textContent='CLOUD'; if(empty) empty.innerHTML='Railway 雲端遠端影像<br><span class="small">外網或內網偵測失敗，自動切換</span>'; if(img){ img.onerror=function(){ if(!videoWanted || currentStreamMode!=='CLOUD') return; setAnswer('雲端影像暫停，5 秒後重連'); clearLanReconnect(); lanReconnectTimer=setTimeout(function(){ if(videoWanted && currentStreamMode==='CLOUD'){ img.src='/api/rt7/camera/stream.mjpg?device_id='+encodeURIComponent(selectedDeviceId||'#1')+'&_cloud_re='+Date.now(); } },5000); }; img.onload=function(){ setDebug('cloud mjpeg loaded'); }; img.src='/api/rt7/camera/stream.mjpg?device_id='+encodeURIComponent(selectedDeviceId||'#1')+'&_cloud='+Date.now(); } setAnswer('雲端遠端影像模式'); setDebug('cloud stream'); }
  function lan(){ videoWanted=true; currentStreamMode='LAN'; try{localStorage.setItem('RT7_V50_WANTED_VIDEO','1');localStorage.setItem('RT7_V50_STREAM_MODE','LAN');}catch(e){} clearLanReconnect(); lanRetryCount=0; if(badge) badge.textContent='LAN'; if(empty) empty.innerHTML='內網直連 ESP32 流暢影像<br><span class="small">'+ip+'</span>'; if(img){ img.style.backgroundImage='url("/api/rt7/camera/latest.jpg?device_id='+encodeURIComponent(selectedDeviceId||'#1')+'&_hold='+Date.now()+'")'; img.style.backgroundSize='cover'; img.style.backgroundPosition='center'; img.onerror=function(){ if(!videoWanted || currentStreamMode!=='LAN') return; lanRetryCount++; setAnswer('LAN 串流暫停，5 秒後重連（保留畫面，不清空黑屏）'); setDebug('lan onerror retry='+lanRetryCount); clearLanReconnect(); lanReconnectTimer=setTimeout(function(){ if(!videoWanted || currentStreamMode!=='LAN') return; // Do NOT clear img.src here. Clearing src causes Android Chrome black screen. Replace source directly.
        var next='http://'+ip+'/api/camera/stream?_lan_re='+Date.now(); try{ img.src=next; }catch(e){} },5000); }; img.onload=function(){ lanRetryCount=0; setDebug('lan mjpeg loaded'); }; var first='http://'+ip+'/api/camera/stream?_lan='+Date.now(); try{ img.src=first; }catch(e){} } setAnswer('內網直連影像模式'); setDebug('lan stream '+ip); }
  function startAuto(){ try{localStorage.setItem('RT7_V50_WANTED_VIDEO','1');localStorage.setItem('RT7_V50_STREAM_MODE','AUTO');}catch(e){} if(videoWanted && (currentStreamMode==='LAN' || currentStreamMode==='CLOUD')){ setAnswer(currentStreamMode==='LAN'?'內網直連影像模式':'雲端遠端影像模式'); return; } videoWanted=true; currentStreamMode='AUTO'; clearLanReconnect(); setAnswer('自動判斷影像來源中'); if(badge) badge.textContent='AUTO'; if(empty) empty.innerHTML='自動判斷中：先用單張 snapshot 測內網，成功才開啟 LAN 串流'; var probe=new Image(); var done=false; var t=setTimeout(function(){ if(done||!videoWanted)return; done=true; try{probe.src='about:blank'}catch(e){} cloud(); },1800); probe.onload=function(){ if(done||!videoWanted)return; done=true; clearTimeout(t); try{probe.src='about:blank'}catch(e){} lan(); }; probe.onerror=function(){ if(done||!videoWanted)return; done=true; clearTimeout(t); try{probe.src='about:blank'}catch(e){} cloud(); }; probe.src='http://'+ip+'/api/camera/snapshot?_probe_once='+Date.now(); }
  async function j(url,opt){ var r=await fetch(url+(url.indexOf('?')>=0?'&':'?')+'_='+Date.now(), Object.assign({cache:'no-store'}, opt||{})); var tx=await r.text(); try{return JSON.parse(tx)}catch(e){return{ok:r.ok,status:r.status,raw:tx}} }
  function rt7EspImgBeacon(path){
    var url='http://'+ip+path+(path.indexOf('?')>=0?'&':'?')+'_='+Date.now();
    try{ var im=new Image(); im.onload=function(){}; im.onerror=function(){}; im.src=url; }catch(e){}
    try{ fetch(url,{mode:'no-cors',cache:'no-store',keepalive:true}).catch(function(){}); }catch(e){}
  }
  function rt7EspFast8081(path){
    var url='http://'+ip+':8081'+path+(path.indexOf('?')>=0?'&':'?')+'_='+Date.now();
    try{ var im=new Image(); im.onload=function(){}; im.onerror=function(){}; im.src=url; }catch(e){}
    try{ fetch(url,{mode:'no-cors',cache:'no-store',keepalive:true}).catch(function(){}); }catch(e){}
  }
  function rt7FaceGateEspEnable(on, silent){
    try{ localStorage.setItem('RT7_FACE_MODE', on ? '1' : '0'); }catch(e){}
    if(on){
      // V5.4X: use fast 8081 control first. Port 80 may be busy when LAN MJPEG is open.
      rt7EspFast8081('/api/motion/enable_fast');
      rt7EspFast8081('/api/face_gate/on');
      rt7EspImgBeacon('/api/motion/config?threshold=1500&cooldown=8000&warmup=1000&face_gate=1&face_threshold=2100&face_min_jpeg=3800&face_center_min_jpeg=3800&face_center_min_motion=1800&face_center_min_candidate=2100&step=31');
      rt7EspImgBeacon('/api/motion/enable');
      setTimeout(function(){
        rt7EspFast8081('/api/motion/enable_fast');
        rt7EspFast8081('/api/face_gate/on');
      }, 250);
      try{ rt7Json('/api/rt7/face_gate/auto?mode=on',{method:'POST'}); }catch(_){ }
      try{ rt7Json('/api/rt7/face_gate/toggle?mode=on',{method:'POST'}); }catch(_){ }
    } else {
      // V5.4X: hard OFF through fast 8081 first, then fallback port 80.
      rt7EspFast8081('/api/motion/disable_fast');
      rt7EspFast8081('/api/face_gate/off');
      rt7EspImgBeacon('/api/motion/disable');
      rt7EspImgBeacon('/api/motion/config?enabled=0&face_gate=0');
      setTimeout(function(){
        rt7EspFast8081('/api/motion/disable_fast');
        rt7EspFast8081('/api/face_gate/off');
        rt7EspImgBeacon('/api/motion/disable');
        rt7EspImgBeacon('/api/motion/config?enabled=0&face_gate=0');
      }, 250);
      setTimeout(function(){
        rt7EspFast8081('/api/motion/disable_fast');
        rt7EspFast8081('/api/face_gate/off');
      }, 900);
      try{ rt7Json('/api/rt7/face_gate/auto?mode=off',{method:'POST'}); }catch(_){ }
      try{ rt7Json('/api/rt7/face_gate/toggle?mode=off',{method:'POST'}); }catch(_){ }
      rt7AutoFaceLastKey='';
    }
  }
  function bind(id,fn){ var el=document.getElementById(id); if(el) el.addEventListener('click', function(ev){ ev.preventDefault(); ev.stopPropagation(); fn(); }, false); }
  bind('btnStart', startAuto); bind('btnStop', stopVideo); bind('btnAudio', enableDoorbellAudio);
  bind('btnAiOn', async function(){ setAiUi(true,'人臉辨識已啟用：靠近鏡頭會自動辨識'); rt7FaceGateEspEnable(true); setDebug('face mode on'); });
  bind('btnAiOff', async function(){ setAiUi(false,'人臉辨識已關閉'); rt7FaceGateEspEnable(false); setDebug('face mode off'); });
  function rt7DoorOpenBeacon_(url, tag){
    try{
      var im=new Image();
      im.onload=function(){ setDebug('door beacon ok '+tag); };
      im.onerror=function(){ setDebug('door beacon sent '+tag); };
      im.src=url+(url.indexOf('?')>=0?'&':'?')+'_door='+Date.now()+'&tag='+encodeURIComponent(tag||'mobile');
      return true;
    }catch(e){ setDebug('door beacon failed '+tag+' '+(e.message||e)); return false; }
  }
  var rt7DoorLastTapMs=0;
  async function rt7OpenDoorFastMain_(){
    var now=Date.now();
    if(now-rt7DoorLastTapMs<900) return;
    rt7DoorLastTapMs=now;
    var host=rt7CleanHost(ip);
    setAnswer('開門命令已送出：內網快速 + 雲端單次直送');
    // V5.6G6: send one Railway command first by HTTPS-relative URL. This is the outer-network path.
    // It is not awaited, so LAN opening still stays instant.
    function rt7SendCloudDoorQueue_(){
      var cloudUrl=(location.origin||'')+'/api/rt7/door/open?device_id='+encodeURIComponent(selectedDeviceId||'#1')+'&source=main_button_cloud_first&cloud_required=1&_='+Date.now();
      var relUrl='/api/rt7/door/open?device_id='+encodeURIComponent(selectedDeviceId||'#1')+'&source=main_button_cloud_first_rel&cloud_required=1&_='+Date.now();
      // V5.6G6: send exactly ONE cloud command per tap.  Do not use image beacons
      // for the Railway queue because on mobile Chrome they create duplicate GETs.
      try{
        fetch(relUrl,{method:'GET',cache:'no-store',keepalive:true,credentials:'same-origin'})
          .then(function(r){ return r.text(); })
          .then(function(tx){
            try{
              var j=JSON.parse(tx);
              if(j&&j.ok){
                setDebug('cloud door queued '+(j.normalized_device_id||''));
                setAnswer('開門命令已送出：內網快速 + 雲端單次直送');
              }
            }catch(e){}
          })
          .catch(function(){ setDebug('cloud door queue fetch failed'); });
      }catch(e){ setDebug('cloud door queue exception '+(e.message||e)); }
    }
    // V5.6G8: IMPORTANT - do NOT queue cloud door command while the phone is on LAN.
    // In G7, LAN tap opened immediately via 8081 and also queued cloud commands.
    // Those queued commands were later delivered by WebSocket when the page was closed,
    // causing 2~3 unexpected GPIO pulses.
    var isLanMode=false;
    try{
      isLanMode = (currentStreamMode==='LAN') || (badge && String(badge.textContent||'').toUpperCase().indexOf('LAN')>=0);
    }catch(e){ isLanMode=false; }

    if(host && isLanMode){
      // LAN: single fast request only.  No Railway queue, no WS direct.
      rt7DoorOpenBeacon_('http://'+host+':8081/api/door/open_fast','lan8081_open_fast_only_no_cloud');
      setAnswer('內網快速開門命令已送出');
    } else {
      // WAN/CLOUD: Railway queue + WebSocket direct fallback.
      rt7SendCloudDoorQueue_();
      setAnswer('雲端開門命令已送出，等待 ESP32 執行');
    }
  }
  bind('btnOpenDoor', rt7OpenDoorFastMain_);
  function speakAnswer(txt){
    return new Promise(function(resolve){
      txt=String(txt||'');
      if(!window.speechSynthesis || !txt.length){ resolve(); return; }
      try{
        // V5.6L5: do not cut the last sentence.  Android Chrome zh-TW TTS can be
        // slower than the old txt.length*180ms watchdog, especially after wake-word
        // continuous mode pauses/resumes recognition.  Use a much longer watchdog
        // and only cancel on real error, not during normal long answers.
        try{ speechSynthesis.cancel(); }catch(_e){}
        var u=new SpeechSynthesisUtterance(txt);
        u.lang='zh-TW';
        u.rate=0.96;
        u.pitch=1.0;
        u.volume=1.0;
        var done=false;
        var lastProgress=Date.now();
        var textLen=txt.replace(/\s+/g,'').length || txt.length || 1;
        var timeoutMs=Math.min(120000, Math.max(15000, textLen*520));
        var progressTimer=null;
        function finish(){
          if(done) return;
          done=true;
          try{ if(progressTimer) clearInterval(progressTimer); }catch(_e){}
          resolve();
        }
        u.onstart=function(){ lastProgress=Date.now(); };
        u.onboundary=function(){ lastProgress=Date.now(); };
        u.onmark=function(){ lastProgress=Date.now(); };
        u.onend=finish;
        u.onerror=function(ev){
          // If the browser reports interrupted/canceled because of a new speak(), end safely.
          // Do not forcibly restart here; routeVoiceQuestion will finish and wake-word will resume.
          finish();
        };
        progressTimer=setInterval(function(){
          if(done) return;
          var elapsed=Date.now()-(lastProgress||Date.now());
          // Safety only: if speechSynthesis hangs for a long time, resolve without cutting early.
          if(elapsed>timeoutMs){
            try{ speechSynthesis.cancel(); }catch(_e){}
            finish();
          }
        },1000);
        speechSynthesis.speak(u);
      }catch(e){ resolve(); }
    });
  }
  function setAiUi(on, msg){
    // V5.4L: this top button controls FACE_GATE auto recognition only, not AI voice.
    ai=!!on; try{localStorage.setItem('RT7_FACE_MODE', ai?'1':'0');}catch(e){}
    var b=document.getElementById('aiBadge');
    if(b){ b.textContent=ai?'FACE_ENABLE':'IDLE'; if(ai)b.classList.add('aiOn'); else b.classList.remove('aiOn'); }
    if(msg) setAnswer(msg);
  }
  function rt7ParseMusicQuery(text){
    text=(text||'').trim();
    var m=text.match(/^(?:請)?(?:幫我)?(?:播放|放|聽|我要聽|想聽)\s*(.+)$/i);
    if(!m) m=text.match(/(?:播放|放|聽)\s*(.+)$/i);
    if(!m) return '';
    var q=(m[1]||'').trim();
    q=q.replace(/[。！!？?，,]$/g,'').trim();
    return q;
  }
  async function rt7MobileMusicPlay(query){
    query=(query||'').trim();
    if(!query){ setAnswer('請說：播放 歌曲名稱'); return; }
    setAnswer('手機播放音樂：'+query+'，正在搜尋第一個 YouTube 影片...');
    setDebug('mobile music first video '+query);
    var searchUrl='https://www.youtube.com/results?search_query='+encodeURIComponent(query);
    var url=searchUrl;
    try{
      var r=await j('/api/rt7/music/mobile?q='+encodeURIComponent(query)+'&mode=watch');
      if(r && r.watch_url) url=r.watch_url;
      else if(r && r.url) url=r.url;
    }catch(e){
      setDebug('music first video fallback search '+e.message);
    }
    var vid='';
    try{ var mm=String(url||'').match(/[?&]v=([a-zA-Z0-9_-]{11})/); if(mm) vid=mm[1]; }catch(e){}
    if(vid){
      var playerUrl='/rt7_music_player?video_id='+encodeURIComponent(vid)+'&q='+encodeURIComponent(query)+'&return='+encodeURIComponent('/rt7_return_doorbell?from=music');
      setAnswer('已找到第一個 YouTube 影片，進入 RT7 音樂播放器。播放結束會自動返回門禁頁。');
      try{ location.href=playerUrl; }catch(e){ window.open(playerUrl,'_blank'); }
    }else{
      setAnswer('無法取得第一個影片，改開 YouTube 搜尋：'+query);
      try{ location.href=url; }catch(e){ window.open(url,'_blank'); }
    }
  }
  async function routeVoiceQuestion(text){
    text=(text||'').trim();
    if(!text){ setAnswer('沒有收到語音內容，請再按一次 AI語音助理後說話'); setDebug('voice empty'); return; }
    var mq=rt7ParseMusicQuery(text);
    if(mq){ await rt7MobileMusicPlay(mq); return; }
    setAnswer('你說：'+text+'，AI 分析中...');
    setDebug('voice question: '+text);
    try{
      var r=await j('/api/rt7/phase9j/voice_vision',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({text:text,mode:'auto'})});
      var ans=r.answer||r.error||'AI 無回應';
      setAnswer(ans);
      await speakAnswer(ans);
      setDebug('voice_vision ok');
    }catch(e){
      setAnswer('AI語音助理失敗：'+e.message);
      setDebug('voice_vision failed');
    }finally{
      // V5.4W: AI voice must not change FACE_GATE mode.
    }
  }
  function startVoiceAsk(){ setAnswer('請開始說話'); var SR=window.SpeechRecognition||window.webkitSpeechRecognition; if(!SR){ var t=prompt('請輸入要問 AI語音助理的內容：','')||''; routeVoiceQuestion(t); return; } try{ var rec=new SR(); rec.lang='zh-TW'; rec.continuous=false; rec.interimResults=false; rec.maxAlternatives=1; setAnswer('請開始說話'); setDebug('speech recognition start'); rec.onresult=function(ev){ var text=''; try{text=ev.results[0][0].transcript||'';}catch(e){} routeVoiceQuestion(text); }; rec.onerror=function(ev){ setAnswer('語音辨識失敗：'+(ev.error||'unknown')+'。請再按一次 AI語音助理。'); setDebug('speech error '+(ev.error||'')); }; rec.onend=function(){ setDebug('speech recognition end'); }; rec.start(); }catch(e){ var t2=prompt('語音辨識無法啟動，請輸入問題：','')||''; routeVoiceQuestion(t2); } }
  bind('btnAiVoice', startVoiceAsk); // btnVoice 是中央對講按鍵，不再啟動 AI 語音助理

  // V5.6L7: explicit XiaoAi start button.
  // Android Chrome needs a clear user gesture to open microphone; do not auto-start on arbitrary page tap.
  // Press 啟用小艾待命 once, then say 小艾 to enter AI session; 30s idle returns to wake waiting.
  var rt7WakeEnabled=false;
  var rt7WakeAutoArmed=false;
  var rt7WakeRec=null;
  var rt7WakeLastMs=0;
  var rt7WakeStopTimer=null;
  var rt7WakeSession=false;
  var rt7WakeBusy=false;
  var rt7WakeLastText='';
  var rt7WakeLastTextMs=0;
  var rt7WakePauseForAi=false;
  var rt7WakeResumeTimer=null;
  var rt7WakeRestarting=false;
  var rt7WakeAckPending=false;
  function rt7WakeButtonText_(txt){ var b=document.getElementById('btnWakeXiaoAi'); var l=document.getElementById('lblWakeXiaoAi'); if(b){ b.textContent='🎙️'; b.title=txt||'小艾'; } if(l){ var t=String(txt||'小艾'); if(t.indexOf('待命中')>=0) t='小艾待命'; else if(t.indexOf('聆聽')>=0) t='小艾聆聽'; else if(t.indexOf('不支援')>=0) t='不支援'; else if(t.indexOf('關閉')>=0) t='小艾'; else t='小艾'; l.textContent=t; } }
  function rt7WakeStop_(msg){
    rt7WakeEnabled=false;
    rt7WakeSession=false;
    rt7WakeBusy=false;
    rt7WakeRestarting=false;
    try{ if(rt7WakeRec){ rt7WakeRec.onresult=null; rt7WakeRec.onerror=null; rt7WakeRec.onend=null; rt7WakeRec.stop(); } }catch(e){}
    rt7WakeRec=null;
    if(rt7WakeStopTimer){ clearInterval(rt7WakeStopTimer); rt7WakeStopTimer=null; }
    if(rt7WakeResumeTimer){ clearTimeout(rt7WakeResumeTimer); rt7WakeResumeTimer=null; }
    rt7WakePauseForAi=false;
    rt7WakeAckPending=false;
    rt7WakeButtonText_('啟用小艾待命');
    if(msg) setAnswer(msg);
    setDebug('xiaoai wake stopped');
  }
  function rt7WakeReturnToWake_(){
    rt7WakeSession=false;
    rt7WakeBusy=false;
    rt7WakeAckPending=false;
    rt7WakeLastMs=Date.now();
    rt7WakeButtonText_('啟用小艾待命');
    setAnswer('小艾：已回到等待喚醒詞。請說「小艾」再開始詢問。');
    setDebug('xiaoai returned to wake-word waiting');
  }
  function rt7WakePauseRecForAi_(){
    rt7WakePauseForAi=true;
    if(rt7WakeResumeTimer){ clearTimeout(rt7WakeResumeTimer); rt7WakeResumeTimer=null; }
    try{ if(rt7WakeRec) rt7WakeRec.stop(); }catch(e){}
    setDebug('xiaoai mic paused while AI speaking');
  }
  function rt7WakeResumeRecAfterAi_(){
    if(rt7WakeResumeTimer){ clearTimeout(rt7WakeResumeTimer); }
    rt7WakeResumeTimer=setTimeout(function(){
      if(!rt7WakeEnabled) return;
      rt7WakePauseForAi=false;
      rt7WakeLastMs=Date.now();
      try{ if(rt7WakeRec) rt7WakeRec.start(); setDebug(rt7WakeSession?'xiaoai ready for next question':'xiaoai waiting wake word'); }catch(e){ setDebug('xiaoai resume failed '+(e.message||e)); }
    },1500);
  }
  function rt7WakeArmTimer_(){
    if(rt7WakeStopTimer) clearInterval(rt7WakeStopTimer);
    rt7WakeStopTimer=setInterval(function(){
      if(rt7WakeEnabled && rt7WakeSession && !rt7WakeBusy && !rt7WakePauseForAi && Date.now()-rt7WakeLastMs>30000){ rt7WakeReturnToWake_(); }
    },1000);
  }
  async function rt7WakeAsk_(cmd){
    cmd=String(cmd||'').trim();
    if(!cmd) return;
    rt7WakeLastMs=Date.now();
    rt7WakeBusy=true;
    rt7WakePauseRecForAi_();
    try{
      var needAck=!!rt7WakeAckPending;
      rt7WakeAckPending=false;
      if(needAck){
        setAnswer('小艾：我是小艾。'+(cmd ? ' 你說：'+cmd : ''));
        await speakAnswer('我是小艾');
      }
      setAnswer('小艾：'+cmd);
      await routeVoiceQuestion(cmd);
    }catch(e){
      setAnswer('小艾處理失敗：'+(e.message||e));
    }finally{
      rt7WakeBusy=false;
      if(rt7WakeEnabled){
        rt7WakeLastMs=Date.now();
        rt7WakeResumeRecAfterAi_();
      }
    }
  }
  function rt7WakeHandleText_(text){
    text=String(text||'').trim();
    if(!text) return;
    if(rt7WakeBusy || rt7WakePauseForAi){ setDebug('xiaoai ignored while AI speaking: '+text); return; }
    var now=Date.now();
    if(text===rt7WakeLastText && now-rt7WakeLastTextMs<1500) return;
    rt7WakeLastText=text;
    rt7WakeLastTextMs=now;
    setDebug('xiaoai heard '+text);
    var normalized=text.replace(/\s+/g,'');
    var hasWake=(normalized.indexOf('小艾')>=0 || normalized.indexOf('小愛')>=0);
    var cmd='';
    if(hasWake){
      rt7WakeSession=true;
      rt7WakeLastMs=now;
      rt7WakeAckPending=true;
      rt7WakeButtonText_('小艾聆聽中');
      try{
        var m=String(text||'').match(/小[艾愛][，,。！!？?\s]*(.*)$/);
        if(m) cmd=(m[1]||'').trim();
      }catch(e){ cmd=''; }
      if(!cmd){
        rt7WakeAckPending=false;
        rt7WakeBusy=true;
        rt7WakePauseRecForAi_();
        setAnswer('小艾：我是小艾。請直接說問題，30秒未說話會回到等待「小艾」。');
        speakAnswer('我是小艾').then(function(){
          rt7WakeBusy=false;
          if(rt7WakeEnabled){ rt7WakeLastMs=Date.now(); rt7WakeResumeRecAfterAi_(); }
        });
        return;
      }
    }else if(rt7WakeSession){
      rt7WakeLastMs=now;
      cmd=text;
    }else{
      return;
    }
    if(rt7WakeBusy){ setDebug('xiaoai busy, ignored '+cmd); return; }
    rt7WakeAsk_(cmd);
  }
  function rt7WakeCreateRecognizer_(){
    var SR=window.SpeechRecognition||window.webkitSpeechRecognition;
    if(!SR) return null;
    var rec=new SR();
    rec.lang='zh-TW';
    rec.continuous=true;
    rec.interimResults=false;
    rec.maxAlternatives=1;
    rec.onresult=function(ev){
      for(var i=ev.resultIndex||0;i<ev.results.length;i++){
        if(ev.results[i] && ev.results[i][0]) rt7WakeHandleText_(ev.results[i][0].transcript||'');
      }
    };
    rec.onerror=function(ev){ setDebug('xiaoai wake error '+(ev&&ev.error||'')); };
    rec.onend=function(){
      if(rt7WakeEnabled && !rt7WakePauseForAi && !rt7WakeRestarting){
        rt7WakeRestarting=true;
        setTimeout(function(){
          rt7WakeRestarting=false;
          try{ if(rt7WakeEnabled && !rt7WakePauseForAi && rt7WakeRec) rt7WakeRec.start(); }catch(e){ setDebug('xiaoai restart failed '+(e.message||e)); }
        },600);
      }
    };
    return rec;
  }
  function rt7WakeStart_(){
    if(rt7WakeEnabled) return;
    var SR=window.SpeechRecognition||window.webkitSpeechRecognition;
    if(!SR){ rt7WakeButtonText_('小艾不支援'); setDebug('SpeechRecognition unsupported'); return; }
    try{
      rt7WakeEnabled=true;
      rt7WakeSession=false;
      rt7WakeBusy=false;
      rt7WakePauseForAi=false;
      rt7WakeLastMs=Date.now();
      rt7WakeButtonText_('啟用小艾待命');
      rt7WakeArmTimer_();
      rt7WakeRec=rt7WakeCreateRecognizer_();
      if(!rt7WakeRec){ rt7WakeStop_('此瀏覽器不支援語音喚醒'); return; }
      try{
        rt7WakeRec.start();
        rt7WakeButtonText_('小艾待命中');
        setAnswer('小艾待命已啟用。請說「小艾」開始詢問，30秒未說話會回到等待小艾。');
        setDebug('xiaoai wake started by button');
      }catch(e){
        rt7WakeStop_('小艾待命需要麥克風權限；請按「啟用小艾待命」並允許麥克風。');
      }
    }catch(e){ rt7WakeStop_('小艾語音喚醒啟動失敗：'+(e.message||e)); }
  }
  function rt7WakeToggle_(){ if(rt7WakeEnabled) rt7WakeStop_('小艾待命已關閉'); else rt7WakeStart_(); }
  bind('btnWakeXiaoAi', rt7WakeToggle_);
  function rt7WakeAutoStartOnce_(){
    if(rt7WakeAutoArmed) return;
    rt7WakeAutoArmed=true;
    setTimeout(function(){ try{ rt7WakeStart_(); }catch(e){} },80);
  }
  document.addEventListener('DOMContentLoaded', function(){
    setTimeout(function(){ rt7WakeButtonText_('啟用小艾待命'); },200);
  });

  // V5.0K: 雙向 PTT WebSocket 對講。
  // 按住中央「對講」：手機 Mic -> ESP32 Speaker；放開：ESP32 Mic -> 手機 Speaker；按下方「◼ 對講」才結束。
  var rt7WsIc=null, rt7WsIcOn=false, rt7WsTxActive=false, rt7WsListenActive=false;
  var rt7WsMicStream=null, rt7WsMicCtx=null, rt7WsMicSource=null, rt7WsMicProc=null;
  var rt7WsTxBytes=[], rt7WsSent=0, rt7WsBeginMs=0, rt7WsListenTimer=null;
  var rt7RxAudioCtx=null, rt7RxPlayAt=0, rt7RxPackets=0, rt7RxBytes=0, rt7RxLastMs=0, rt7RxJitterMaxMs=0;
  async function rt7Json(url,opt){ var r=await fetch(url+(url.indexOf('?')>=0?'&':'?')+'_='+Date.now(),Object.assign({cache:'no-store'},opt||{})); var t=await r.text(); try{return JSON.parse(t)}catch(e){return{ok:r.ok,raw:t}} }
  var rt7SelfieStream=null;
  async function rt7OpenSelfieEnroll(){
    var name=(document.getElementById('regName')&&document.getElementById('regName').value||'').trim()||'未命名';
    var panel=document.getElementById('selfiePanel'); var video=document.getElementById('selfieVideo');
    try{
      if(!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia){
        setAnswer('此手機瀏覽器不支援相機註冊，請使用 Chrome / Safari 並允許相機權限。'); return;
      }
      setAnswer('開啟手機前鏡頭註冊：'+name);
      if(panel) panel.style.display='block';
      rt7SelfieStream=await navigator.mediaDevices.getUserMedia({video:{facingMode:'user',width:{ideal:720},height:{ideal:960}},audio:false});
      if(video){ video.srcObject=rt7SelfieStream; try{ await video.play(); }catch(e){} }
    }catch(e){
      if(panel) panel.style.display='none';
      setAnswer('手機相機開啟失敗：'+(e.message||e));
    }
  }
  function rt7CloseSelfieEnroll(){
    try{ if(rt7SelfieStream){ rt7SelfieStream.getTracks().forEach(function(t){try{t.stop();}catch(e){}}); } }catch(e){}
    rt7SelfieStream=null;
    var panel=document.getElementById('selfiePanel'); if(panel) panel.style.display='none';
  }
  async function rt7CaptureSelfieEnroll(){
    var name=(document.getElementById('regName')&&document.getElementById('regName').value||'').trim()||'未命名';
    var video=document.getElementById('selfieVideo'); var canvas=document.getElementById('selfieCanvas');
    try{
      if(!video || !canvas || !video.videoWidth){ setAnswer('手機鏡頭尚未準備好，請稍候再拍照。'); return; }
      var maxW=720; var vw=video.videoWidth; var vh=video.videoHeight; var scale=Math.min(1, maxW/Math.max(1,vw));
      canvas.width=Math.max(1,Math.round(vw*scale)); canvas.height=Math.max(1,Math.round(vh*scale));
      var ctx=canvas.getContext('2d'); ctx.drawImage(video,0,0,canvas.width,canvas.height);
      var data=canvas.toDataURL('image/jpeg',0.86);
      setAnswer('手機自拍照片上傳註冊中：'+name);
      var j=await rt7Json('/api/rt7/face/enroll_mobile',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name:name,device_id:'#mobile',source:'mobile_selfie',image:data})});
      if(j.ok){ rt7CloseSelfieEnroll(); setAnswer('已用手機前鏡頭註冊：'+(j.enrolled&&j.enrolled.name||name)); }
      else { setAnswer('手機註冊失敗：'+(j.answer||j.error||'NO_FACE')); }
    }catch(e){ setAnswer('手機註冊失敗：'+(e.message||e)); }
  }
  async function rt7FaceEnroll(){ rt7OpenSelfieEnroll(); }
  bind('btnSelfieCapture', rt7CaptureSelfieEnroll);
  bind('btnSelfieCancel', function(){ rt7CloseSelfieEnroll(); setAnswer('已取消手機人臉註冊'); });
  async function rt7FaceList(){
    try{
      var j=await rt7Json('/api/rt7/faces');
      if(!j.ok){ setAnswer('名單讀取失敗'); return; }
      var names=(j.faces||[]).map(function(f){return f.name;}).filter(Boolean);
      setAnswer(names.length ? ('已註冊 '+names.length+' 人：'+names.join('、')) : '尚未註冊人臉');
    }catch(e){ setAnswer('名單讀取失敗：'+(e.message||e)); }
  }

  async function rt7FaceGateToggle(){
    try{
      var btn=document.getElementById('btnFaceGate');
      setAnswer('切換 FACE_GATE 測試中...');
      var j=await rt7Json('/api/rt7/face_gate/toggle',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});
      if(btn) btn.textContent = j.enabled ? 'FACE_GATE ON' : 'FACE_GATE OFF';
      if(btn) btn.className = j.enabled ? 'vbtn vorange' : 'vbtn vgreen';
      setAnswer('FACE_GATE 測試模式：'+(j.enabled?'ON（先 Gate，通過才做 Railway 比對）':'OFF（直接 AI 辨識，等同 V5.0S）'));
    }catch(e){ setAnswer('FACE_GATE 切換失敗：'+(e.message||e)); }
  }

  function rt7ShowFaceSnapshot(j){
    // V5.4M: UI minimal mode - hide Snapshot debug block on phone page.
    return;

    try{
      var box=document.getElementById('faceSnapBox'); var im=document.getElementById('faceSnapImg'); var meta=document.getElementById('faceSnapMeta');
      if(!box||!im||!j) return;
      var h=j.snap_hash||j.snapshot_hash||''; var u=j.face_snapshot_url||('/api/rt7/face/last_snapshot.jpg?_='+(Date.now())+'&h='+encodeURIComponent(h));
      im.src=u; box.style.display='block';
      if(meta) meta.innerHTML='SNAP='+(j.snap_time||'')+'<br>BYTES='+(j.latest_bytes||j.bytes||'')+'<br>HASH='+(h||'')+'<br>AGE='+(j.snap_age_ms!=null?j.snap_age_ms+'ms':'')+'<br>SOURCE='+(j.snap_source||'none')+'<br>FORCE='+(j.snap_forced_realtime?'YES':'NO')+'<br>WAIT='+(j.snap_wait_ms!=null?j.snap_wait_ms+'ms':'')+'<br>WS_SENT='+(j.snap_request_ws_sent!=null?j.snap_request_ws_sent:'')+'<br>LIVE_FB='+(j.snap_live_frame_fallback?'YES':'NO')+'<br>CACHE='+(j.cache_mode||'')+'<br>MATCH_MS='+(j.match_ms!=null?j.match_ms+'ms':'');
    }catch(_){ }
  }

  // V5.4D: FACE_GATE auto recognition result polling.
  // ESP32 FACE_GATE PASS now uploads snapshot and Railway auto-matches it.
  // The phone UI must poll the last_face_match result because no manual button callback runs.
  var rt7AutoFaceLastKey='';
  var rt7AutoFacePollBusy=false;
  function rt7FaceResultText_(j, autoLabel){
    if(!j) return '';
    var NL = String.fromCharCode(10);
    var prefix = autoLabel ? ('FACE_GATE 自動辨識：' + NL) : '';
    var found = j.face_found ? 'YES' : 'NO';
    var faceName = j.face_match || j.matched_name || (j.face_match_pass ? '已註冊' : 'unknown');
    var match = (typeof j.match_score !== 'undefined') ? j.match_score : (j.confidence || 0);
    var live = (j.liveness_label || (j.liveness && j.liveness.verdict) || (j.liveness_pass ? 'REAL' : 'UNKNOWN') || 'UNKNOWN').toString().toUpperCase();
    var door = j.door || (j.known_face ? 'ALLOW' : 'DENY');
    var s = prefix +
      'FACE_FOUND=' + found + NL +
      'FACE_MATCH=' + faceName + NL +
      'MATCH=' + match + '%' + NL + NL +
      'LIVENESS=' + live + NL + NL +
      'DOOR=' + door;
    if(j.reason) s += NL + 'REASON=' + j.reason;
    return s;
  }
  async function rt7PollAutoFaceResult_(){
    if(rt7AutoFacePollBusy) return;
    if(!ai) return;
    if(rt7FaceBusy || rt7FaceMatchBusy || rt7FaceRestoreBusy) return;
    rt7AutoFacePollBusy=true;
    try{
      var s=await rt7Json('/api/rt7/face_gate/state?_='+Date.now());
      var m=s && s.last_face_match;
      if(!m) return;
      var isAuto = !!(m.auto_face_gate || m.trigger_source==='esp32_face_gate' || (m.snap_source||'').indexOf('face_gate_auto')>=0);
      if(!isAuto) return;
      // V5.4E: PASS-only UI update.
      // ESP32 SKIP does not POST to Railway, so the server may still hold the previous PASS result.
      // Do not redisplay stale PASS results while current FACE_GATE samples are SKIP.
      var t = Date.parse(m.time || m.snap_time || '');
      var ageMs = isFinite(t) ? (Date.now() - t) : 999999;
      if(ageMs < 0) ageMs = 0;
      if(ageMs > 9000) {
        if(rt7AutoFaceLastKey) {
          rt7AutoFaceLastKey='';
          setDebug('auto face waiting for new PASS');
        }
        return;
      }
      var key=[m.snap_hash||'',m.snap_time||m.time||'',m.confidence||0,m.reason||'',m.known_face?'1':'0'].join('|');
      if(!key || key===rt7AutoFaceLastKey) return;
      rt7AutoFaceLastKey=key;
      rt7ShowFaceSnapshot(m);
      setAnswer(rt7FaceResultText_(m,true));
      setDebug('auto face PASS result '+key);
    }catch(e){
      // Keep silent to avoid disturbing normal stream UI.
    }finally{
      rt7AutoFacePollBusy=false;
    }
  }
  setInterval(rt7PollAutoFaceResult_, 1800);
  var rt7FaceMatchBusy=false;
  var rt7FaceRestoreBusy=false;
  var rt7FaceLastDoneAt=0;
  var rt7FaceResumeTimer=null;
  var rt7FaceResumeStarted=false;
  function rt7FaceSetButtonBusy(faceBtn,busy){
    try{ if(faceBtn){ faceBtn.disabled=!!busy; faceBtn.style.opacity=busy?'0.55':''; faceBtn.textContent=busy?'辨識中':'人臉辨識'; } }catch(_){}
  }
  function rt7FacePauseStreamOnce(){
    // V5.3C: exactly one intentional MJPEG close before face snapshot.
    clearLanReconnect();
    try{ if(img){ img.onerror=null; img.onload=null; img.src='about:blank'; img.removeAttribute('src'); } }catch(_){}
    videoWanted=false;
    currentStreamMode='FACE_PAUSE';
    if(badge) badge.textContent='FACE';
  }
  function rt7FaceResumeStreamOnce(prevMode,hold,faceBtn){
    // V5.3C: exactly one restore after result; block focus/resize restore while doing it.
    if(rt7FaceResumeStarted) return;
    rt7FaceResumeStarted=true;
    clearTimeout(rt7FaceResumeTimer);
    rt7FaceRestoreBusy=true;
    rt7FaceResumeTimer=setTimeout(function(){
      try{
        if(prevMode==='LAN') lan();
        else if(prevMode==='CLOUD') cloud();
        else startAuto();
      }catch(_){ try{ startAuto(); }catch(__){} }
      setTimeout(function(){ if(hold) setAnswer(hold); }, 180);
      setTimeout(function(){
        rt7FaceBusy=false; rt7FaceMatchBusy=false; rt7FaceRestoreBusy=false; rt7FaceLastDoneAt=Date.now(); rt7FaceResumeStarted=false;
        rt7FaceSetButtonBusy(faceBtn,false);
      }, 1500);
    }, 650);
  }
  async function rt7FaceMatch(){
    if(rt7FaceMatchBusy || rt7FaceRestoreBusy){ setAnswer('人臉辨識進行中，請稍候...'); return; }
    if(Date.now() - rt7FaceLastDoneAt < 3500){ setAnswer('剛完成辨識，請稍候再按。'); return; }
    rt7FaceMatchBusy=true; rt7FaceBusy=true; rt7FaceResumeStarted=false;
    var faceBtn=document.getElementById('btnFaceCheck');
    rt7FaceSetButtonBusy(faceBtn,true);
    var wasVideo = !!videoWanted;
    var prevMode = currentStreamMode || 'AUTO';
    var keepResult = '';
    try{
      // V5.3C: pause stream only once, then resume only once after result.
      if(wasVideo){
        setAnswer('暫停影像，準備人臉辨識...');
        rt7FacePauseStreamOnce();
        await new Promise(function(resolve){ setTimeout(resolve, 850); });
      }

      setAnswer('人臉辨識中...');
      var j=await rt7Json('/api/rt7/face/match',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({pause_stream:true,no_stream_reload:true,mode:'face_result_no_stream_reload_v52n'})});
      rt7ShowFaceSnapshot(j);
      if(j.ok) {
        keepResult=rt7FaceResultText_(j,false);
      } else {
        keepResult='人臉辨識失敗：'+(j.answer||j.error||'UNKNOWN');
      }
      setAnswer(keepResult);
      try{ setDebug((j.debug_text||JSON.stringify(j)).slice(0,240)); }catch(_){ }
    }catch(e){
      keepResult='人臉辨識失敗：'+(e.message||e);
      setAnswer(keepResult);
    }finally{
      if(wasVideo){
        var hold = keepResult || (answer && answer.textContent) || '';
        rt7FaceResumeStreamOnce(prevMode, hold, faceBtn);
      } else {
        rt7FaceBusy=false; rt7FaceMatchBusy=false; rt7FaceLastDoneAt=Date.now();
        rt7FaceSetButtonBusy(faceBtn,false);
      }
    }
  }
  var _faceEnrollBtn=document.getElementById('btnFaceEnroll'); if(_faceEnrollBtn)_faceEnrollBtn.addEventListener('click',function(ev){ev.preventDefault();rt7FaceEnroll();});
  var _faceListBtn=document.getElementById('btnFaceList'); if(_faceListBtn)_faceListBtn.addEventListener('click',function(ev){ev.preventDefault();rt7FaceList();});
  var _faceCheckBtn=document.getElementById('btnFaceCheck'); if(_faceCheckBtn)_faceCheckBtn.addEventListener('click',function(ev){ev.preventDefault();rt7FaceMatch();});
  try{ if(document.getElementById('btnAiOn')) document.getElementById('btnAiOn').addEventListener('dblclick',function(ev){ev.preventDefault();rt7FaceMatch();}); }catch(_){ }
  function rt7WsUrl(){ return (location.protocol==='https:'?'wss://':'ws://')+location.host+'/ws'; }
  function rt7WsPcm16Bytes(f32){ var b=new Uint8Array(f32.length*2); for(var i=0;i<f32.length;i++){ var v=Math.max(-1,Math.min(1,f32[i])); var s=Math.round(v<0?v*0x8000:v*0x7fff); b[i*2]=s&255; b[i*2+1]=(s>>8)&255; } return b; }
  function rt7WsDown16(input,rate){ if(!rate||Math.abs(rate-16000)<1)return input; var ratio=rate/16000, len=Math.floor(input.length/ratio); var out=new Float32Array(Math.max(0,len)); for(var i=0;i<len;i++){ var a=Math.floor(i*ratio), b=Math.min(Math.floor((i+1)*ratio),input.length), sum=0,c=0; for(var j=a;j<b;j++){sum+=input[j];c++;} out[i]=c?sum/c:0; } return out; }
  function rt7WsClean(input){ var out=new Float32Array(input.length); var peak=0; for(var i=0;i<input.length;i++){ var x=input[i]*0.86; if(x>0.98)x=0.98; if(x<-0.98)x=-0.98; out[i]=x; var a=Math.abs(x); if(a>peak)peak=a; } out.peak=peak; return out; }
  function rt7WsSendJson(o){ try{ if(rt7WsIc&&rt7WsIc.readyState===1) rt7WsIc.send(JSON.stringify(o)); }catch(e){} }
  function rt7WsQueue(bytes){ for(var i=0;i<bytes.length;i++) rt7WsTxBytes.push(bytes[i]); while(rt7WsTxBytes.length>=640){ var chunk=rt7WsTxBytes.splice(0,640); if(rt7WsTxActive&&rt7WsIc&&rt7WsIc.readyState===1){ rt7WsSent++; try{ rt7WsIc.send(new Uint8Array(chunk).buffer); }catch(e){ setDebug('ws pcm send failed '+(e.message||e)); } } } }
  function rt7WsStopMic(){ try{ if(rt7WsMicProc){rt7WsMicProc.disconnect();rt7WsMicProc.onaudioprocess=null;} }catch(_){} try{ if(rt7WsMicSource)rt7WsMicSource.disconnect(); }catch(_){} try{ if(rt7WsMicCtx)rt7WsMicCtx.close(); }catch(_){} try{ if(rt7WsMicStream)rt7WsMicStream.getTracks().forEach(function(t){try{t.stop();}catch(_){}}); }catch(_){} rt7WsMicStream=null; rt7WsMicCtx=null; rt7WsMicSource=null; rt7WsMicProc=null; }
  async function rt7WsStartMic(){
    rt7WsStopMic();
    rt7WsMicStream=await navigator.mediaDevices.getUserMedia({audio:{echoCancellation:{ideal:true},noiseSuppression:{ideal:false},autoGainControl:{ideal:false},channelCount:{ideal:1},sampleRate:{ideal:48000}},video:false});
    var AC=window.AudioContext||window.webkitAudioContext; rt7WsMicCtx=new AC();
    rt7WsMicSource=rt7WsMicCtx.createMediaStreamSource(rt7WsMicStream);
    rt7WsMicProc=rt7WsMicCtx.createScriptProcessor(2048,1,1);
    rt7WsMicProc.onaudioprocess=function(e){ if(!rt7WsTxActive)return; var raw=e.inputBuffer.getChannelData(0); var cl=rt7WsClean(raw); if(cl.peak<0.00004)return; var ds=rt7WsDown16(cl,rt7WsMicCtx.sampleRate); if(ds.length)rt7WsQueue(rt7WsPcm16Bytes(ds)); };
    rt7WsMicSource.connect(rt7WsMicProc); rt7WsMicProc.connect(rt7WsMicCtx.destination); if(rt7WsMicCtx.state!=='running') await rt7WsMicCtx.resume();
    setDebug('WS PTT mic ready sr='+Math.round(rt7WsMicCtx.sampleRate));
  }
  function rt7RxEnsureAudio(){ var AC=window.AudioContext||window.webkitAudioContext; if(!rt7RxAudioCtx) rt7RxAudioCtx=new AC({sampleRate:16000}); if(rt7RxAudioCtx.state!=='running') rt7RxAudioCtx.resume().catch(function(){}); }
  function rt7RxResetQueue_(){
    // V5.5C: Railway phone player fix. Reset scheduled playback tail so old PCM
    // does not play again after PTT release/stop, which sounded like echo.
    rt7RxPlayAt=0; rt7RxLastMs=0; rt7RxJitterMaxMs=0;
  }
  function rt7RxPlayPcm(ab){
    try{
      // V5.5C: play ESP32->phone PCM only in listen mode. During phone TX or after stop,
      // discard late binary frames instead of scheduling them into AudioContext.
      if(!rt7WsListenActive){ rt7RxResetQueue_(); return; }
      rt7RxEnsureAudio();
      var u8=new Uint8Array(ab); var n=(u8.length/2)|0; if(n<=0)return;
      var f=new Float32Array(n);
      // V5.5D: phone-side soft noise gate for ESP32->phone receive path only.
      // Keep V5.5C queue fix, but suppress very low-level mic noise when ESP32 side is quiet.
      var sum2=0, peak=0;
      for(var i=0;i<n;i++){
        var lo=u8[i*2], hi=u8[i*2+1];
        var s=(hi<<8)|lo; if(s&0x8000)s-=0x10000;
        var x=Math.max(-1,Math.min(1,s/32768));
        f[i]=x;
        var ax=Math.abs(x); if(ax>peak)peak=ax; sum2 += x*x;
      }
      var rms=Math.sqrt(sum2/Math.max(1,n));
      // Do not schedule near-silent packets; this removes the small hiss heard while listening.
      if(rms < 0.0045 && peak < 0.018){
        rt7RxPackets++; rt7RxBytes+=u8.length;
        if(rt7RxPackets<=5||rt7RxPackets%30===0)setDebug('ESP32→手機 PCM[V55D] muted noise rms='+(rms*1000).toFixed(1)+' peak='+(peak*1000).toFixed(1));
        return;
      }
      // Soft knee: quiet packets are attenuated rather than hard-cut, preserving speech starts.
      var gain=1.0;
      if(rms < 0.012){ gain = Math.max(0.18, Math.min(1.0, (rms-0.0045)/(0.012-0.0045))); }
      if(gain < 0.999){ for(var gi=0; gi<n; gi++) f[gi] *= gain; }
      var buf=rt7RxAudioCtx.createBuffer(1,n,16000); buf.copyToChannel(f,0);
      var src=rt7RxAudioCtx.createBufferSource(); src.buffer=buf; src.connect(rt7RxAudioCtx.destination);
      var wall=Date.now(); var dt=rt7RxLastMs?(wall-rt7RxLastMs):0; rt7RxLastMs=wall; if(dt>rt7RxJitterMaxMs)rt7RxJitterMaxMs=dt;
      var now=rt7RxAudioCtx.currentTime;
      // V5.5C: fixed low-latency queue like the Node-RED player; do not let adaptive
      // cushion accumulate 0.1s+ tails that cause tremble/echo after speech ends.
      var cushion=0.02;
      if(!rt7RxPlayAt || rt7RxPlayAt<now+cushion) rt7RxPlayAt=now+cushion;
      if(rt7RxPlayAt>now+0.12) rt7RxPlayAt=now+0.04;
      src.start(rt7RxPlayAt); rt7RxPlayAt += n/16000;
      rt7RxPackets++; rt7RxBytes+=u8.length; if(rt7RxPackets<=5||rt7RxPackets%20===0)setDebug('ESP32→手機 PCM[V55D] packets='+rt7RxPackets+' bytes='+rt7RxBytes+' dt='+dt+' jitterMax='+rt7RxJitterMaxMs+'ms');
    }catch(e){ setDebug('rx play failed '+(e.message||e)); }
  }
  var rt7WsPausedVideoMode=null;
  function rt7Delay(ms){ return new Promise(function(resolve){ setTimeout(resolve, ms); }); }
  function rt7RememberAndPauseVideoForTalk(){
    rt7WsPausedVideoMode=null;
    if(videoWanted || currentStreamMode==='LAN' || currentStreamMode==='CLOUD' || currentStreamMode==='AUTO'){
      rt7WsPausedVideoMode=currentStreamMode||'AUTO';
      try{ localStorage.setItem('RT7_V50_TALK_RESTORE_MODE', rt7WsPausedVideoMode); }catch(_){}
      stopVideo();
      setAnswer('對講準備中：已先暫停影像，避免內網串流擋住麥克風');
      return true;
    }
    return false;
  }
  function rt7RestoreVideoAfterTalk(){
    var m=rt7WsPausedVideoMode; rt7WsPausedVideoMode=null;
    if(!m || m==='IDLE') return;
    setAnswer('對講結束，恢復影像中...');
    setTimeout(function(){ if(rt7WsIcOn) return; if(m==='LAN') lan(); else if(m==='CLOUD') cloud(); else startAuto(); }, 650);
  }
  function rt7SetTalkIcon_(state){
    try{
      var b=document.getElementById('btnEndTalk');
      var v=document.getElementById('btnVoice');
      if(state==='talk'){ if(b){b.textContent='◼'; b.classList.add('talking');} if(v){v.textContent='◼'; v.classList.add('talking');} }
      else if(state==='listen'){ if(b){b.textContent='◼'; b.classList.add('talking');} if(v){v.textContent='🔊'; v.classList.remove('talking');} }
      else { if(b){b.textContent='◼'; b.classList.remove('talking');} if(v){v.textContent='🎙️'; v.classList.remove('talking');} }
    }catch(e){}
  }
  async function rt7WsEnsureSocket(label){
    if(rt7WsIc && rt7WsIc.readyState===1) return true;
    return new Promise(function(resolve){
      rt7WsIc=new WebSocket(rt7WsUrl()+'?role=phone_pcm&device_id='+encodeURIComponent(selectedDeviceId||'#1')+'&phase=V50P'); rt7WsIc.binaryType='arraybuffer';
      var done=false; function finish(ok){ if(done)return; done=true; resolve(ok); }
      rt7WsIc.onopen=function(){ setDebug('WS duplex open'); rt7WsSendJson({role:'phone_pcm',type:'intercom_probe',device_id:(selectedDeviceId||'#1'),label:label||'open',t:Date.now(),phase:'V50P'}); finish(true); };
      rt7WsIc.onmessage=function(ev){ try{ if(typeof ev.data==='string'){ if(ev.data.indexOf('trace')>=0||ev.data.indexOf('relay')>=0) setDebug(ev.data.slice(0,180)); } else if(ev.data){ rt7RxPlayPcm(ev.data); } }catch(e){ setDebug('ws msg err '+(e.message||e)); } };
      rt7WsIc.onerror=function(){ setDebug('WS duplex error'); finish(false); };
      rt7WsIc.onclose=function(){ rt7WsIcOn=false; rt7WsTxActive=false; rt7WsListenActive=false; rt7RxResetQueue_(); rt7WsStopMic(); var a=document.getElementById('btnEndTalk'); if(a)a.classList.remove('talking'); var b=document.getElementById('btnVoice'); if(b)b.classList.remove('talking'); };
      setTimeout(function(){ finish(rt7WsIc&&rt7WsIc.readyState===1); }, 2200);
    });
  }
  async function rt7WsPttDown(label){
    if(rt7WsListenTimer){ clearTimeout(rt7WsListenTimer); rt7WsListenTimer=null; }
    var paused=false;
    if(!rt7WsIcOn){ paused=rt7RememberAndPauseVideoForTalk(); if(paused) await rt7Delay(260); }
    rt7WsIcOn=true; rt7WsTxActive=true; rt7WsListenActive=false; rt7WsSent=0; rt7WsTxBytes=[]; rt7RxPackets=0; rt7RxBytes=0; rt7RxResetQueue_();
    var e=document.getElementById('btnEndTalk'); if(e)e.classList.add('talking'); var vm=document.getElementById('btnVoice'); if(vm)vm.classList.add('talking');
    rt7SetTalkIcon_('talk'); setAnswer('對講中：手機 → ESP32，放開後接收 ESP32 聲音');
    var ok=await rt7WsEnsureSocket(label||'ptt_down'); if(!ok){ setAnswer('對講連線失敗'); return; }
    rt7WsSendJson({role:'phone_pcm',type:'intercom_begin',device_id:(selectedDeviceId||'#1'),label:label||'ptt_down',t:Date.now(),phase:'V50P'});
    try{ await rt7WsStartMic(); }catch(err){ setAnswer('手機麥克風啟用失敗：'+(err.message||err)); rt7WsPttStop('mic_failed'); }
  }
  function rt7WsPttUp(label){
    if(!rt7WsIcOn) return;
    rt7WsTxActive=false; rt7WsStopMic();
    rt7WsSendJson({role:'phone_pcm',type:'intercom_end',device_id:(selectedDeviceId||'#1'),label:label||'ptt_up',sent:rt7WsSent,t:Date.now(),phase:'V50P'});
    rt7WsSendJson({role:'phone_pcm',type:'esp_begin',device_id:(selectedDeviceId||'#1'),label:label||'ptt_up_listen',t:Date.now(),phase:'V50P'});
    rt7WsListenActive=true; rt7RxResetQueue_(); rt7RxEnsureAudio();
    var e=document.getElementById('btnEndTalk'); if(e)e.classList.remove('talking'); var vm=document.getElementById('btnVoice'); if(vm)vm.classList.remove('talking');
    rt7SetTalkIcon_('listen'); setAnswer('接收中：ESP32 → 手機；按下 ◼ 對講 才結束');
    if(rt7WsListenTimer){ clearTimeout(rt7WsListenTimer); rt7WsListenTimer=null; }
    // V50P: 放開中央對講鍵後，保持 ESP32→手機接收，不再自動 10 秒結束。
    // 只有按下下方「◼ 對講」結束鍵，才會停止接收與恢復影像。
  }
  function rt7WsPttStop(label){
    if(rt7WsListenTimer){ clearTimeout(rt7WsListenTimer); rt7WsListenTimer=null; }
    rt7WsTxActive=false; rt7WsListenActive=false;
    rt7RxResetQueue_();
    rt7WsSendJson({role:'phone_pcm',type:'esp_end',device_id:(selectedDeviceId||'#1'),label:label||'stop',t:Date.now(),phase:'V50P'});
    rt7WsSendJson({role:'phone_pcm',type:'intercom_end',device_id:(selectedDeviceId||'#1'),label:label||'stop',sent:rt7WsSent,t:Date.now(),phase:'V50P'});
    setTimeout(function(){ try{ if(rt7WsIc)rt7WsIc.close(); }catch(_){} rt7WsIc=null; rt7WsStopMic(); rt7WsIcOn=false; rt7RestoreVideoAfterTalk(); },120);
    rt7SetTalkIcon_('idle'); setAnswer('對講結束');
  }
  function rt7BindPtt(id,label){
    var el=document.getElementById(id); if(!el)return;
    var down=false;
    function d(ev){ if(ev){ev.preventDefault();ev.stopPropagation();} if(down)return; down=true; rt7WsPttDown(label); }
    function u(ev){ if(ev){ev.preventDefault();ev.stopPropagation();} if(!down)return; down=false; rt7WsPttUp(label); }
    el.addEventListener('pointerdown',d,{passive:false}); el.addEventListener('pointerup',u,{passive:false}); el.addEventListener('pointercancel',u,{passive:false}); el.addEventListener('pointerleave',function(ev){ if(down)u(ev); },{passive:false});
    el.addEventListener('touchstart',d,{passive:false}); el.addEventListener('touchend',u,{passive:false});
    el.addEventListener('click',function(ev){ ev.preventDefault(); ev.stopPropagation(); },true);
  }
  rt7BindPtt('btnVoice','center_ptt');
  // V50P: 下方「◼ 對講」只做結束鍵，不再做 PTT。
  (function(){
    var endBtn=document.getElementById('btnEndTalk');
    if(!endBtn) return;
    function stop(ev){
      if(ev){ ev.preventDefault(); ev.stopPropagation(); }
      if(rt7WsIcOn || rt7WsTxActive || rt7WsListenActive){ rt7WsPttStop('manual_end_button'); }
      else { setAnswer('目前沒有進行中的對講'); rt7SetTalkIcon_('idle'); }
    }
    endBtn.addEventListener('click', stop, true);
    endBtn.addEventListener('touchend', stop, {passive:false});
    endBtn.addEventListener('pointerup', stop, {passive:false});
  })();
  var lastCount=null;
  function rt7HandleDoorbellFast_(payload){
    try{
      var p=payload&&payload.payload?payload.payload:payload;
      var c=Number((p&&p.count) || (p&&p.state&&p.state.count) || 0);
      if(c && (lastCount===null || c>lastCount)) lastCount=c;
      showDoorbellInline();
    }catch(e){ showDoorbellInline(); }
  }
  async function pollDoor(){
    try{
      var r=await fetch('/api/rt7/doorbell/state?_='+Date.now(),{cache:'no-store'});
      var jj=await r.json(); var st=jj.state||jj;
      if(st&&typeof st.count==='number'){
        if(lastCount===null) lastCount=st.count;
        else if(st.count!==lastCount){ lastCount=st.count; showDoorbellInline(); }
      }
    }catch(e){}
    setTimeout(pollDoor,500);
  }
  function rt7DoorbellWsFast_(){
    try{
      var w=new WebSocket((location.protocol==='https:'?'wss://':'ws://')+location.host+'/ws?role=viewer&device_id='+encodeURIComponent(selectedDeviceId||'#1')+'&fast=doorbell');
      w.onmessage=function(ev){
        try{
          if(typeof ev.data!=='string') return;
          var m=JSON.parse(ev.data);
          if(m && m.type==='doorbell') rt7HandleDoorbellFast_(m);
        }catch(e){}
      };
      w.onclose=function(){ setTimeout(rt7DoorbellWsFast_,1200); };
    }catch(e){ setTimeout(rt7DoorbellWsFast_,2500); }
  }
  pollDoor(); rt7DoorbellWsFast_();

  // V5.0D: Phone sleep / foreground-background auto recovery, based on original Node-RED design.
  // - Uses Screen Wake Lock when user starts video or taps page.
  // - Records whether the user wants video in localStorage.
  // - On visibilitychange/pageshow/focus/resize, restores the previous LAN/CLOUD stream.
  // - On background, notifies Railway to lower cloud FPS, but does not destroy the user's wanted state.
  var rt7WakeLock=null; var rt7RestoreBusy=false; var rt7RestoreTimer=null; var rt7FaceBusy=false;
  async function rt7RequestWakeLock(reason){
    try{
      if(document.visibilityState && document.visibilityState!=='visible') return false;
      if(!('wakeLock' in navigator) || !navigator.wakeLock || !navigator.wakeLock.request) return false;
      if(rt7WakeLock) return true;
      rt7WakeLock = await navigator.wakeLock.request('screen');
      rt7WakeLock.addEventListener('release', function(){ rt7WakeLock=null; setDebug('wakelock released'); });
      setDebug('wakelock on '+reason);
      return true;
    }catch(e){ setDebug('wakelock unavailable '+reason); return false; }
  }
  async function rt7ResumeAudio(reason){
    try{ if(audioCtx && audioCtx.state!=='running') await audioCtx.resume(); }catch(e){}
  }
  function rt7VideoWanted(){
    try{ return videoWanted || localStorage.getItem('RT7_V50_WANTED_VIDEO')==='1'; }catch(e){ return videoWanted; }
  }
  function rt7SavedMode(){
    try{ return localStorage.getItem('RT7_V50_STREAM_MODE') || currentStreamMode || 'AUTO'; }catch(e){ return currentStreamMode || 'AUTO'; }
  }
  function rt7RestoreVideo(reason){
    if(rt7FaceBusy || rt7FaceRestoreBusy) return;
    if(!rt7VideoWanted()) return;
    if(rt7RestoreBusy) return;
    clearTimeout(rt7RestoreTimer);
    rt7RestoreTimer=setTimeout(function(){
      if(!rt7VideoWanted()) return;
      if(document.visibilityState && document.visibilityState!=='visible') return;
      rt7RestoreBusy=true;
      rt7RequestWakeLock(reason);
      rt7ResumeAudio(reason);
      var m=rt7SavedMode();
      setAnswer('回到前景：恢復影像串流中');
      setDebug('restore video '+reason+' mode='+m);
      try{
        if(m==='LAN' && currentStreamMode==='LAN'){
          if(img) img.src='http://'+ip+'/api/camera/stream?_fg='+Date.now();
        }else if(m==='CLOUD' && currentStreamMode==='CLOUD'){
          if(img) img.src='/api/rt7/camera/stream.mjpg?device_id='+encodeURIComponent(selectedDeviceId||'#1')+'&_fg='+Date.now();
        }else{
          startAuto();
        }
      }catch(e){ startAuto(); }
      setTimeout(function(){ rt7RestoreBusy=false; }, 1200);
    }, reason==='resize' ? 800 : 350);
  }
  function rt7BackgroundIdle(reason){
    if(rt7FaceBusy || rt7FaceRestoreBusy) return;
    if(!rt7VideoWanted()) return;
    setDebug('background idle '+reason);
    try{ navigator.sendBeacon && navigator.sendBeacon('/api/rt7/camera/viewer/ping?state=hidden&_='+Date.now(), ''); }catch(e){}
    try{ fetch('/api/rt7/camera/stream/stop?_bg='+Date.now(), {cache:'no-store', keepalive:true}).catch(function(){}); }catch(e){}
  }
  ['click','touchend','pointerup'].forEach(function(ev){ document.addEventListener(ev, function(){ rt7RequestWakeLock(ev); rt7ResumeAudio(ev); }, {passive:true}); });
  document.addEventListener('visibilitychange', function(){
    if(document.visibilityState==='visible') rt7RestoreVideo('visibilitychange');
    else rt7BackgroundIdle('visibilitychange');
  });
  window.addEventListener('pageshow', function(){ rt7RestoreVideo('pageshow'); });
  window.addEventListener('focus', function(){ rt7RestoreVideo('focus'); });
  window.addEventListener('resize', function(){ if(document.visibilityState==='visible') rt7RestoreVideo('resize'); });
  setInterval(function(){
    if(!rt7VideoWanted()) return;
    var state=(document.visibilityState==='visible')?'visible':'hidden';
    try{ fetch('/api/rt7/camera/viewer/ping?state='+state+'&_='+Date.now(), {cache:'no-store'}).catch(function(){}); }catch(e){}
  }, 15000);

  if(mode==='auto') setTimeout(startAuto, 300); else if(mode==='lan') lan(); else if(mode==='cloud') cloud(); else stopVideo();
})();
</script>
</body></html>`);
});

app.get('/api/rt7/events/latest', (req,res)=>res.redirect(307, '/api/events/latest?limit=' + encodeURIComponent(req.query.limit || '200')));
app.get('/api/rt7/events/clear', (req,res)=>res.redirect(307, '/api/events/clear'));
app.get('/api/rt7/devices/list', (req,res)=>res.json({ ok:true, version:SERVER_VERSION, devices:rt7FilterDevicesForRequest_(req, readDevices()), master:rt7ReadMasterRegistry_(), current_device_id:cloudState.current_device_id, file:'data/devices.json' }));
app.post('/api/rt7/devices/save', (req,res)=>{
  const devices = saveDevices(req.body?.devices || req.body || []);
  appendEvent({ type:'devices_save', device_count:devices.length, file:'data/devices.json', message:'devices saved from V5.6B Device Manager' });
  broadcast('devices_save', { devices, file:'data/devices.json' });
  res.json({ ok:true, version:SERVER_VERSION, devices, file:'data/devices.json' });
});
app.get('/api/rt7/device/state', (req,res)=>res.json({ ok:true, current_device_id:cloudState.current_device_id, device:getCurrentDevice(req), cloudState }));
app.post('/api/rt7/device/set', (req,res)=>{ cloudState.current_device_id = safeString(req.body?.device_id || req.body?.id || req.query.device_id || '#1'); res.json({ ok:true, current_device_id:cloudState.current_device_id, device:getCurrentDevice(req) }); });
app.get('/api/rt7/device/proxy_status', (req,res)=>proxyToEsp(req,res,'/api/status','GET'));

// Independent full intercom proxy-compatible endpoints
app.get('/api/ind_full/ui/state', (req,res)=>proxyToEsp(req,res,'/api/ui/state','GET'));
app.get('/api/ind_full/audio/phone_begin', (req,res)=>proxyToEsp(req,res,'/api/audio/phone_begin','GET'));
app.post('/api/ind_full/audio/phone_pcm_hex', express.text({type:'*/*', limit:'2mb'}), (req,res)=>proxyToEsp(req,res,'/api/audio/phone_pcm_hex','POST'));
app.get('/api/ind_full/audio/phone_end', (req,res)=>proxyToEsp(req,res,'/api/audio/phone_end','GET'));
app.get('/api/ind_full/audio/esp_begin', (req,res)=>proxyToEsp(req,res,'/api/audio/esp_begin','GET'));
app.get('/api/ind_full/audio/esp_pcm_hex', (req,res)=>proxyToEsp(req,res,'/api/audio/esp_pcm_hex','GET'));
app.get('/api/ind_full/audio/esp_end', (req,res)=>proxyToEsp(req,res,'/api/audio/esp_end','GET'));
app.get('/api/ind_full/audio/speaker_tone', (req,res)=>proxyToEsp(req,res,'/api/audio/speaker_tone','GET'));
app.get('/api/ind_full/audio/mic_raw_test', (req,res)=>proxyToEsp(req,res,'/api/audio/mic_raw_test','GET'));

// Plugin / guard state compatible with Phase6C3 Node-RED flow
app.get('/api/rt7/phase6c3_plugin/ping', (req,res)=>res.json({ ok:true, version:SERVER_VERSION, mode:'railway_no_nodered', time:nowIso() }));
app.get('/api/rt7/phase6c3_plugin/plugins/state', (req,res)=>res.json({ ok:true, plugins:cloudState.plugins, ai_enabled:cloudState.ai_enabled, last_snapshot:cloudState.last_snapshot, last_vision:cloudState.last_vision }));
app.get('/api/rt7/phase6c3_plugin/plugins/:plugin/:action', (req,res)=>{ const p=req.params.plugin, a=req.params.action; cloudState.plugins[p] = !/disable|off|0/i.test(a); appendEvent({type:'plugin_set', plugin:p, enabled:cloudState.plugins[p]}); res.json({ok:true, plugins:cloudState.plugins}); });
app.get('/api/rt7/phase6c3_plugin/plugins/reset', (req,res)=>{ cloudState.plugins={ motion:true, face:true, doorbell:true, intercom:true }; res.json({ok:true, plugins:cloudState.plugins}); });
app.get('/api/rt7/phase6c3_plugin/status', (req,res)=>res.json({ ok:true, version:SERVER_VERSION, cloudState, doorbellState }));
app.get('/api/rt7/phase6c3_plugin/camera/status', (req,res)=>res.json({ ok:true, camera:{ latest:!!cloudState.last_snapshot, last_snapshot:cloudState.last_snapshot } }));
app.get('/api/rt7/phase6c3_plugin/camera/state', (req,res)=>res.json({ ok:true, state:{ latest_snapshot:cloudState.last_snapshot, url: cloudState.last_snapshot ? '/api/rt7/camera/latest.jpg' : '' } }));


// V5.4A: ESP32 FACE_GATE auto recognition.
// ESP32 performs motion + FACE_GATE candidate detection, then POSTs the candidate JPEG.
// Railway receives that snapshot and runs the same Railway-local face detect/match flow
// without sending face_snapshot_now back to ESP32 and without restarting the MJPEG viewer.
function rt7IsFaceGateAutoSnapshot_(req) {
  const q = safeString(req && req.query && (req.query.face_gate_auto || req.query.auto_face || req.query.source) || '');
  const h = safeString(req && (req.headers['x-rt7-face-gate-auto'] || req.headers['x-rt7-source'] || req.headers['x-rt7-snapshot-source']) || '');
  return /face_gate_auto|rt7_face_gate_auto|1|true/i.test(q) || /face_gate_auto|rt7_face_gate_auto/i.test(h);
}
function rt7HasPendingFaceSnapshotCommand_() {
  try {
    const a = Array.isArray(cloudState.command_queue) ? cloudState.command_queue : [];
    const b = Array.isArray(pendingCommands) ? pendingCommands : [];
    return a.concat(b).some(c => c && c.status === 'pending' && (c.command === 'face_snapshot_now' || c.action === 'face_snapshot_now' || c.priority === 'face_snapshot'));
  } catch (_) { return false; }
}
function rt7ShouldAutoMatchSnapshot_(req, bytes) {
  // V5.4B: ESP32 FACE_GATE auto snapshot currently POSTs to the same snapshot endpoint
  // without a reliable query/header marker.  If FACE_GATE auto is enabled and the
  // snapshot is not part of a manual face_snapshot_now command, treat it as an
  // ESP32 FACE_GATE candidate.  Cooldown inside rt7StartFaceGateAutoMatch_ prevents
  // repeated recognition while the person remains in front of the camera.
  if (rt7IsFaceGateAutoSnapshot_(req)) return true;
  // V5.4C: FACE_GATE auto recognition is controlled by face_gate_auto_enabled,
  // not by the old AI-enable flag.  ESP32 posts only FACE_GATE candidate
  // snapshots to this endpoint in this mode; Railway must force local match.
  if (!cloudState.face_gate_auto_enabled) return false;
  const q = safeString(req && req.query && (req.query.no_auto_face || req.query.manual || req.query.probe) || '');
  if (/1|true|yes/i.test(q)) return false;
  if (rt7HasPendingFaceSnapshotCommand_()) return false;
  return Number(bytes || 0) >= 3000;
}
function rt7StartFaceGateAutoMatch_(reason) {
  const now = Date.now();
  if (!cloudState.face_gate_auto_enabled) {
    console.log('[RT7_FACE_GATE_AUTO][V54O][SKIP] auto_disabled ai=' + (cloudState.ai_enabled?1:0) + ' auto=' + (cloudState.face_gate_auto_enabled?1:0));
    return false;
  }
  if (cloudState.face_gate_auto_busy) {
    console.log('[RT7_FACE_GATE_AUTO][V54O][SKIP] busy reason=' + safeString(reason));
    return false;
  }
  const cd = Number(cloudState.face_gate_auto_cooldown_ms || 8000);
  if (cloudState.face_gate_auto_last_ms && (now - cloudState.face_gate_auto_last_ms) < cd) {
    console.log('[RT7_FACE_GATE_AUTO][V54O][SKIP] cooldown remain=' + (cd - (now - cloudState.face_gate_auto_last_ms)) + 'ms');
    return false;
  }
  cloudState.face_gate_auto_busy = true;
  cloudState.face_gate_auto_last_ms = now;
  setTimeout(async () => {
    try {
      const latest = rt7GetLatestWithMeta_();
      if (!latest || !latest.b64) throw new Error('NO_LATEST_SNAPSHOT_FOR_AUTO_FACE');
      latest.snap_source = 'face_gate_auto_snapshot';
      latest.snap_forced_realtime = false;
      latest.snap_request_ws_sent = 0;
      latest.snap_wait_ms = 0;
      latest.snap_live_frame_fallback = false;
      latest.snap_stale_warning = false;
      latest.auto_face_gate = true;
      console.log('[RT7_FACE_GATE_AUTO][V54O][MATCH_START] hash=' + latest.snap_hash + ' bytes=' + latest.bytes + ' age=' + latest.snap_age_ms + 'ms reason=' + safeString(reason));
      const r = await rt7FaceMatchLatestCore_(latest, { auto_face_gate:true });
      if (r && typeof r === 'object') {
        r.auto_face_gate = true;
        r.trigger_source = 'esp32_face_gate';
        cloudState.last_face_match = r;
        broadcast('face_match', r);
      }
      console.log('[RT7_FACE_GATE_AUTO][V54O][MATCH_DONE] known=' + (r && r.known_face ? 1 : 0) + ' conf=' + (r && r.confidence || 0) + ' reason=' + (r && r.reason || ''));
    } catch (e) {
      const fail = { ok:false, version:SERVER_VERSION, type:'face_match', auto_face_gate:true, trigger_source:'esp32_face_gate', reason:'AUTO_FACE_MATCH_ERROR', error:String(e && e.message || e), time:nowIso() };
      cloudState.last_face_match = fail;
      broadcast('face_match', fail);
      console.warn('[RT7_FACE_GATE_AUTO][V54O][MATCH_ERR] ' + String(e && e.message || e));
    } finally {
      cloudState.face_gate_auto_busy = false;
    }
  }, 20);
  return true;
}
app.post('/api/rt7/phase6a_fix2/motion/event', (req,res)=>{
  const body = req.body || {};
  let autoStarted = false;
  try {
    const b64 = safeString(body.jpeg_b64 || body.image_b64 || body.b64 || '').replace(/^data:image\/jpeg;base64,/, '');
    if (b64 && body.motion_active !== false) {
      const buf = Buffer.from(b64, 'base64');
      if (buf && buf.length > 800 && buf[0] === 0xFF && buf[1] === 0xD8) {
        ensureDataDir();
        fs.writeFileSync(SNAPSHOT_FILE, buf);
        cloudState.last_snapshot = { ok:true, bytes:buf.length, time:nowIso(), source:'face_gate_auto_snapshot', device_id:safeString(body.device_id || body.ip || '#1'), ip:clientIp(req), url:'/api/rt7/camera/latest.jpg' };
        broadcast('snapshot', cloudState.last_snapshot);
        autoStarted = rt7StartFaceGateAutoMatch_('motion_event');
      }
    }
  } catch (e) { console.warn('[RT7_FACE_GATE_AUTO][V54O][MOTION_EVENT_ERR] ' + String(e && e.message || e)); }
  const ev=appendEvent(Object.assign({ type:'motion', message:'ESP32 motion event', auto_face_gate_started:autoStarted }, body));
  broadcast('motion', ev);
  res.json({ok:true,busy:autoStarted,event:ev,auto_face_gate_started:autoStarted});
});
app.get('/api/rt7/phase6c3_plugin/alarm/confirm', (req,res)=>{ const ev=appendEvent({type:'alarm_confirm', message:'alarm confirmed from cloud UI'}); broadcast('alarm_confirm', ev); res.json({ok:true,event:ev}); });


function getSnapshotMeta_() {
  ensureDataDir();
  if (cloudState.last_snapshot) return cloudState.last_snapshot;
  if (fs.existsSync(SNAPSHOT_FILE)) {
    const st = fs.statSync(SNAPSHOT_FILE);
    cloudState.last_snapshot = {
      ok: true,
      bytes: st.size,
      time: st.mtime.toISOString(),
      source: 'restored_from_file',
      device_id: '#1',
      ip: '',
      url: '/api/rt7/camera/latest.jpg'
    };
    return cloudState.last_snapshot;
  }
  return null;
}

// ESP32 actively uploads snapshots here. Supports raw image/jpeg or JSON {image_b64/jpeg_b64}.
app.post('/api/rt7/camera/snapshot', express.raw({type:['image/jpeg','image/jpg','application/octet-stream'], limit:'6mb'}), (req,res)=>{
  ensureDataDir();
  let buf = Buffer.isBuffer(req.body) ? req.body : null;
  if (!buf || buf.length < 10) return res.status(400).json({ok:false,error:'JPEG_BODY_REQUIRED'});
  fs.writeFileSync(SNAPSHOT_FILE, buf);
  const isAutoFace = rt7ShouldAutoMatchSnapshot_(req, buf.length);
  cloudState.last_snapshot = { ok:true, bytes:buf.length, time:nowIso(), source:isAutoFace?'face_gate_auto_snapshot':'raw_post', device_id:safeString(req.query.device_id || req.headers['x-rt7-device-id'] || '#1'), ip:clientIp(req), url:'/api/rt7/camera/latest.jpg' };
  const autoStarted = isAutoFace ? rt7StartFaceGateAutoMatch_('snapshot_post_auto_detect') : false;
  const ev=appendEvent({ type:isAutoFace?'face_gate_auto_snapshot':'snapshot', bytes:buf.length, message:isAutoFace?'face gate auto snapshot uploaded':'snapshot uploaded', auto_face_gate_started:autoStarted });
  broadcast('snapshot', cloudState.last_snapshot);
  res.json({ ok:true, snapshot:cloudState.last_snapshot, event:ev, auto_face_gate_started:autoStarted });
});
app.post('/api/rt7/camera/snapshot_json', (req,res)=>{
  ensureDataDir();
  const b64 = safeString(req.body?.image_b64 || req.body?.jpeg_b64 || req.body?.b64 || '').replace(/^data:image\/jpeg;base64,/, '');
  if (!b64) return res.status(400).json({ok:false,error:'image_b64 required'});
  const buf = Buffer.from(b64, 'base64');
  fs.writeFileSync(SNAPSHOT_FILE, buf);
  const isAutoFace = rt7ShouldAutoMatchSnapshot_(req, buf.length);
  cloudState.last_snapshot = { ok:true, bytes:buf.length, time:nowIso(), source:isAutoFace?'face_gate_auto_snapshot':'json_b64', device_id:safeString(req.body?.device_id || '#1'), ip:clientIp(req), url:'/api/rt7/camera/latest.jpg' };
  const autoStarted = isAutoFace ? rt7StartFaceGateAutoMatch_('snapshot_json_auto_detect') : false;
  const ev=appendEvent({ type:isAutoFace?'face_gate_auto_snapshot':'snapshot', bytes:buf.length, message:isAutoFace?'face gate auto snapshot uploaded json':'snapshot uploaded json', auto_face_gate_started:autoStarted });
  broadcast('snapshot', cloudState.last_snapshot);
  res.json({ ok:true, snapshot:cloudState.last_snapshot, event:ev, auto_face_gate_started:autoStarted });
});


function streamViewerPrune_() {
  const now = Date.now();
  for (const [id, meta] of streamViewers.entries()) {
    if (!meta || (now - (meta.ts || 0)) > RT7_VIEWER_ACTIVE_TTL_MS) streamViewers.delete(id);
  }
  liveStreamState.viewer_count = streamViewers.size;
  liveStreamState.last_viewer_ping = streamViewers.size ? new Date(Math.max(...Array.from(streamViewers.values()).map(v=>v.ts||0))).toISOString() : null;
  return streamViewers.size;
}
function streamMode_(mode, req) {
  const dev = getCurrentDevice(req);
  const deviceId = normalizeDoorCommandDeviceId_(safeString(req.query.device_id || dev.id || '#1'));
  const fast = mode === 'fast';
  liveStreamState.enabled = true;
  liveStreamState.fps_mode = fast ? 'fast' : 'idle';
  liveStreamState.desired_interval_ms = fast ? ((Date.now() < rt7MjpegCongestUntilMs) ? RT7_STREAM_STABLE_MS : RT7_STREAM_FAST_MS) : RT7_STREAM_IDLE_MS;
  liveStreamState.adaptive_mode = fast ? ((Date.now() < rt7MjpegCongestUntilMs) ? 'fallback_7fps' : 'target_10fps') : 'idle_1fps';
  const cmd = queueCommand({
    command: fast ? 'stream_start' : 'stream_idle',
    action: fast ? 'stream_start' : 'stream_idle',
    device_id: deviceId,
    interval_ms: liveStreamState.desired_interval_ms,
    message: fast ? 'viewer foreground: fast live stream' : 'viewer background: idle live stream'
  });
  return { ok:true, version:SERVER_VERSION, stream:liveStreamState, command:cmd };
}

function acceptWsStreamFrame_(buf, ws) {
  ensureDataDir();
  if (!Buffer.isBuffer(buf)) buf = Buffer.from(buf || []);
  if (!buf || buf.length < 16 || buf[0] !== 0xFF || buf[1] !== 0xD8) return false;
  if (rt7AudioActive_()) return true;
  latestStreamFrame = Buffer.from(buf);
  fs.writeFileSync(STREAM_FRAME_FILE, latestStreamFrame);
  fs.writeFileSync(SNAPSHOT_FILE, latestStreamFrame); // keep Vision QA / latest.jpg aligned with live stream
  const meta = { ok:true, bytes:buf.length, time:nowIso(), source:'ws_frame', device_id:safeString(ws?.rt7DeviceId || '#1'), ip:safeString(ws?._socket?.remoteAddress || ''), url:'/api/rt7/camera/latest.jpg' };
  cloudState.last_snapshot = meta;
  liveStreamState = Object.assign({}, liveStreamState, { ok:true, transport:'ws_frame', seq:(liveStreamState.seq||0)+1, bytes:buf.length, time:meta.time, device_id:meta.device_id, ip:meta.ip, last_url:'/ws', last_frame_ms:Date.now() });
  broadcastBinaryToViewers(latestStreamFrame);
  broadcast('stream_frame', liveStreamState);
  return true;
}

// V4.7 WebSocket Frame Stream: ESP32 sends binary JPEG frames to /ws; browser receives binary JPEG frames.
// HTTP POST /api/rt7/camera/frame remains as a fallback.
function acceptStreamFrame_(req, res) {
  ensureDataDir();
  const buf = Buffer.isBuffer(req.body) ? req.body : null;
  if (!buf || buf.length < 16 || buf[0] !== 0xFF || buf[1] !== 0xD8) return res.status(400).json({ok:false,error:'JPEG_FRAME_REQUIRED'});
  if (rt7AudioActive_()) return res.json({ ok:true, version:SERVER_VERSION, audio_gate:true, skipped:true });
  latestStreamFrame = Buffer.from(buf);
  fs.writeFileSync(STREAM_FRAME_FILE, latestStreamFrame);
  fs.writeFileSync(SNAPSHOT_FILE, latestStreamFrame); // keep Vision QA / latest.jpg aligned with live stream
  const meta = { ok:true, bytes:buf.length, time:nowIso(), source:'live_frame', device_id:safeString(req.query.device_id || req.headers['x-rt7-device-id'] || '#1'), ip:clientIp(req), url:'/api/rt7/camera/latest.jpg' };
  cloudState.last_snapshot = meta;
  liveStreamState = Object.assign({}, liveStreamState, { ok:true, transport:'http_frame_relay', seq:(liveStreamState.seq||0)+1, bytes:buf.length, time:meta.time, device_id:meta.device_id, ip:meta.ip, last_url:'/api/rt7/camera/stream.mjpg', last_frame_ms:Date.now() });
  // V4.7C: IMPORTANT FIX. If ESP32 falls back to HTTP POST frames, still relay
  // the JPEG bytes to WebSocket viewers. Previous V4.7A/B only updated cache and
  // sent JSON metadata, so phone showed "WS connected" but received no binary JPEG.
  broadcastBinaryToViewers(latestStreamFrame);
  broadcast('stream_frame', liveStreamState);
  res.json({ ok:true, version:SERVER_VERSION, frame:{ seq:liveStreamState.seq, bytes:buf.length, time:meta.time, transport:'http_frame_relay' }, snapshot:meta });
}
app.post('/api/rt7/camera/frame', express.raw({type:['image/jpeg','image/jpg','application/octet-stream'], limit:'6mb'}), acceptStreamFrame_);
app.post('/api/rt7/camera/stream/frame', express.raw({type:['image/jpeg','image/jpg','application/octet-stream'], limit:'6mb'}), acceptStreamFrame_);
app.get('/api/rt7/camera/stream/state', (req,res)=>{ streamViewerPrune_(); res.json({ ok:true, version:SERVER_VERSION, stream:liveStreamState, snapshot:getSnapshotMeta_() }); });
app.get('/api/rt7/camera/ws/state', (req,res)=>{
  let viewers=0, uploaders=0;
  for (const ws of wss.clients) { if (ws.readyState === WebSocket.OPEN) { if (ws.rt7Role === 'viewer') viewers++; if (ws.rt7Role === 'esp32_frame_upload') uploaders++; } }
  liveStreamState.ws_viewers=viewers; liveStreamState.ws_uploaders=uploaders;
  res.json({ ok:true, version:SERVER_VERSION, ws:{ path:'/ws', viewers, uploaders }, stream:liveStreamState, snapshot:getSnapshotMeta_() });
});
app.get('/api/rt7/camera/stream/start', (req,res)=>res.json(streamMode_('fast', req)));
app.get('/api/rt7/camera/stream/stop', (req,res)=>res.json(streamMode_('idle', req)));
app.get('/api/rt7/camera/viewer/ping', (req,res)=>{
  const id = safeString(req.query.viewer_id || req.ip || clientIp(req) || 'viewer');
  const state = safeString(req.query.state || 'visible');
  if (state === 'hidden' || state === 'stop') streamViewers.delete(id);
  else streamViewers.set(id, { ts:Date.now(), ip:clientIp(req), ua:req.headers['user-agent']||'', state });
  const n = streamViewerPrune_();
  if (n > 0 && liveStreamState.fps_mode !== 'fast') return res.json(streamMode_('fast', req));
  if (n <= 0 && liveStreamState.fps_mode !== 'idle') return res.json(streamMode_('idle', req));
  res.json({ok:true, version:SERVER_VERSION, stream:liveStreamState, viewers:n});
});
app.get('/api/rt7/camera/stream.mjpg', (req,res)=>{
  res.writeHead(200, {
    'Content-Type':'multipart/x-mixed-replace; boundary=rt7frame',
    'Cache-Control':'no-cache, no-store, must-revalidate, private',
    'Connection':'keep-alive',
    'Pragma':'no-cache',
    'Expires':'0',
    'X-Accel-Buffering':'no',
    'X-RT7-Relay':'stable-5fps-repeat-latest-frame'
  });
  liveStreamState.clients = (liveStreamState.clients || 0) + 1;
  liveStreamState.cloud_mjpeg_clients = liveStreamState.clients;
  liveStreamState.cloud_relay_mode = 'stable_5fps_repeat_latest';
  liveStreamState.cloud_relay_interval_ms = 200;

  let lastFrame = null;
  let lastSeq = -1;
  let sent = 0;
  let busy = false;
  let closed = false;

  // V5.0C: Stable Cloud MJPEG relay.
  // The external phone viewer must not depend on ESP32 frame timing. Railway sends
  // a constant 5 FPS multipart MJPEG stream. If ESP32 briefly misses WS/HTTP upload,
  // repeat the most recent JPEG instead of letting the browser wait and appear frozen.
  const readFallbackFrame = () => {
    try {
      if (latestStreamFrame && Buffer.isBuffer(latestStreamFrame) && latestStreamFrame.length > 16) return latestStreamFrame;
      if (fs.existsSync(STREAM_FRAME_FILE)) return fs.readFileSync(STREAM_FRAME_FILE);
      if (fs.existsSync(SNAPSHOT_FILE)) return fs.readFileSync(SNAPSHOT_FILE);
    } catch (_) {}
    return null;
  };

  const writeOneFrame = () => {
    if (closed || busy || res.destroyed || res.writableEnded) return;
    try {
      const now = Date.now();
      const seq = liveStreamState.seq || 0;
      let frame = readFallbackFrame();
      if (frame && frame.length > 16) {
        lastFrame = Buffer.from(frame);
        lastSeq = seq;
      } else if (lastFrame) {
        frame = lastFrame;
      } else {
        return;
      }

      const repeated = (seq === lastSeq && frame === lastFrame) || (lastFrame && frame.length === lastFrame.length && seq === lastSeq);
      const head = `--rt7frame\r\nContent-Type: image/jpeg\r\nContent-Length: ${frame.length}\r\nX-RT7-Seq: ${seq}\r\nX-RT7-Repeat: ${repeated ? 1 : 0}\r\nX-RT7-Relay-Mode: stable_5fps\r\n\r\n`;
      busy = true;
      const ok = res.write(head) && res.write(frame) && res.write('\r\n');
      sent++;
      liveStreamState.cloud_mjpeg_sent = (liveStreamState.cloud_mjpeg_sent || 0) + 1;
      liveStreamState.cloud_mjpeg_last_sent_ms = now;
      liveStreamState.cloud_mjpeg_last_seq = seq;
      liveStreamState.cloud_mjpeg_last_repeat = repeated ? 1 : 0;
      if (!ok) {
        liveStreamState.cloud_mjpeg_backpressure = (liveStreamState.cloud_mjpeg_backpressure || 0) + 1;
        res.once('drain', ()=>{ busy=false; });
      } else {
        busy = false;
      }
    } catch (e) {
      closed = true;
      clearInterval(timer);
      try { res.end(); } catch(_){ }
    }
  };

  writeOneFrame();
  const timer = setInterval(writeOneFrame, 200); // fixed 5 FPS cloud output
  req.on('close', ()=>{
    closed = true;
    clearInterval(timer);
    liveStreamState.clients = Math.max(0, (liveStreamState.clients || 1)-1);
    liveStreamState.cloud_mjpeg_clients = liveStreamState.clients;
    liveStreamState.cloud_mjpeg_last_client_frames = sent;
  });
});

app.get('/api/rt7/camera/latest.jpg', (req,res)=>{ ensureDataDir(); if (!fs.existsSync(SNAPSHOT_FILE)) return res.status(404).json({ok:false,error:'NO_SNAPSHOT'}); res.type('image/jpeg').send(fs.readFileSync(SNAPSHOT_FILE)); });
app.get('/api/rt7/camera/state', (req,res)=>{ const snap=getSnapshotMeta_(); res.json({ ok:true, version:SERVER_VERSION, snapshot:snap, latest_url: snap ? '/api/rt7/camera/latest.jpg' : '', test_page:'/rt7_snapshot_bridge_test' }); });
app.post('/api/rt7/camera/clear', (req,res)=>{ ensureDataDir(); if (fs.existsSync(SNAPSHOT_FILE)) fs.unlinkSync(SNAPSHOT_FILE); cloudState.last_snapshot=null; const ev=appendEvent({type:'snapshot_clear', message:'Snapshot cleared'}); broadcast('snapshot_clear', ev); res.json({ok:true, event:ev}); });

// Phase8C motion configuration: stored in cloud, ESP32 may poll it later.
let motionConfig = { enabled:false, sensitivity:5, updated_at:null };
app.get('/api/rt7/phase8c/esp_motion/enable', (req,res)=>{ motionConfig.enabled=true; motionConfig.updated_at=nowIso(); res.json({ok:true, motionConfig}); });
app.get('/api/rt7/phase8c/esp_motion/disable', (req,res)=>{ motionConfig.enabled=false; motionConfig.updated_at=nowIso(); res.json({ok:true, motionConfig}); });
app.get('/api/rt7/phase8c/esp_motion/config', (req,res)=>res.json({ok:true, motionConfig}));
app.get('/api/rt7/phase8c/esp_motion/status', (req,res)=>res.json({ok:true, motionConfig, last_motion: readEvents(50).reverse().find(e=>e.type==='motion') || null}));
app.get('/api/rt7/return_fix2/enable', (req,res)=>{ cloudState.ai_enabled=true; res.json({ok:true, ai_enabled:true}); });
app.post('/api/rt7/return_fix2/enable', (req,res)=>{ cloudState.ai_enabled=true; res.json({ok:true, ai_enabled:true}); });
app.get('/api/rt7/return_fix2/disable', (req,res)=>{ cloudState.ai_enabled=false; res.json({ok:true, ai_enabled:false}); });

async function openAiChat(messages, max_tokens=360) {
  const key = safeString(process.env.OPENAI_API_KEY).replace(/^Bearer\s+/i,'').trim();
  if (!key) throw new Error('OPENAI_API_KEY missing');
  const model = safeString(process.env.OPENAI_CHAT_MODEL || 'gpt-4o-mini').trim();
  const r = await fetch('https://api.openai.com/v1/chat/completions', { method:'POST', headers:{ Authorization:'Bearer '+key, 'Content-Type':'application/json' }, body:JSON.stringify({ model, temperature:0.25, max_tokens, messages }) });
  const body = await r.json().catch(()=>({}));
  if (!r.ok) throw new Error(body?.error?.message || ('OpenAI HTTP '+r.status));
  return safeString(body?.choices?.[0]?.message?.content).trim();
}
async function analyzeLatestSnapshot(question) {
  ensureDataDir();
  if (!fs.existsSync(SNAPSHOT_FILE)) return { ok:false, mode:'VISION', error:'NO_CLOUD_SNAPSHOT', answer:'雲端尚未收到 ESP32 上傳的照片。請先讓 ESP32 POST /api/rt7/camera/snapshot。' };
  const b64 = fs.readFileSync(SNAPSHOT_FILE).toString('base64');
  const answer = await openAiChat([{ role:'user', content:[ {type:'text', text: question || '請用繁體中文簡短描述門口畫面，並判斷是否有人臉或可疑狀況。'}, {type:'image_url', image_url:{url:'data:image/jpeg;base64,'+b64} } ] }], 360);
  cloudState.last_vision = { ok:true, question, answer, time:nowIso(), snapshot:cloudState.last_snapshot };
  appendEvent({ type:'vision_qa', question, answer:answer.slice(0,200), message:'vision qa completed' });
  return { ok:true, mode:'VISION', question, answer, snapshot:cloudState.last_snapshot };
}

async function transcribeAudioB64(audio_b64, mime) {
  const key = safeString(process.env.OPENAI_API_KEY).replace(/^Bearer\s+/i,'').trim();
  if (!key) throw new Error('OPENAI_API_KEY missing');
  let b64 = safeString(audio_b64); if (b64.includes(',')) b64 = b64.split(',').pop();
  const buf = Buffer.from(b64, 'base64');
  if (buf.length < 800) throw new Error('AUDIO_TOO_SMALL');
  const blob = new Blob([buf], { type: mime || 'audio/webm' });
  const fd = new FormData(); fd.append('model','whisper-1'); fd.append('language','zh'); fd.append('response_format','json'); fd.append('file', blob, 'rt7_voice.webm');
  const r = await fetch('https://api.openai.com/v1/audio/transcriptions', { method:'POST', headers:{ Authorization:'Bearer '+key }, body:fd });
  const body = await r.json().catch(()=>({}));
  if (!r.ok) throw new Error(body?.error?.message || ('Whisper HTTP '+r.status));
  return safeString(body.text).trim();
}

async function handleVisionQa(req,res) {
  try {
    const q = safeString(req.query.q || req.query.question || req.body?.q || req.body?.question || '請問鏡頭目前看到什麼？');
    const out = await analyzeLatestSnapshot(q);
    broadcast('vision_qa', { ok:out.ok, answer:out.answer, question:q, time:nowIso() });
    res.json(Object.assign({ version:SERVER_VERSION }, out));
  }
  catch(e) { res.status(200).json({ok:false, version:SERVER_VERSION, mode:'VISION', error:String(e.message||e), answer:'雲端 Vision 分析失敗。請確認 Railway 已設定 OPENAI_API_KEY，且已先上傳 Snapshot。'}); }
}
app.get('/api/rt7/phase9a/vision_qa', handleVisionQa);
app.post('/api/rt7/phase9a/vision_qa', handleVisionQa);
app.get('/api/rt7/phase9b/vision_qa', handleVisionQa);
app.post('/api/rt7/phase9b/vision_qa', handleVisionQa);
app.get('/api/rt7/phase9g/vision_qa', handleVisionQa);
app.post('/api/rt7/phase9g/vision_qa', handleVisionQa);
app.get('/api/rt7/phase9i/vision_qa', handleVisionQa);
app.post('/api/rt7/phase9i/vision_qa', handleVisionQa);
app.get('/api/rt7/vision/qa', handleVisionQa);
app.post('/api/rt7/vision/qa', handleVisionQa);
app.get('/api/rt7/phase9d/vision_qa_ping', (req,res)=>res.json({ok:true, version:SERVER_VERSION, openai_key:!!safeString(process.env.OPENAI_API_KEY).trim(), latest_snapshot:getSnapshotMeta_(), last_vision:cloudState.last_vision}));
app.get('/api/rt7/vision/state', (req,res)=>res.json({ok:true, version:SERVER_VERSION, openai_key:!!safeString(process.env.OPENAI_API_KEY).trim(), latest_snapshot:getSnapshotMeta_(), last_vision:cloudState.last_vision}));

// V5.6F1: Mobile music player first-video link.  No Node-RED required.
// The phone asks Railway to resolve the first YouTube video, then opens the watch page.
async function rt7FindFirstYoutubeVideo_(q) {
  q = safeString(q).trim();
  if (!q) return '';
  const searchUrl = 'https://www.youtube.com/results?search_query=' + encodeURIComponent(q);
  try {
    const r = await fetch(searchUrl, {
      headers: {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36',
        'accept-language': 'zh-TW,zh;q=0.9,en;q=0.8'
      }
    });
    const html = await r.text();
    const ids = [];
    let m;
    const re1 = /\"videoId\":\"([a-zA-Z0-9_-]{11})\"/g;
    while ((m = re1.exec(html)) && ids.length < 20) ids.push(m[1]);
    const re2 = /watch\?v=([a-zA-Z0-9_-]{11})/g;
    while ((m = re2.exec(html)) && ids.length < 20) ids.push(m[1]);
    for (const id of ids) {
      if (/^[a-zA-Z0-9_-]{11}$/.test(id)) return 'https://www.youtube.com/watch?v=' + id;
    }
  } catch (e) {
    console.log('[MUSIC][YT_FIRST] fail', e.message);
  }
  return '';
}
app.get('/api/rt7/music/mobile', async (req, res) => {
  const q = safeString(req.query.q || req.query.query || '').trim();
  const mode = safeString(req.query.mode || 'watch').toLowerCase();
  if (!q) return res.status(400).json({ ok:false, version:SERVER_VERSION, error:'missing q' });
  const yt = 'https://www.youtube.com/results?search_query=' + encodeURIComponent(q);
  const ytm = 'https://music.youtube.com/search?q=' + encodeURIComponent(q);
  let watch = '';
  if (mode === 'watch' || mode === 'first' || mode === 'youtube') watch = await rt7FindFirstYoutubeVideo_(q);
  const url = watch || (mode === 'ytmusic' ? ytm : yt);
  const ev = appendEvent({ type:'mobile_music', query:q, target:watch?'watch':'search', message:(watch?'mobile music first video opened: ':'mobile music search opened: ') + q });
  broadcast('mobile_music', ev);
  res.json({ ok:true, version:SERVER_VERSION, action:'mobile_music', query:q, url, watch_url:watch, youtube_url:yt, youtube_music_url:ytm, fallback:!watch, event:ev });
});


// V5.6F2: RT7 internal YouTube player page. It can detect video end and return to the doorbell page.
app.get('/rt7_music_player', (req, res) => {
  const videoId = safeString(req.query.video_id || req.query.v || '').replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 11);
  const q = safeString(req.query.q || '').slice(0, 120);
  const returnUrlRaw = safeString(req.query.return || '/rt7_return_doorbell?from=music');
  let returnUrl = returnUrlRaw.startsWith('/') ? returnUrlRaw : '/rt7_return_doorbell?from=music';
  // V5.8D3: music player must return through the authenticated one-time main gate.
  // Directly returning to /rt7_cloud_original_ui_doorbell may show the login page.
  if (returnUrl === '/rt7_cloud_original_ui_doorbell') returnUrl = '/rt7_return_doorbell?from=music';
  if (!/^[a-zA-Z0-9_-]{11}$/.test(videoId)) return res.status(400).send('missing video_id');
  const h = (v) => String(v || '').replace(/[&<>\"']/g, (m) => ({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',"'":'&#39;'}[m]));
  appendEvent({ type:'mobile_music_player', video_id:videoId, query:q, message:'RT7 music player opened: '+(q||videoId) });
  res.type('html').send(`<!doctype html><html lang="zh-Hant"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<title>RT7 Music Player V5.6F3</title>
<style>
*{box-sizing:border-box}html,body{margin:0;padding:0;background:#06191d;color:#fff;font-family:system-ui,-apple-system,"Noto Sans TC","Microsoft JhengHei",Arial,sans-serif;min-height:100vh}body{display:flex;flex-direction:column}.top{height:64px;background:#0b252b;display:flex;align-items:center;gap:10px;padding:0 14px}.back{border:0;border-radius:10px;background:#334155;color:#fff;font-size:16px;font-weight:900;padding:10px 14px}.title{font-weight:900;line-height:1.2}.sub{font-size:12px;color:#cbd5e1}.wrap{flex:1;display:flex;flex-direction:column;padding:12px;gap:12px}.playerBox{position:relative;width:100%;background:#000;border-radius:14px;overflow:hidden;box-shadow:0 8px 24px rgba(0,0,0,.35);aspect-ratio:16/9}.playerBox iframe,.playerBox #player{position:absolute;inset:0;width:100%;height:100%}.card{background:#102a31;border:1px solid rgba(255,255,255,.12);border-radius:14px;padding:14px;line-height:1.45}.btns{display:grid;grid-template-columns:1fr 1fr;gap:10px}.btn{border:0;border-radius:12px;color:#fff;font-weight:900;font-size:17px;padding:13px 10px}.green{background:#16a34a}.gray{background:#475569}.red{background:#dc2626}.status{font-weight:900;color:#bbf7d0}.small{font-size:13px;color:#cbd5e1;margin-top:6px}.ytlink{color:#93c5fd;word-break:break-all}.alert{display:none;position:fixed;left:12px;right:12px;top:76px;z-index:99;background:#dc2626;color:#fff;border:3px solid #fff;border-radius:18px;padding:16px;box-shadow:0 12px 36px rgba(0,0,0,.55);font-weight:900}.alertTitle{font-size:24px}.alertMsg{font-size:15px;margin-top:6px;line-height:1.45}.alertBtns{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:12px}.alertBtns button{border:0;border-radius:12px;padding:12px;font-size:16px;font-weight:900}.white{background:#fff;color:#991b1b}.dark{background:#111827;color:#fff}
</style></head><body>
<header class="top"><button class="back" onclick="goBack()">← 返回</button><div><div class="title">RT7 音樂播放器</div><div class="sub">播放結束會自動返回；門鈴會立即提醒</div></div></header>
<div class="alert" id="doorbellAlert"><div class="alertTitle">🔔 有人按門鈴</div><div class="alertMsg" id="doorbellMsg">播放音樂中收到門鈴事件。</div><div class="alertBtns"><button class="white" onclick="goBack()">返回門禁</button><button class="dark" onclick="dismissDoorbell()">繼續播放</button></div></div>
<main class="wrap">
  <section class="playerBox"><div id="player"></div></section>
  <section class="card"><div class="status" id="status">正在載入 YouTube 播放器...</div><div class="small">歌曲：${h(q || videoId)}</div><div class="small">若手機禁止自動播放，請在影片中按一次播放；播放結束後仍會自動返回。</div></section>
  <section class="btns"><button class="btn green" onclick="tryPlay()">播放</button><button class="btn red" onclick="goBack()">停止並返回</button></section>
  <section class="card small">備用連結：<br><a class="ytlink" href="https://www.youtube.com/watch?v=${videoId}">https://www.youtube.com/watch?v=${videoId}</a></section>
</main>
<script>
var VIDEO_ID=${JSON.stringify(videoId)};
var RETURN_URL=${JSON.stringify(returnUrl)};
var player=null;
var returned=false;
function setStatus(t){var el=document.getElementById('status'); if(el) el.textContent=t;}
function goBack(){ if(returned) return; returned=true; try{ if(player && player.stopVideo) player.stopVideo(); }catch(e){} location.href=RETURN_URL; }
function tryPlay(){ try{ if(player && player.playVideo){ player.playVideo(); setStatus('播放中。播放結束後會自動返回。'); } }catch(e){ setStatus('請按影片中央播放鍵。'); } }
function onYouTubeIframeAPIReady(){
  player=new YT.Player('player',{
    videoId:VIDEO_ID,
    playerVars:{autoplay:1,playsinline:1,rel:0,enablejsapi:1,origin:location.origin},
    events:{
      onReady:function(ev){ setStatus('播放器已就緒，嘗試自動播放...'); try{ ev.target.playVideo(); }catch(e){ setStatus('手機可能禁止自動播放，請按一次播放。'); } },
      onStateChange:function(ev){
        if(ev.data===YT.PlayerState.PLAYING) setStatus('播放中。播放結束後會自動返回。');
        if(ev.data===YT.PlayerState.PAUSED) setStatus('已暫停，可繼續播放或返回。');
        if(ev.data===YT.PlayerState.ENDED){ setStatus('播放結束，正在返回 RT7 門禁頁...'); setTimeout(goBack,700); }
      },
      onError:function(ev){ setStatus('YouTube 播放錯誤：'+ev.data+'。請使用備用連結或返回。'); }
    }
  });
}

var lastDoorbellCount=null;
var doorbellAlertOpen=false;
function dingDoorbell(){
  try{
    var AC=window.AudioContext||window.webkitAudioContext; if(!AC) return;
    var ctx=new AC(); var o=ctx.createOscillator(); var g=ctx.createGain();
    o.type='sine'; o.frequency.value=880; g.gain.value=0.18; o.connect(g); g.connect(ctx.destination); o.start();
    setTimeout(function(){try{o.frequency.value=660;}catch(e){}},180);
    setTimeout(function(){try{o.stop();ctx.close();}catch(e){}},520);
  }catch(e){}
}
function dismissDoorbell(){ var a=document.getElementById('doorbellAlert'); if(a) a.style.display='none'; doorbellAlertOpen=false; }
function showDoorbellAlert(src){
  doorbellAlertOpen=true;
  try{ if(player && player.pauseVideo) player.pauseVideo(); }catch(e){}
  try{ if(navigator.vibrate) navigator.vibrate([250,120,250]); }catch(e){}
  dingDoorbell();
  var msg=document.getElementById('doorbellMsg');
  if(msg) msg.textContent='收到門鈴：'+(src && (src.message||src.source||src.device_id) ? (src.message||src.source||src.device_id) : '請返回門禁頁查看。');
  var a=document.getElementById('doorbellAlert'); if(a) a.style.display='block';
  setStatus('🔔 收到門鈴，音樂已暫停。');
}
async function pollDoorbell(){
  try{
    var r=await fetch('/api/rt7/doorbell/state?_='+Date.now(),{cache:'no-store'});
    var j=await r.json(); var c=Number((j.state&&j.state.count)||0);
    if(lastDoorbellCount===null){ lastDoorbellCount=c; return; }
    if(c>lastDoorbellCount){ lastDoorbellCount=c; showDoorbellAlert((j.state&&j.state.last)||{}); }
  }catch(e){}
}
function startDoorbellWatch(){
  pollDoorbell(); setInterval(pollDoorbell,1200);
  try{
    var ws=new WebSocket((location.protocol==='https:'?'wss://':'ws://')+location.host+'/ws?role=music_player&fast=doorbell');
    ws.onmessage=function(ev){try{var m=JSON.parse(ev.data); if(m&&m.type==='doorbell') { lastDoorbellCount=null; showDoorbellAlert(m.event||m.payload||m); pollDoorbell(); }}catch(e){}};
    ws.onclose=function(){setTimeout(startDoorbellWatch,2500);};
  }catch(e){}
}
startDoorbellWatch();
var tag=document.createElement('script'); tag.src='https://www.youtube.com/iframe_api'; document.head.appendChild(tag);
setTimeout(function(){ if(!player) setStatus('YouTube API 載入較慢，請稍候或按返回。'); },7000);
</script>
</body></html>`);
});

app.post('/api/rt7/phase9j/voice_vision', async (req,res)=>{
  const started = Date.now();
  try {
    const mode = safeString(req.body?.mode || 'auto').toLowerCase();
    const text = req.body?.text ? safeString(req.body.text).trim() : await transcribeAudioB64(req.body?.audio_b64 || '', req.body?.mime || 'audio/webm');
    if (!text) return res.json({ok:false, error:'NO_TRANSCRIPT', answer:'沒有辨識到文字。'});
    const visionWords = ['鏡頭','畫面','看到','看見','門口','人臉','有人','誰在','照片','影像','辨識'];
    const isVision = mode === 'vision' || (mode !== 'chat' && visionWords.some(w=>text.includes(w)));
    let result;
    if (isVision) result = await analyzeLatestSnapshot(text);
    else {
      const answer = await openAiChat([{role:'system', content:'你是 RT7 AI 語音助理。請用繁體中文、口語、簡潔回答。'}, {role:'user', content:text}], 420);
      result = { ok:true, mode:'CHAT', text, answer };
    }
    cloudState.last_voice = Object.assign({ time:nowIso(), ms:Date.now()-started }, result);
    res.json(Object.assign({ version:SERVER_VERSION, voice_ms:Date.now()-started }, result, { text }));
  } catch(e) { res.status(200).json({ok:false, version:SERVER_VERSION, error:String(e.message||e), answer:'雲端語音/影像 AI 處理失敗。'}); }
});

// Door open queue: phone/Railway queues command; ESP32 actively polls and ACKs.
let pendingCommands = [];
let doorOpenQueueState = { ok:true, queued:0, acked:0, last:null, last_ack:null };
function queueCommand(cmd) {
  const c = Object.assign({ id:'cmd_'+Date.now()+'_'+Math.floor(Math.random()*1000), time:nowIso(), status:'pending' }, cmd);
  pendingCommands.push(c);
  pendingCommands = pendingCommands.slice(-50);
  doorOpenQueueState.queued += 1;
  doorOpenQueueState.last = c;
  broadcast('command', c);
  appendEvent({ type:'command', command:c.command, id:c.id, device_id:c.device_id, message:c.message||c.command });
  return c;
}
function normalizeDoorCommandDeviceId_(id) {
  const raw = safeString(id || '');
  const low = raw.toLowerCase();
  // UI device list historically uses #1, while ESP32 V4.4 polls with rt7-esp32-s3-cam-01.
  // Normalize the primary camera so Railway Queue and ESP32 polling use the same device_id.
  if (!raw || raw === '#1' || raw === '1' || low.includes('rt7 esp32-s3-cam') || low.includes('esp32-s3-cam')) return 'rt7-esp32-s3-cam-01';
  return raw;
}
function commandMatchesDevice_(cmd, id) {
  const pollId = normalizeDoorCommandDeviceId_(id);
  const cmdId = normalizeDoorCommandDeviceId_(cmd?.device_id || '');
  return !pollId || !cmdId || cmdId === pollId;
}
function enqueueDoorOpen(req, res, endpointName) {
  const dev = getCurrentDevice(req);
  const requestedDeviceId = safeString(req.query.device_id || req.query.device || dev.id || '#1') || '#1';
  const deviceId = normalizeDoorCommandDeviceId_(requestedDeviceId);
  // V5.6G4: external door open must not be blocked by stale face/stream/intercom commands.
  // Keep commands for other devices, but clear all pending commands for this device before queueing door_open.
  pendingCommands = pendingCommands.filter(c => !commandMatchesDevice_(c, deviceId));
  const cmd = queueCommand({
    command:'door_open',
    action:'door_open',
    type:'door_open',
    device_id:deviceId,
    requested_device_id:requestedDeviceId,
    endpoint:endpointName || 'door_open_queue',
    pulse_ms:Number(req.query.pulse_ms || 800),
    message:'雲端開門命令已排入佇列，等待 ESP32 輪詢或 WebSocket 直送'
  });

  // V5.6G6: while cloud streaming is active, the ESP32 HTTPS poll may be starved by
  // continuous JPEG upload.  Send the same door_open command over the existing
  // persistent ESP32 WebSocket as a real-time fallback.  The queue remains as backup.
  const wsPayload = {
    ok:true, type:'door_open', command:'door_open', action:'door_open',
    id:cmd.id, command_id:cmd.id, device_id:deviceId, requested_device_id:requestedDeviceId,
    pulse_ms:cmd.pulse_ms, endpoint:endpointName || 'door_open_queue',
    source:'railway_ws_direct_v56g6', time:nowIso()
  };
  const wsSent = rt7SendWsJsonToEsp_(wsPayload);
  cmd.ws_sent = wsSent;
  doorOpenQueueState.last_ws_sent = wsSent;
  cloudState.last_door_open = cmd;
  res.json({ ok:true, mode:'cloud_command_queue_ws_direct', command:cmd, ws_sent:wsSent, requested_device_id:requestedDeviceId, normalized_device_id:deviceId, state:doorOpenQueueState, note:'已排入 Queue 並同步 WS 直送 ESP32；ESP32 輪詢仍作備援' });
}
app.get('/api/rt7/phase9l/door/open', (req,res)=>enqueueDoorOpen(req,res,'phase9l'));
app.post('/api/rt7/phase9l/door/open', (req,res)=>enqueueDoorOpen(req,res,'phase9l_post'));
app.get('/api/rt7/door/open', (req,res)=>enqueueDoorOpen(req,res,'rt7_door_open'));
app.post('/api/rt7/door/open', (req,res)=>enqueueDoorOpen(req,res,'rt7_door_open_post'));
app.get('/api/door/open', (req,res)=>enqueueDoorOpen(req,res,'compat_api_door_open'));
app.get('/api/rt7/door/open/state', (req,res)=>res.json({ ok:true, state:doorOpenQueueState, pending:pendingCommands }));
app.get('/api/rt7/face/command_debug', (req,res)=>res.json({ ok:true, version:SERVER_VERSION, pending:pendingCommands.filter(c=>c.command==='face_snapshot_now'||c.action==='face_snapshot_now'||c.priority==='face_snapshot'), all_pending:pendingCommands.length, state:doorOpenQueueState }));
app.get('/api/rt7/device/commands', (req,res)=>{ const id=normalizeDoorCommandDeviceId_(req.query.device_id||req.query.device||''); const list=id?pendingCommands.filter(c=>commandMatchesDevice_(c,id)):pendingCommands; res.json({ok:true, device_id:id, commands:list, count:list.length, state:doorOpenQueueState}); });
app.get('/api/rt7/device/commands/next', (req,res)=>{
  const id=normalizeDoorCommandDeviceId_(req.query.device_id||req.query.device||'');
  const matches=pendingCommands.filter(c=>commandMatchesDevice_(c,id));
  // V5.6G4: door_open has absolute priority. Old face/stream/intercom commands must not block external open.
  const doorCmd=matches.find(c=>c && (c.command==='door_open' || c.action==='door_open'));
  const faceCmd=matches.find(c=>c && (c.command==='face_snapshot_now' || c.action==='face_snapshot_now' || c.priority==='face_snapshot'));
  const cmd=doorCmd || faceCmd || matches[0] || null;
  res.json({ok:true, version:SERVER_VERSION, device_id:id, command:cmd, has_command:!!cmd, pending:pendingCommands.length, matching:matches.length, door_priority:!!doorCmd, face_priority:!!faceCmd, state:doorOpenQueueState});
});
function ackCommand(req,res){ const id=safeString(req.body?.id||req.query.id); const status=safeString(req.body?.status||req.query.status||'done'); const idx=pendingCommands.findIndex(c=>c.id===id); let cmd=null; if(idx>=0){cmd=pendingCommands[idx]; pendingCommands.splice(idx,1);} doorOpenQueueState.acked+=1; doorOpenQueueState.last_ack={id, status, time:nowIso(), found:!!cmd, command:cmd}; appendEvent({type:'command_ack', id, status, found:!!cmd}); res.json({ok:true, id, status, found:!!cmd, pending:pendingCommands.length, state:doorOpenQueueState}); }
app.get('/api/rt7/device/commands/ack', ackCommand);
app.post('/api/rt7/device/commands/ack', ackCommand);

app.get('/rt7_cloud_phase10_no_nodered', (req,res)=>{
  res.type('html').send(htmlShell('RT7 Phase10 Cloud No Node-RED', `${baseCss}
<style>.phone{max-width:430px;margin:0 auto;background:#fff;min-height:100vh}.videoBox{background:#000;aspect-ratio:4/3;position:relative;display:flex;align-items:center;justify-content:center;color:#cbd5e1;text-align:center;font-weight:900}.videoBox img{width:100%;height:100%;object-fit:cover}.doorAlert{display:none;background:#fff7ed;border:1px solid #fed7aa;color:#9a3412;border-radius:12px;padding:12px;margin:10px;font-size:22px;font-weight:900;text-align:center}.rowbtn{display:grid;grid-template-columns:1fr 1fr;gap:8px}.small{font-size:12px;color:#64748b}.mic{width:118px;height:118px;border-radius:50%;border:3px solid #cbd5e1;background:#eef2f7;font-size:62px}.pill{display:inline-block;border-radius:999px;padding:4px 9px;background:#e2e8f0;font-weight:900}</style>
<div class="phone">
<header class="top"><h1>RT7 PHASE10</h1><p>Railway 雲端影像 / 對講 / AI 門禁（無 Node-RED）</p></header>
<div class="wrap">
<section class="card"><b>目前設備</b><select id="deviceSel"></select><p class="small">區網 IP 在 Railway 通常無法被反向連線；建議 ESP32 主動上傳 snapshot / doorbell / commands polling。</p></section>
<section class="videoBox"><img id="snap" style="display:none"><div id="empty">等待 ESP32 上傳照片<br><span class="small">POST /api/rt7/camera/snapshot</span></div></section>
<div id="doorAlert" class="doorAlert">🔔 有人按門鈴</div>
<section class="card"><div>狀態：<span class="pill" id="status">ready</span></div><div>回答：<b id="answer">雲端待機中</b></div></section>
<section class="card rowbtn"><button class="btn green" onclick="refreshSnap()">更新影像</button><button class="btn" onclick="askVision()">問鏡頭</button><button class="btn red" onclick="openDoor()">開門</button><button class="btn gray" onclick="testDoorbell()">測試門鈴</button><button class="btn" onclick="enableAudio()">啟用提示音</button><button class="btn gray" onclick="loadState(true)">更新狀態</button></section>
<section class="card" style="text-align:center"><button class="mic" onclick="voiceText()">🎙️</button><p class="small">第一版先支援文字測試；手機錄音可 POST /api/rt7/phase9j/voice_vision。</p></section>
<pre class="status" id="log">ready</pre>
</div></div>
<script>
let DEVICES=[], audioCtx=null, audioOK=false, lastCount=null; const $=id=>document.getElementById(id);
function log(o){$('log').textContent='['+new Date().toLocaleTimeString()+'] '+(typeof o==='string'?o:JSON.stringify(o,null,2))+'\\n'+$('log').textContent}
async function j(url,opt){const r=await fetch(url+(url.includes('?')?'&':'?')+'_='+Date.now(),Object.assign({cache:'no-store'},opt||{}));const t=await r.text();try{return JSON.parse(t)}catch(e){return{ok:r.ok,raw:t}}}
function beep(f,d,t){if(!audioCtx)return;const o=audioCtx.createOscillator(),g=audioCtx.createGain();o.frequency.value=f;g.gain.value=.18;o.connect(g);g.connect(audioCtx.destination);o.start(audioCtx.currentTime+t);o.stop(audioCtx.currentTime+t+d)}
function ding(){if(audioOK){beep(880,.18,0);beep(660,.22,.26)}}
async function enableAudio(){audioCtx=audioCtx||new(window.AudioContext||window.webkitAudioContext)();await audioCtx.resume();audioOK=true;$('answer').textContent='提示音已啟用';ding()}
async function loadDevices(){const d=await j('/api/rt7/devices/list');DEVICES=d.devices||[];$('deviceSel').innerHTML=(DEVICES.length?DEVICES:[{id:'#1',name:'RT7'}]).map(x=>'<option value="'+(x.id||'')+'">'+(x.id||'')+' / '+(x.name||'')+(x.ip?' / '+x.ip:'')+'</option>').join('')}
async function refreshSnap(){const s=await j('/api/rt7/camera/state');log(s); if(s.latest_url){$('snap').src=s.latest_url+'?_='+Date.now();$('snap').style.display='block';$('empty').style.display='none';$('status').textContent='snapshot';}else{$('answer').textContent='尚無雲端照片'}}
async function loadState(manual){const s=await j('/api/rt7/doorbell/state');log(s); const c=Number(s.state?.count||0), last=s.state?.last||{}; if(lastCount===null)lastCount=c; else if(c>lastCount){$('doorAlert').style.display='block';$('answer').textContent='收到門鈴';ding();setTimeout(()=>$('doorAlert').style.display='none',5000)} lastCount=c; if(manual&&last.message)$('answer').textContent=last.message;}
async function testDoorbell(){log(await j('/api/test/doorbell'));loadState(false)}
async function openDoor(){const r=await j('/api/rt7/phase9l/door/open?device_id='+encodeURIComponent($('deviceSel').value));log(r);$('answer').textContent=r.note||r.message||'開門命令已送出'}
async function askVision(){const q=prompt('要問鏡頭什麼？','門口目前看到什麼？')||''; if(!q)return; $('answer').textContent='Vision 分析中...'; const r=await j('/api/rt7/phase9i/vision_qa?q='+encodeURIComponent(q));log(r);$('answer').textContent=r.answer||r.error||'無回應'}
async function voiceText(){const t=prompt('輸入要測試的語音文字','請問鏡頭看到什麼？')||''; if(!t)return; const r=await j('/api/rt7/phase9j/voice_vision',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({text:t,mode:'auto'})});log(r);$('answer').textContent=r.answer||r.error||'無回應'}
function ws(){try{const w=new WebSocket((location.protocol==='https:'?'wss://':'ws://')+location.host+'/ws');w.onmessage=e=>{try{const m=JSON.parse(e.data); if(['doorbell','snapshot','command'].includes(m.type)){log(m); if(m.type==='doorbell'){loadState(false);ding()} if(m.type==='snapshot')refreshSnap();}}catch(_){}};w.onclose=()=>setTimeout(ws,3000)}catch(e){}}
loadDevices().then(()=>{refreshSnap();loadState(true)});setInterval(()=>loadState(false),2500);ws();
</script>`));
});
app.get('/rt7_independent_full_video_intercom', (req,res)=>res.redirect(307,'/rt7_cloud_phase10_no_nodered'));
app.get('/rt7_face_guard', (req,res)=>res.redirect(307,'/rt7_cloud_phase10_no_nodered'));
app.get('/rt7_admin_home', (req,res)=>res.redirect(307,'/rt7_cloud_admin'));

app.get('/rt7_device_manager', (req,res)=>{
  const page = String.raw`${baseCss}
<style>
.dmTop{display:flex;gap:10px;align-items:center;justify-content:space-between;flex-wrap:wrap}.dmGrid{display:grid;grid-template-columns:72px 1.1fr 1.3fr 80px;gap:8px;align-items:center}.dmHead{font-weight:900;color:#334155}.dmGrid input{width:100%;height:42px;border:1px solid #94a3b8;border-radius:10px;padding:0 10px;font-size:15px}.dmGrid label{display:flex;gap:6px;align-items:center;font-weight:900}.dmGrid input[type=checkbox]{width:22px;height:22px}.preview{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px}.devCard{border:1px solid #d8e0e8;border-radius:12px;padding:12px;background:#f8fafc}.devCard b{font-size:17px}.devCard .ip{font-family:ui-monospace,Consolas,monospace;color:#0f172a;word-break:break-all}.current{outline:3px solid #22c55e;background:#ecfdf5}@media(max-width:640px){.dmGrid{grid-template-columns:54px 1fr}.dmHead{display:none}.span2{grid-column:span 2}.dmGrid input{height:44px}.dmTop .btn{width:100%}}
</style>
<header class="top"><h1>RT7 V5.6D1 DEVICE MANAGER</h1><p>Railway 設備名稱 / IP 管理：#1 ~ #4</p></header>
<div class="alert" id="doorbellAlert"><div class="alertTitle">🔔 有人按門鈴</div><div class="alertMsg" id="doorbellMsg">播放音樂中收到門鈴事件。</div><div class="alertBtns"><button class="white" onclick="goBack()">返回門禁</button><button class="dark" onclick="dismissDoorbell()">繼續播放</button></div></div>
<main class="wrap">
<section class="card dmTop"><div><h2 style="margin:0">設備管理頁</h2><div class="muted">儲存位置：<code>data/devices.json</code></div></div><div><a class="btn gray" href="/rt7_cloud_original_ui_doorbell">回手機門禁頁</a><a class="btn" href="/rt7_cloud_admin">管理頁</a></div></section>
<section class="card"><h3>編輯 #1 ~ #4 設備</h3><div class="dmGrid" id="devForm"><div class="dmHead">編號</div><div class="dmHead">設備名稱</div><div class="dmHead">ESP32 IP / Host</div><div class="dmHead">啟用</div></div><p class="muted">IP 請填 <code>192.168.x.x</code> 或主機名稱，不需要加 <code>http://</code>。</p><button class="btn green" id="saveBtn">儲存 devices.json</button><button class="btn" id="reloadBtn">重新載入</button><button class="btn gray" id="resetBtn">恢復預設 #1~#4</button></section>
<section class="card"><h3>手機頁面自動載入預覽</h3><div id="preview" class="preview"></div></section>
<section class="card"><h3>測試</h3><button class="btn green" id="phoneBtn">開啟手機門禁頁</button><button class="btn" id="stateBtn">讀取目前設備狀態</button><pre class="status" id="log">ready</pre></section>
</main>
<script>
const IDS = ['#1','#2','#3','#4'];
let DEVICES = [];
function E(id){ return document.getElementById(id); }
function esc(s){ return String(s || '').replace(/[&<>"']/g, function(m){ return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]; }); }
function log(x){ var line = '[' + new Date().toLocaleTimeString() + '] ' + (typeof x === 'string' ? x : JSON.stringify(x,null,2)); E('log').textContent = line + String.fromCharCode(10) + E('log').textContent.slice(0,5000); }
async function j(url,opt){ var r = await fetch(url + (url.indexOf('?') >= 0 ? '&' : '?') + '_=' + Date.now(), Object.assign({cache:'no-store'}, opt || {})); var t = await r.text(); try { return JSON.parse(t); } catch(e) { return {ok:r.ok,status:r.status,raw:t}; } }
function defaults(){ return IDS.map(function(id,i){ return {id:id,name:i===0?'RT7 ESP32-S3-CAM':id,ip:'',enabled:true,note:''}; }); }
function norm(list){ var m = new Map((list || []).map(function(d){ return [d.id,d]; })); return IDS.map(function(id,i){ return Object.assign(defaults()[i], m.get(id) || {}); }); }
function render(){
  DEVICES = norm(DEVICES);
  var root = E('devForm');
  var html = '<div class="dmHead">編號</div><div class="dmHead">設備名稱</div><div class="dmHead">ESP32 IP / Host</div><div class="dmHead">啟用</div>';
  DEVICES.forEach(function(d,i){
    html += '<div><b>'+esc(d.id)+'</b></div>';
    html += '<div><input id="name'+i+'" value="'+esc(d.name)+'" placeholder="設備名稱"></div>';
    html += '<div class="span2"><input id="ip'+i+'" value="'+esc(d.ip)+'" placeholder="例如 192.168.0.179"></div>';
    html += '<div><label><input id="en'+i+'" type="checkbox" '+(d.enabled!==false?'checked':'')+'> ON</label></div>';
  });
  root.innerHTML = html;
  renderPreview();
}
function cleanIp(v){ v = String(v || '').trim(); v = v.replace('http://','').replace('https://',''); var slash = v.indexOf('/'); if(slash >= 0) v = v.slice(0, slash); return v; }
function collect(){ return IDS.map(function(id,i){ return {id:id,name:(E('name'+i).value || id).trim(),ip:cleanIp(E('ip'+i).value),enabled:E('en'+i).checked,note:'',version:(DEVICES[i]&&DEVICES[i].version)||'',last_online:(DEVICES[i]&&DEVICES[i].last_online)||''}; }); }
function renderPreview(){ var cur = localStorage.getItem('RT7_CURRENT_DEVICE_ID') || '#1'; E('preview').innerHTML = DEVICES.map(function(d){ return '<div class="devCard '+(d.id===cur?'current':'')+'"><b>'+esc(d.id)+' '+esc(d.name)+'</b><div class="ip">'+(d.ip?esc(d.ip):'<span class="muted">尚未設定 IP</span>')+'</div><div>'+(d.enabled!==false?'✅ 啟用':'⏸ 關閉')+'</div><button class="btn green" data-dev-id="'+esc(d.id)+'">切換到 '+esc(d.id)+'</button></div>'; }).join(''); Array.from(E('preview').querySelectorAll('button[data-dev-id]')).forEach(function(b){ b.onclick = function(){ selectDevice(b.getAttribute('data-dev-id')); }; }); }
async function loadDevices(){ try{ var d = await j('/api/rt7/devices/list'); DEVICES = norm(d.devices || []); render(); log(d); }catch(e){ log('載入失敗：'+e.message); DEVICES = defaults(); render(); } }
async function saveDevices(){ var payload = {devices:collect()}; var d = await j('/api/rt7/devices/save',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)}); DEVICES = norm(d.devices || payload.devices); render(); log(d); }
async function selectDevice(id){ localStorage.setItem('RT7_CURRENT_DEVICE_ID',id); await j('/api/rt7/device/set',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({device_id:id})}); renderPreview(); log('目前手機頁預設設備：'+id); }
function resetDefaults(){ DEVICES = defaults(); render(); log('已恢復畫面預設，按「儲存 devices.json」才會寫入。'); }
function openPhone(){ location.href='/rt7_cloud_original_ui_doorbell'; }
async function readState(){ log(await j('/api/rt7/device/state')); }
E('saveBtn').onclick = saveDevices; E('reloadBtn').onclick = loadDevices; E('resetBtn').onclick = resetDefaults; E('phoneBtn').onclick = openPhone; E('stateBtn').onclick = readState;
loadDevices();
</script>`;
  res.type('html').send(htmlShell('RT7 V5.6D1 Device Manager', page));
});

app.get('/rt7_log_viewer', (req,res)=>res.redirect(307,'/rt7_cloud_admin'));
// /rt7_user_manager and /rt7_event_log are defined above; do not redefine here.



// -----------------------------------------------------------------------------
// V4.1 Maintenance Mapping API
// Purpose: keep Railway Node.js and original Node-RED flow comparable.
// -----------------------------------------------------------------------------
const NODE_RED_MAPPING = [
  { group:'00 Core', status:'done', nodered:'GET /rt7_independent_full_video_intercom', railway:'GET /rt7_cloud_phase10_no_nodered', test:'Open phone page; verify original UI style and buttons' },
  { group:'01 Doorbell', status:'done', nodered:'POST /api/rt7/phase9n/doorbell/event, POST /api/rt7/doorbell/ring, GET /api/rt7/doorbell/state', railway:'same API names retained', test:'ESP32 POST -> phone UI shows event and dingdong plays' },
  { group:'02 Event Log', status:'done', nodered:'GET /api/rt7/events/latest, GET /api/rt7/events/clear, /rt7_event_log', railway:'same API names retained; stored in data/rt7_event_log.jsonl', test:'GET latest, clear, then trigger doorbell' },
  { group:'03 Device Manager', status:'done', nodered:'GET /api/rt7/device/state, POST /api/rt7/device/set, /rt7_device_manager', railway:'same API names retained; stored in data/rt7_devices.json', test:'save device IP/name and reload admin page' },
  { group:'04 Snapshot Bridge', status:'done-v4.2', nodered:'ESP32 /api/camera/snapshot via Node-RED local proxy', railway:'POST /api/rt7/camera/snapshot; POST /api/rt7/camera/snapshot_json; GET /api/rt7/camera/latest.jpg; GET /api/rt7/camera/state; GET /rt7_snapshot_bridge_test', test:'ESP32 actively uploads JPEG/base64; phone page refreshes latest image; clear endpoint works' },
  { group:'04B Original UI Snapshot', status:'done-v4.3', nodered:'Original phone UI camera block / Node-RED image refresh', railway:'GET /rt7_cloud_original_ui_doorbell now displays /api/rt7/camera/latest.jpg and auto-refreshes on snapshot WebSocket event', test:'Open original UI after ESP32 snapshot POST; verify image appears in black video area' },
  { group:'04C Live Stream Bridge', status:'done-v4.7e-ws-upload-native-mjpeg-7fps', nodered:'Original Node-RED MJPEG / live camera view', railway:'ESP32 WebSocket binary JPEG upload to /ws; HTTP POST /api/rt7/camera/frame fallback; GET /api/rt7/camera/stream.mjpg native browser MJPEG output; /rt7_cloud_original_ui_doorbell uses native MJPEG live stream', test:'ESP32 targets about 10 FPS via WebSocket upload; phone UI uses native MJPEG for Android Chrome compatibility; Snapshot remains fallback' },
  { group:'05 Vision QA', status:'partial', nodered:'GET /api/rt7/phase9i/vision_qa', railway:'GET /api/rt7/phase9i/vision_qa uses latest uploaded snapshot + OpenAI if OPENAI_API_KEY exists', test:'Upload snapshot, ask question, verify answer' },
  { group:'06 Voice Vision Router', status:'partial', nodered:'POST /api/rt7/phase9j/voice_vision', railway:'POST /api/rt7/phase9j/voice_vision text-mode scaffold; audio upload reserved', test:'POST {text:"請問鏡頭看到什麼"}' },
  { group:'07 Door Open Queue', status:'done-v4.4', nodered:'GET /api/rt7/phase9l/door/open direct local ESP32 request', railway:'GET /api/rt7/phase9l/door/open queues command; ESP32 polls /api/rt7/device/commands', test:'GET door/open then GET device/commands' },
  { group:'08 Phase6C3 Plugin', status:'stub', nodered:'phase6c3_plugin ping/plugins/motion/face endpoints', railway:'compatible endpoints kept; advanced face cache/enroll needs next incremental port', test:'GET ping/plugins/state; do not enable full face match yet' },
  { group:'09 Intercom Audio', status:'stub', nodered:'/api/ind_full/audio/* local proxy to ESP32 audio endpoints', railway:'/api/ind_full/audio/* returns compatibility JSON / queue scaffold', test:'Call begin/end endpoints; later add WebSocket PCM bridge one step at a time' }
];
app.get('/api/rt7/mapping', (req,res)=>res.json({ ok:true, version:SERVER_VERSION, mapping:NODE_RED_MAPPING }));
app.get('/api/rt7/mapping/status', (req,res)=>{
  const count = NODE_RED_MAPPING.reduce((a,x)=>{ a[x.status]=(a[x.status]||0)+1; return a; },{});
  res.json({ ok:true, version:SERVER_VERSION, count, next_recommended:'V4.4 Door Open Queue only after V4.3 original UI snapshot passes' });
});
app.get('/rt7_mapping', (req,res)=>{
  const rows = NODE_RED_MAPPING.map(x=>`<tr><td>${x.group}</td><td><b>${x.status}</b></td><td><code>${x.nodered}</code></td><td><code>${x.railway}</code></td><td>${x.test}</td></tr>`).join('');
  res.type('html').send(htmlShell('RT7 Node-RED / Railway Mapping', `${baseCss}<div class="wrap"><h1>RT7 Node-RED / Railway API 對照表</h1><p>版本：${SERVER_VERSION}</p><p><a class="btn" href="/rt7_cloud_phase10_no_nodered">回手機頁</a> <a class="btn gray" href="/api/rt7/mapping">JSON</a></p><table border="1" cellspacing="0" cellpadding="8" style="width:100%;border-collapse:collapse;background:#fff"><thead><tr><th>功能</th><th>狀態</th><th>Node-RED 原始路由</th><th>Railway 對應 API</th><th>測試</th></tr></thead><tbody>${rows}</tbody></table></div>`));
});



// -----------------------------------------------------------------------------
// V4.8 Stream Compare Test: LAN direct stream vs Railway cloud stream.
// Goal: keep product target no Node-RED / no Tailscale, but measure whether LAN
// direct ESP32 MJPEG is smooth before comparing with cloud relay.
// -----------------------------------------------------------------------------
app.get('/rt7_stream_compare_test', (req, res) => {
  res.type('html').send(`<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no"><title>RT7 V4.8 Stream Compare</title>
<style>
body{font-family:system-ui,-apple-system,"Noto Sans TC","Microsoft JhengHei",Arial,sans-serif;margin:0;background:#f3f6f8;color:#17262a} .wrap{max-width:980px;margin:0 auto;padding:14px} .top{background:#0d2a30;color:#fff;padding:18px;text-align:center;font-weight:900} .card{background:#fff;border:1px solid #d7dee5;border-radius:14px;padding:14px;margin:12px 0;box-shadow:0 2px 10px rgba(0,0,0,.05)} button{border:0;border-radius:12px;padding:12px 14px;margin:4px;color:#fff;font-weight:900;font-size:16px} .blue{background:#1583d8}.green{background:#16a34a}.red{background:#dc2626}.gray{background:#475569}.orange{background:#d97706} select,input{font-size:16px;padding:10px;border:1px solid #9aa8b4;border-radius:10px;width:100%;box-sizing:border-box;margin:5px 0}.grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}.video{background:#000;min-height:260px;display:flex;align-items:center;justify-content:center;border-radius:12px;overflow:hidden}.video img{width:100%;height:100%;min-height:260px;object-fit:contain}.label{font-weight:900;margin:8px 0;color:#7a2a19}.small{font-size:13px;color:#64748b;line-height:1.6} pre{white-space:pre-wrap;background:#0b1220;color:#cbd5e1;border-radius:12px;padding:10px;max-height:260px;overflow:auto}.ok{color:#16a34a;font-weight:900}.warn{color:#d97706;font-weight:900}@media(max-width:720px){.grid{grid-template-columns:1fr}.video{min-height:220px}.video img{min-height:220px}}
</style></head><body><div class="top">RT7 V4.8 內網 / 外網影像串流比較測試</div><div class="wrap">
<div class="card"><b>測試目的</b><div class="small">不使用 Node-RED、不使用 Tailscale。先測手機與 ESP32 同 Wi-Fi 時，直接讀 ESP32 <code>/api/camera/stream</code> 是否順暢；再比較 Railway 雲端轉發 <code>/api/rt7/camera/stream.mjpg</code>。若內網順、外網慢，代表瓶頸在雲端轉發路徑，不是 ESP32 攝影機本身。</div></div>
<div class="card"><label>選擇/輸入 ESP32 IP</label><select id="deviceSel"></select><input id="espIp" placeholder="例如 192.168.0.179"><div><button class="blue" onclick="loadDevices()">重新讀取設備</button><button class="green" onclick="saveIp()">套用 IP</button><button class="gray" onclick="loadState()">讀取狀態</button></div></div>
<div class="card"><div><button class="orange" onclick="startLan()">1. 內網直連 ESP32 串流</button><button class="green" onclick="startCloud()">2. Railway 雲端串流</button><button class="blue" onclick="startBoth()">同時比較</button><button class="red" onclick="stopAll()">停止</button></div><div class="small">手機若不在同一 Wi-Fi，內網直連會失敗，這是正常。一般使用者外網仍走 Railway。</div></div>
<div class="grid"><div class="card"><div class="label">內網直連 ESP32 /api/camera/stream</div><div class="video"><img id="lanImg"><span id="lanEmpty" style="color:#fff">尚未開始</span></div><div id="lanInfo" class="small"></div></div><div class="card"><div class="label">外網 / Railway /api/rt7/camera/stream.mjpg</div><div class="video"><img id="cloudImg"><span id="cloudEmpty" style="color:#fff">尚未開始</span></div><div id="cloudInfo" class="small"></div></div></div>
<div class="card"><b>判讀方式</b><div class="small">A. 內網直連順、Railway 慢：ESP32 攝影機正常，雲端 relay 是瓶頸。<br>B. 內網也慢：需回頭調 ESP32 camera frame size / jpeg quality / Wi-Fi。<br>C. 內網不能開但 Railway 可開：手機不在同 Wi-Fi 或瀏覽器擋 HTTP 私網影像。</div></div>
<div class="card"><b>狀態</b><pre id="log">ready</pre></div>
</div><script>
function $(id){return document.getElementById(id)}
function log(x){$('log').textContent=(typeof x==='string'?x:JSON.stringify(x,null,2))+'\n\n'+$('log').textContent.slice(0,4000)}
async function j(u){const r=await fetch(u+(u.includes('?')?'&':'?')+'_='+Date.now(),{cache:'no-store'});const t=await r.text();try{return JSON.parse(t)}catch(e){return{ok:r.ok,raw:t}}}
let espIp='192.168.0.179';
async function loadDevices(){const d=await j('/api/devices');const devs=d.devices||[];$('deviceSel').innerHTML=devs.map(x=>'<option value="'+(x.ip||'')+'">'+(x.id||'')+' / '+(x.name||'')+' / '+(x.ip||'')+'</option>').join(''); if(devs[0]&&devs[0].ip){espIp=devs[0].ip;$('espIp').value=espIp;} log(d)}
function saveIp(){espIp=($('espIp').value||$('deviceSel').value||espIp).trim().replace(/^https?:\/\//,'').replace(/\/.*/,'');$('espIp').value=espIp;log('ESP32 IP = '+espIp)}
$('deviceSel').addEventListener('change',()=>{if($('deviceSel').value){$('espIp').value=$('deviceSel').value;saveIp();}})
function startLan(){saveIp();$('lanEmpty').style.display='none';$('lanImg').onerror=()=>{$('lanInfo').innerHTML='<span class="warn">內網直連失敗：請確認手機與 ESP32 同 Wi-Fi，或瀏覽器是否擋 HTTP 私網影像。</span>';};$('lanImg').onload=()=>{$('lanInfo').innerHTML='<span class="ok">內網直連已啟動。</span> 這一路徑不經 Railway。';};$('lanImg').src='http://'+espIp+'/api/camera/stream?_lan='+Date.now();$('lanInfo').textContent='連線中： http://'+espIp+'/api/camera/stream';}
async function startCloud(){await j('/api/rt7/camera/stream/start');$('cloudEmpty').style.display='none';$('cloudImg').onerror=()=>{$('cloudInfo').innerHTML='<span class="warn">Railway MJPEG 載入失敗</span>';};$('cloudImg').onload=()=>{$('cloudInfo').innerHTML='<span class="ok">Railway 雲端串流已啟動。</span>';};$('cloudImg').src='/api/rt7/camera/stream.mjpg?device_id='+encodeURIComponent(selectedDeviceId||'#1')+'&_cloud='+Date.now();$('cloudInfo').textContent='連線中：/api/rt7/camera/stream.mjpg';}
function startBoth(){startLan();startCloud();}
async function stopAll(){$('lanImg').removeAttribute('src');$('lanImg').src='';$('cloudImg').removeAttribute('src');$('cloudImg').src='';$('lanEmpty').style.display='block';$('cloudEmpty').style.display='block';await j('/api/rt7/camera/stream/stop');log('stopped')}
async function loadState(){const s=await j('/api/rt7/camera/stream/state');log(s)}
loadDevices().then(loadState);setInterval(loadState,5000);
</script></body></html>`);
});



// -----------------------------------------------------------------------------
// V4.8F Auto LAN/Cloud Stream Test page
// Production idea: no Node-RED, no Tailscale.  Browser first tries LAN direct
// ESP32 MJPEG, then falls back to Railway cloud MJPEG if LAN is unavailable.
// -----------------------------------------------------------------------------
app.get('/rt7_auto_stream_test', (req, res) => {
  res.type('html').send(`<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no"><title>RT7 V4.8F3 Auto LAN/Cloud Stream</title>
<style>body{font-family:system-ui,-apple-system,"Noto Sans TC","Microsoft JhengHei",Arial,sans-serif;margin:0;background:#f5f7fb;color:#17262a}.wrap{max-width:720px;margin:0 auto;padding:14px}.top{background:#0d2a30;color:#fff;padding:18px;text-align:center;font-weight:900}.card{background:#fff;border:1px solid #d7dee5;border-radius:14px;padding:14px;margin:12px 0}.video{background:#000;aspect-ratio:4/3;border-radius:14px;overflow:hidden;display:flex;align-items:center;justify-content:center;color:#fff}.video img{width:100%;height:100%;object-fit:contain;pointer-events:none}.btn{width:100%;border:0;border-radius:12px;padding:14px;margin:6px 0;color:#fff;font-weight:900;font-size:18px;background:#0b84d8}.red{background:#dc2626}.green{background:#16a34a}.orange{background:#d97706}input{width:100%;box-sizing:border-box;padding:12px;border:1px solid #9aa8b4;border-radius:10px;font-size:18px}.badge{display:inline-block;border-radius:999px;padding:5px 10px;color:#fff;background:#64748b;font-weight:900}.lan{background:#16a34a}.cloud{background:#d97706}.small{font-size:13px;color:#64748b;line-height:1.55}pre{white-space:pre-wrap;background:#0b1220;color:#d8f2ff;border-radius:12px;padding:10px;max-height:260px;overflow:auto}</style></head><body><div class="top">RT7 V4.8F3 自動內網/雲端串流</div><div class="wrap">
<div class="card"><b>ESP32 IP</b><input id="ip" value="192.168.0.179"><div class="small">在家同 Wi-Fi 會自動直連 ESP32；外網或失敗時自動切 Railway 雲端。</div></div>
<div class="card"><button class="btn green" id="startBtn">開始影像（自動判斷）</button><button class="btn red" id="stopBtn">停止影像</button><p>目前模式：<span id="mode" class="badge">AUTO</span></p></div>
<div class="video"><img id="img"><span id="empty">尚未開始</span></div>
<div class="card"><b>說明</b><div class="small">LAN = 手機直接讀 ESP32 <code>/api/camera/stream</code>，流暢。CLOUD = Railway <code>/api/rt7/camera/stream.mjpg</code>，遠端可用但 FPS 較低。</div></div>
<pre id="log">ready</pre></div><script>
const $=id=>document.getElementById(id); let wanted=false;
function log(s){$('log').textContent='['+new Date().toLocaleTimeString()+'] '+s+'\\n'+$('log').textContent.slice(0,3000)}
async function j(u){const r=await fetch(u+(u.includes('?')?'&':'?')+'_='+Date.now(),{cache:'no-store'});const t=await r.text();try{return JSON.parse(t)}catch(e){return{ok:r.ok,raw:t}}}
function setMode(m){$('mode').textContent=m;$('mode').className='badge '+(m==='LAN'?'lan':m==='CLOUD'?'cloud':'')}
function lanUrl(){return 'http://'+$('ip').value.trim().replace(/^https?:\/\//,'').replace(/\/.*$/,'')+'/api/camera/stream'}
function probe(url,ms){return new Promise(resolve=>{const im=new Image();let done=false;const fin=ok=>{if(done)return;done=true;try{im.src=''}catch(e){}resolve(ok)};im.onload=()=>fin(true);im.onerror=()=>fin(false);setTimeout(()=>fin(false),ms||2600);im.src=url+'?_probe='+Date.now();});}
async function cloud(){await j('/api/rt7/camera/stream/start');$('empty').style.display='none';$('img').onerror=()=>log('Cloud MJPEG error');$('img').src='/api/rt7/camera/stream.mjpg?device_id='+encodeURIComponent(selectedDeviceId||'#1')+'&_cloud='+Date.now();setMode('CLOUD');log('CLOUD mode: Railway remote stream');}
async function lan(url){$('empty').style.display='none';$('img').onerror=()=>{log('LAN lost -> CLOUD fallback');cloud();};$('img').src=url+'?_lan='+Date.now();setMode('LAN');log('LAN mode: direct ESP32 stream '+url);}
async function start(){wanted=true;setMode('AUTO');log('probe LAN...');const u=lanUrl();if(await probe(u,2600)) await lan(u); else await cloud();}
async function stop(){wanted=false;$('img').removeAttribute('src');$('img').src='';$('empty').style.display='block';setMode('AUTO');await j('/api/rt7/camera/stream/stop');log('stopped')}
$('startBtn').addEventListener('click',start);$('stopBtn').addEventListener('click',stop);
document.addEventListener('visibilitychange',()=>{if(document.visibilityState==='visible'&&wanted)start();});
</script></body></html>`);
});
app.get('/api/rt7/stream/compare/state', (req, res) => {
  streamViewerPrune_();
  const dev = getCurrentDevice(req);
  res.json({ ok:true, version:SERVER_VERSION, lan:{ url: dev.base_url ? dev.base_url + '/api/camera/stream' : '', note:'手機與 ESP32 同 Wi-Fi 時測試；不經 Node-RED/Tailscale/Railway relay' }, cloud:{ url:'/api/rt7/camera/stream.mjpg', stream:liveStreamState, snapshot:getSnapshotMeta_() } });
});

app.get('/api/rt7/intercom/ws/state', (req,res)=>res.json({ ok:true, version:SERVER_VERSION, ws:rt7IntercomWsState_() }));
app.get('/api/rt7/intercom/ws/probe', (req,res)=>{ const label=safeString(req.query.label||'probe'); const n=rt7SendToEspIntercom_(JSON.stringify({type:'intercom_probe',role:'intercom_probe_http',device_id:safeString(req.query.device_id||'#1'),label,t:Date.now(),version:SERVER_VERSION})); res.json({ok:true,version:SERVER_VERSION,esp:n,state:rt7IntercomWsState_()}); });


app.get('/api/rt7/face/live_frame_state', (req,res)=>{
  const latest = rt7GetLatestWithMeta_();
  res.json({ ok:true, version:SERVER_VERSION, latest, stream:liveStreamState, intercom_ws:rt7IntercomWsState_() });
});

app.get('/api/rt7/face/snapshot_trigger_test', (req,res)=>{
  const requestId = 'manual_face_snap_' + Date.now();
  const cmd = queueCommand({ command:'face_snapshot_now', action:'face_snapshot_now', request_id:requestId, device_id:'rt7-esp32-s3-cam-01', message:'manual face snapshot trigger test' });
  const wsSent = rt7SendWsJsonToEsp_({ type:'face_snapshot_now', command:'face_snapshot_now', request_id:requestId, phase:'V54O', manual:true, time:nowIso() });
  res.json({ ok:true, version:SERVER_VERSION, request_id:requestId, ws_sent:wsSent, command:cmd, state:rt7IntercomWsState_(), latest_snapshot:getSnapshotMeta_() });
});


wss.on('connection', (ws, req) => {
  ws.rt7Role = 'control';
  ws.rt7DeviceId = '';
  try {
    const u = new URL(req.url || '/ws', 'http://localhost');
    const qRole = safeString(u.searchParams.get('role') || '');
    const qDev = safeString(u.searchParams.get('device_id') || u.searchParams.get('device') || '');
    const qPcmRole = safeString(u.searchParams.get('pcm_role') || '');
    if (qRole) ws.rt7Role = qRole;
    if (qDev) ws.rt7DeviceId = qDev;
    if (qPcmRole) { ws.rt7PcmRole = qPcmRole; ws.rt7PcmClient = rt7IsEspPcmRole_(qPcmRole); }
    if (rt7IsEspPcmRole_(qRole)) { ws.rt7PcmClient = true; if (!ws.rt7PcmRole) ws.rt7PcmRole = 'esp32_pcm'; }
  } catch (_) {}
  try { ws.send(JSON.stringify({ ok: true, type: 'hello', version: SERVER_VERSION, time: nowIso(), ws_frame:true, intercom_ws:rt7IntercomWsState_() })); } catch (_) {}
  ws.on('message', (data, isBinary) => {
    try {
      if (isBinary) {
        const buf = Buffer.from(data);
        // V5.0K: ESP32 mic PCM upstream. JPEG uploads are normally > 2 KB;
        // ESP PCM frames are 640 bytes, so relay small ESP binary frames to phone clients.
        if (rt7IsEspPcmClient_(ws) && buf.length <= 2048) {
          rt7AudioHold_(5000);
          ws.rt7EspPcmPackets = (ws.rt7EspPcmPackets || 0) + 1;
          ws.rt7EspPcmBytes = (ws.rt7EspPcmBytes || 0) + buf.length;
          rt7WsTrace.espPcmPackets++;
          rt7WsTrace.espPcmBytes += buf.length;
          rt7WsTrace.lastEspPcmTime = nowIso();
          const pn = rt7SendToPhoneIntercom_(buf, { binary:true });
          if (pn > 0) {
            rt7WsTrace.phoneRxPackets++;
            rt7WsTrace.phoneRxBytes += buf.length;
            rt7WsTrace.lastPhoneRxTime = nowIso();
          }
          if (ws.rt7EspPcmPackets <= 5 || ws.rt7EspPcmPackets % 50 === 0) {
            try { ws.send(JSON.stringify({ ok:true, type:'esp_pcm_relay_trace_v50n', esp_packets:ws.rt7EspPcmPackets, esp_bytes:ws.rt7EspPcmBytes, phone_clients:pn, state:rt7IntercomWsState_() })); } catch (_) {}
          }
          return;
        }
        const looksLikePhonePcm = rt7IsPhonePcmRole_(ws.rt7Role) || (!rt7IsEspPcmRole_(ws.rt7Role) && buf.length <= 2048);
        if (looksLikePhonePcm) {
          rt7AudioHold_(5000);
          if (!rt7IsPhonePcmRole_(ws.rt7Role)) ws.rt7Role = 'phone_pcm_auto';
          ws.rt7IntercomPackets = (ws.rt7IntercomPackets || 0) + 1;
          ws.rt7IntercomBytes = (ws.rt7IntercomBytes || 0) + buf.length;
          rt7WsTrace.phonePcmPackets++;
          rt7WsTrace.phonePcmBytes += buf.length;
          rt7WsTrace.lastPhonePcmTime = nowIso();
          const n = rt7SendToEspIntercom_(buf, { binary:true });
          if (n > 0) {
            rt7WsTrace.relayPcmPackets++;
            rt7WsTrace.relayPcmBytes += buf.length;
            rt7WsTrace.lastRelayTime = nowIso();
          }
          if (ws.rt7IntercomPackets <= 5 || ws.rt7IntercomPackets % 50 === 0) {
            try { ws.send(JSON.stringify({ ok:true, type:'ws_relay_trace_v50n', phone_packets:ws.rt7IntercomPackets, phone_bytes:ws.rt7IntercomBytes, relay_clients:n, phone_pcm_rx:rt7WsTrace.phonePcmPackets, relay_to_esp32:rt7WsTrace.relayPcmPackets, esp32_clients:rt7IntercomWsState_().esp, state:rt7IntercomWsState_() })); } catch (_) {}
          }
          return;
        }
        acceptWsStreamFrame_(buf, ws);
        return;
      }
      const txt = data.toString('utf8');
      let msg = null;
      try { msg = JSON.parse(txt); } catch (_) {}
      if (msg && msg.role) {
        ws.rt7Role = safeString(msg.role);
        ws.rt7DeviceId = safeString(msg.device_id || msg.device || msg.id || ws.rt7DeviceId || '#1');
        if (msg.pcm_role) { ws.rt7PcmRole = safeString(msg.pcm_role); ws.rt7PcmClient = rt7IsEspPcmRole_(ws.rt7PcmRole); }
        if (msg.pcm_client === true || msg.type === 'esp32_pcm_register') { ws.rt7PcmClient = true; if (!ws.rt7PcmRole) ws.rt7PcmRole = 'esp32_pcm'; }
        if (ws.rt7Role === 'viewer') streamViewers.set(safeString(msg.viewer_id || req.socket.remoteAddress || Math.random()), { ts:Date.now(), ip:req.socket.remoteAddress, state:'visible', ws:true });
        ws.send(JSON.stringify({ ok:true, type:'role_ack', phase:'V50P', role:ws.rt7Role, pcm_role:ws.rt7PcmRole||'', pcm_client:!!ws.rt7PcmClient, version:SERVER_VERSION, time:nowIso(), intercom_ws:rt7IntercomWsState_() }));
      }
      if (msg && rt7IsPhonePcmRole_(ws.rt7Role) && (msg.type === 'intercom_begin' || msg.type === 'intercom_end' || msg.type === 'intercom_ping' || msg.type === 'intercom_probe' || msg.type === 'esp_begin' || msg.type === 'esp_end' || msg.type === 'intercom_listen')) {
        if (msg.type === 'intercom_begin') rt7AudioHold_(8000);
        if (msg.type === 'intercom_end') rt7AudioHold_(3500);
        const n = rt7SendToEspIntercom_(JSON.stringify(Object.assign({ relay_time:Date.now() }, msg)));
        try { ws.send(JSON.stringify({ ok:true, type:'intercom_control_relay', control:msg.type, esp:n, state:rt7IntercomWsState_() })); } catch (_) {}
      }
    } catch (e) {
      try { ws.send(JSON.stringify({ ok:false, type:'ws_error', error:String(e.message||e) })); } catch (_) {}
    }
  });
  ws.on('close', () => { ws.rt7Closed = true; });
});


// ===== V5.6H GPIO Fast Control Page =====
// Standalone fast-control page.  It does not change the main doorbell page.
// LAN first: browser sends an image beacon directly to ESP32:8081.
// Cloud fallback: Railway tries to proxy a short GPIO/door request to the selected ESP32.
function rt7BuildDeviceUrl_(dev, port, espPath, query) {
  const raw = safeString(dev && dev.ip).trim().replace(/^https?:\/\//i,'').replace(/\/.*/,'');
  const host = raw.replace(/:\d+$/,'');
  const proto = /^https?:\/\//i.test(safeString(dev && dev.ip)) ? safeString(dev.ip).match(/^https?:\/\//i)[0].replace('://','') : 'http';
  return proto + '://' + host + (port ? (':' + port) : '') + espPath + (query || '');
}
async function rt7TryFetchText_(url, timeoutMs) {
  const ac = new AbortController();
  const t = setTimeout(()=>{ try{ ac.abort(); }catch(_){} }, timeoutMs || 900);
  try {
    const r = await fetch(url, { method:'GET', signal:ac.signal });
    const text = await r.text().catch(()=> '');
    return { ok:r.ok, status:r.status, url, text:text.slice(0,500) };
  } catch(e) {
    return { ok:false, status:0, url, error:String(e && e.message || e) };
  } finally { clearTimeout(t); }
}
app.get('/api/rt7/gpio/pulse', async (req,res)=>{
  const dev=getCurrentDevice(req);
  const pin=Number(req.query.pin || 40);
  const ms=Number(req.query.ms || req.query.pulse_ms || 300);
  const tag=encodeURIComponent(safeString(req.query.tag || 'gpio_fast_control'));
  const q='?pin='+encodeURIComponent(pin)+'&ms='+encodeURIComponent(ms)+'&pulse_ms='+encodeURIComponent(ms)+'&_='+Date.now()+'&tag='+tag;
  const tries=[];
  if(pin===40){
    tries.push(rt7BuildDeviceUrl_(dev,8081,'/api/door/open_fast','?_='+Date.now()+'&tag=rt7_gpio_control_door_fast'));
  }
  tries.push(rt7BuildDeviceUrl_(dev,8081,'/api/gpio/pulse',q));
  tries.push(rt7BuildDeviceUrl_(dev,0,'/api/gpio/pulse',q));
  tries.push(rt7BuildDeviceUrl_(dev,8081,'/api/gpio',q+'&value=1'));
  const results=[];
  for(const u of tries){
    const r=await rt7TryFetchText_(u, 800); results.push(r);
    if(r.ok){ appendEvent({type:'gpio_pulse', device:dev.id, ip:dev.ip, pin, ms, via:'cloud_proxy', url:u}); return res.json({ok:true, version:SERVER_VERSION, mode:'gpio_pulse_cloud_proxy', device:dev, pin, ms, result:r, tried:results}); }
  }
  appendEvent({type:'gpio_pulse_failed', device:dev.id, ip:dev.ip, pin, ms, tried:results.length});
  res.status(502).json({ok:false, version:SERVER_VERSION, error:'GPIO_PULSE_PROXY_FAILED', device:dev, pin, ms, tried:results});
});
app.get('/api/rt7/gpio/write', async (req,res)=>{
  const dev=getCurrentDevice(req);
  const pin=Number(req.query.pin || 40);
  const value=Number(req.query.value || 0) ? 1 : 0;
  const tag=encodeURIComponent(safeString(req.query.tag || 'gpio_write_control'));
  const q='?pin='+encodeURIComponent(pin)+'&value='+encodeURIComponent(value)+'&_='+Date.now()+'&tag='+tag;
  const tries=[rt7BuildDeviceUrl_(dev,8081,'/api/gpio',q), rt7BuildDeviceUrl_(dev,0,'/api/gpio',q), rt7BuildDeviceUrl_(dev,8081,'/api/gpio/write',q)];
  const results=[];
  for(const u of tries){ const r=await rt7TryFetchText_(u,800); results.push(r); if(r.ok){ appendEvent({type:'gpio_write', device:dev.id, ip:dev.ip, pin, value, via:'cloud_proxy', url:u}); return res.json({ok:true, version:SERVER_VERSION, mode:'gpio_write_cloud_proxy', device:dev, pin, value, result:r, tried:results}); } }
  appendEvent({type:'gpio_write_failed', device:dev.id, ip:dev.ip, pin, value, tried:results.length});
  res.status(502).json({ok:false, version:SERVER_VERSION, error:'GPIO_WRITE_PROXY_FAILED', device:dev, pin, value, tried:results});
});
app.get('/api/rt7/gpio/key', async (req,res)=>{
  const dev=getCurrentDevice(req);
  const key=safeString(req.query.key || '').trim();
  const tag=encodeURIComponent(safeString(req.query.tag || 'keypad_control'));
  const q='?key='+encodeURIComponent(key)+'&_='+Date.now()+'&tag='+tag;
  const tries=[rt7BuildDeviceUrl_(dev,8081,'/api/keypad',q), rt7BuildDeviceUrl_(dev,0,'/api/keypad',q), rt7BuildDeviceUrl_(dev,8081,'/api/gpio/key',q), rt7BuildDeviceUrl_(dev,0,'/api/gpio/key',q)];
  const results=[];
  for(const u of tries){ const r=await rt7TryFetchText_(u,700); results.push(r); if(r.ok){ appendEvent({type:'gpio_key', device:dev.id, ip:dev.ip, key, via:'cloud_proxy', url:u}); return res.json({ok:true, version:SERVER_VERSION, mode:'gpio_key_cloud_proxy', device:dev, key, result:r, tried:results}); } }
  res.status(502).json({ok:false, version:SERVER_VERSION, error:'GPIO_KEY_PROXY_FAILED', device:dev, key, tried:results});
});
app.get('/rt7_gpio_control', (req,res)=>{
  const devs0 = readDevices().filter(d => d && d.enabled !== false && d.ip);
  const devs = devs0.length ? devs0 : [
    { id:'#1', name:'RT7 ESP32-S3-CAM', ip:'192.168.0.179', enabled:true },
    { id:'#2', name:'影像對講', ip:'192.168.0.11', enabled:true },
    { id:'#3', name:'RT7 S3-CAM-A', ip:'192.168.0.12', enabled:true },
    { id:'#4', name:'RT7 S3-CAM-B', ip:'192.168.0.13', enabled:true }
  ];
  const di0 = Math.max(0, Math.min(devs.length-1, Number(req.query.d || 0) || 0));
  const streamOn = String(req.query.stream || '') === '1';
  const dev = devs[di0] || devs[0];
  const esc = (v)=>String(v==null?'':v).replace(/[&<>"']/g, c=>({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c]));
  const host = String(dev.ip||'').replace(/^https?:\/\//,'').split('/')[0].split(':')[0];
  const optHtml = devs.map((d,i)=>`<option value="${i}" ${i===di0?'selected':''}>${esc(d.id||('#'+(i+1)))} / ${esc(d.name||'設備')} / ${esc(d.ip||'')}</option>`).join('');
  const mkUrl = (path)=>`http://${host}:8081${path}${path.includes('?')?'&':'?'}_=${Date.now()}`;
  const camSrc = streamOn ? `http://${host}/api/camera/stream?_gpio=${Date.now()}` : '';
  const keys = ['1','2','3','A','4','5','6','B','7','8','9','C','*','0','#','D'];
  const keyHtml = keys.map(k=>`<button type="button" class="key ${/[ABCD*#]/.test(k)?'red':''}" data-key="${esc(k)}">${esc(k)}</button>`).join('');
  res.type('html').send(`<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no"><title>RT7 GPIO Fast Control V5.6M2</title>
<style>
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}html,body{margin:0;background:#fff;font-family:system-ui,-apple-system,'Noto Sans TC','Microsoft JhengHei',Arial,sans-serif;color:#17262a}body{max-width:520px;margin:0 auto;padding-bottom:34px}.top{height:56px;background:linear-gradient(90deg,#0b252b,#0d2c32);color:#fff;display:flex;align-items:center;padding:0 10px}.back{background:#41546b;color:#fff;text-decoration:none;border-radius:8px;padding:8px 10px;font-weight:900;font-size:13px}.menu{font-size:28px;margin-left:8px;color:#dbeafe;text-decoration:none}.title{flex:1;text-align:center;font-weight:900;line-height:1.05;font-size:12px;letter-spacing:.3px}.wrap{padding:7px}.deviceRow{display:grid;grid-template-columns:1fr 54px;gap:6px}.device{width:100%;height:36px;font-size:13px;font-weight:900;border:1px solid #334155;border-radius:4px;background:#fff;padding:0 7px}.apply{height:36px;border:0;border-radius:5px;background:#40516a;color:#fff;font-weight:900}.video{position:relative;background:#000;aspect-ratio:16/9;overflow:hidden;margin-top:6px}.video img{width:100%;height:100%;object-fit:cover;display:block;background:#000}.badge{position:absolute;top:10px;left:10px;background:#71839d;color:#fff;border-radius:5px;padding:6px 10px;font-size:12px;font-weight:900}.badge2{position:absolute;top:10px;right:10px;background:#e03131;color:#fff;border-radius:5px;padding:6px 10px;font-size:12px;font-weight:900}.hint{position:absolute;left:0;right:0;top:43%;text-align:center;color:#dbe3ee;font-weight:900;font-size:16px}.videoBtns{display:grid;grid-template-columns:1fr 1fr;gap:7px;margin-top:7px}.vbtn{display:block;text-align:center;text-decoration:none;color:#fff;border-radius:7px;padding:12px 8px;font-size:17px;font-weight:900;min-height:44px;background:#08272d}.vblue{background:#1293dd}.vdark{background:#0b252b}.keypadBox{display:flex;justify-content:center;margin:12px 0 6px}.keypad{background:#333;border:4px solid #777;border-radius:10px;padding:8px;display:grid;grid-template-columns:repeat(4,56px);gap:8px}.key{width:56px;height:45px;border-radius:6px;background:#2d8fd6;border:2px solid #9fc5dd;color:#fff;font-size:24px;font-weight:900;line-height:41px;padding:0;text-align:center;text-decoration:none;cursor:pointer;touch-action:none;user-select:none;-webkit-user-select:none;-webkit-touch-callout:none}.key:active{transform:scale(.96);filter:brightness(1.2)}.key.red{background:#c73b3b;border-color:#e6a0a0}.small{text-align:center;color:#64748b;font-size:12px;margin:7px 0}.card{border-top:1px solid #e5e7eb;padding:10px 8px}.grid2{display:grid;grid-template-columns:1fr 1fr;gap:8px}.grid3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px}.btn{display:block;text-align:center;text-decoration:none;border:0;border-radius:8px;padding:13px 8px;font-weight:900;color:#fff;background:#0b88d8;font-size:16px}.green{background:#13a85a}.redBtn{background:#d12f2f}.orange{background:#f39c12}.gray{background:#40516a}input{width:100%;font-size:17px;padding:8px;border:1px solid #cbd5e1;border-radius:6px;margin-top:7px}.status{white-space:pre-wrap;background:#071120;color:#d8f7ff;border-radius:8px;padding:9px;font-family:monospace;font-size:12px;margin:8px;min-height:38px}@media(max-width:380px){.keypad{grid-template-columns:repeat(4,47px)}.key{width:47px;height:38px;line-height:34px;font-size:20px}.title{font-size:11px}}
</style></head><body>
<div class="top"><a class="back" href="/rt7_return_doorbell?from=gpio">← 返回</a><a class="menu" href="/rt7_return_doorbell?from=gpio">☰</a><div class="title">RT7 PHASE10<br>GPIO FAST CONTROL</div></div>
<div class="wrap">
  <form id="devForm" method="get" action="/rt7_gpio_control" class="deviceRow"><select id="devSel" name="d" class="device">${optHtml}</select><button class="apply" type="submit">切換</button></form>
  <div class="video" id="videoBox"><img id="cam" alt="preview" ${camSrc?`src="${camSrc}"`:''}><div class="badge">${streamOn?'LAN':'IDLE'}</div><div class="badge2">LAN</div>${streamOn?'':`<div class="hint">等待影像串流<br><span style="font-size:12px;color:#94a3b8">按開始影像</span></div>`}</div>
  <div class="videoBtns"><a id="btnStartCam" class="vbtn vblue" href="/rt7_gpio_control?d=${di0}&stream=1">開始影像</a><a id="btnStopCam" class="vbtn vdark" href="/rt7_gpio_control?d=${di0}&stream=0">停止影像</a></div>
  <div class="keypadBox"><div class="keypad">${keyHtml}</div></div>
  <div class="small">按下送控制碼（1→15、2→25、A→A5），放開送 99；ESP32 串口會顯示 [GPIO_KEYPAD] key=...</div>
  <div class="card">
    <div class="grid2"><a class="btn green" href="${mkUrl('/api/door/open_fast?tag=rt7_gpio_page_door_h11_tone')}" target="rt7_hidden">開門 GPIO40</a><a id="pulseLink" class="btn orange" href="${mkUrl('/api/gpio/pulse?pin=40&ms=300&tag=rt7_gpio_page')}" target="rt7_hidden">Pulse 指定 GPIO</a></div>
    <div class="grid2"><input id="pin" value="40" inputmode="numeric"><input id="ms" value="300" inputmode="numeric"></div>
    <div class="grid3" style="margin-top:8px"><a id="onLink" class="btn" href="${mkUrl('/api/gpio?pin=40&value=1&tag=rt7_gpio_page')}" target="rt7_hidden">ON</a><a id="offLink" class="btn redBtn" href="${mkUrl('/api/gpio?pin=40&value=0&tag=rt7_gpio_page')}" target="rt7_hidden">OFF</a><a class="btn gray" href="${mkUrl('/api/health?tag=rt7_gpio_ping')}" target="rt7_hidden">測試</a></div>
  </div>
  <div id="status" class="status">ready V5.6M2 instant release / ${esc(dev.id||'')} / ${esc(host)}</div>
</div>
<iframe name="rt7_hidden" style="display:none;width:0;height:0;border:0"></iframe>
<script>
(function(){
  var h=${JSON.stringify(host)};
  function q(id){return document.getElementById(id)}
  function upd(){
    var p=(q('pin')&&q('pin').value)||'40'; var m=(q('ms')&&q('ms').value)||'300'; var t=Date.now();
    if(q('pulseLink')) q('pulseLink').href='http://'+h+':8081/api/gpio/pulse?pin='+encodeURIComponent(p)+'&ms='+encodeURIComponent(m)+'&tag=rt7_gpio_page&_='+t;
    if(q('onLink')) q('onLink').href='http://'+h+':8081/api/gpio?pin='+encodeURIComponent(p)+'&value=1&tag=rt7_gpio_page&_='+(t+1);
    if(q('offLink')) q('offLink').href='http://'+h+':8081/api/gpio?pin='+encodeURIComponent(p)+'&value=0&tag=rt7_gpio_page&_='+(t+2);
  }
  ['pin','ms'].forEach(function(id){var x=q(id); if(x){x.addEventListener('input',upd);x.addEventListener('change',upd);}}); upd();
  var ds=q('devSel'); if(ds){ds.addEventListener('change',function(){q('devForm').submit();});}
  var activeKey=null;
  var activePointer=null;
  var lastReleaseAt=0;
  var touchModeUntil=0;
  var pressMap={
    // V5.6M22: align to original ESP-NOW transmitter / original CAN receiver codes (ij=1 group).
    // Original receiver understands these text codes directly over ESP-NOW broadcast.
    '1':'15','2':'16','3':'19','A':'13',
    '4':'11','5':'12','6':'1C','B':'1D',
    '7':'15','8':'16','9':'19','C':'13',
    '*':'11','0':'12','#':'1C','D':'1D'
  };
  function setStatus(t){ var st=q('status'); if(st) st.textContent=t; }
  function sendCode(code, phase, rawKey){
    var now=Date.now();
    var url='http://'+h+':8081/api/keypad?key='+encodeURIComponent(code)+'&phase='+encodeURIComponent(phase||'')+'&raw='+encodeURIComponent(rawKey||'')+'&tag=rt7_gpio_original_receiver_m22&_='+now;
    // Release(99) must be fastest: fire two image beacons with unique query ids.
    // Android Chrome allows mixed-content image requests more reliably than fetch from HTTPS to HTTP LAN.
    if(String(code)==='99'){
      var img0=new Image();
      img0.src=url+'&fast=1';
      setStatus('RELEASE '+(rawKey||'')+' -> 99 sent instantly');
      return;
    }
    var img=new Image();
    img.onload=function(){ setStatus('KEY '+(phase||'send')+' '+(rawKey||'')+' -> '+code+' sent'); };
    img.onerror=function(){ setStatus('KEY '+(phase||'send')+' '+(rawKey||'')+' -> '+code+' sent; check ESP32 Serial'); };
    img.src=url;
  }
  function pressKey(rawKey, pointerId){
    rawKey=String(rawKey||'').trim();
    var code=pressMap[rawKey] || rawKey;
    if(activeKey){ return; }
    activeKey=rawKey;
    activePointer=pointerId==null?'mouse':pointerId;
    setStatus('PRESS '+rawKey+' -> '+code);
    sendCode(code,'press',rawKey);
  }
  function releaseKey(pointerId){
    if(!activeKey){ return; }
    if(pointerId!=null && activePointer!=null && activePointer!=='mouse' && pointerId!==activePointer){ return; }
    var k=activeKey;
    activeKey=null;
    activePointer=null;
    var now=Date.now();
    if((now-lastReleaseAt)<80){ return; }
    lastReleaseAt=now;
    setStatus('RELEASE '+k+' -> 99');
    sendCode('99','release',k);
  }
  document.querySelectorAll('.key[data-key]').forEach(function(b){
    function raw(){ return b.getAttribute('data-key')||b.textContent.trim(); }
    b.addEventListener('touchstart', function(ev){
      touchModeUntil=Date.now()+900;
      ev.preventDefault();
      pressKey(raw(), 'touch');
    }, {passive:false});
    b.addEventListener('touchend', function(ev){
      touchModeUntil=Date.now()+900;
      ev.preventDefault();
      releaseKey('touch');
    }, {passive:false});
    b.addEventListener('touchcancel', function(ev){
      touchModeUntil=Date.now()+900;
      ev.preventDefault();
      releaseKey('touch');
    }, {passive:false});
    b.addEventListener('pointerdown', function(ev){
      if(Date.now()<touchModeUntil) return;
      ev.preventDefault();
      try{ b.setPointerCapture(ev.pointerId); }catch(_){ }
      pressKey(raw(), ev.pointerId);
    }, {passive:false});
    b.addEventListener('pointerup', function(ev){
      if(Date.now()<touchModeUntil) return;
      ev.preventDefault();
      releaseKey(ev.pointerId);
    }, {passive:false});
    b.addEventListener('pointercancel', function(ev){ ev.preventDefault(); releaseKey(ev.pointerId); }, {passive:false});
    b.addEventListener('pointerleave', function(ev){ if(activeKey){ releaseKey(ev.pointerId); } }, {passive:false});
    b.addEventListener('contextmenu', function(ev){ ev.preventDefault(); }, {passive:false});
  });
  window.addEventListener('blur', function(){ releaseKey(); });
  document.addEventListener('visibilitychange', function(){ if(document.hidden) releaseKey(); });
})();
</script></body></html>`);
});

ensureDataDir();
const port = process.env.PORT || 3000;
server.listen(port, () => console.log(`${SERVER_VERSION} listening on ${port}`));

