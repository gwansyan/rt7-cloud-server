// RT7_COMMUNITY_MANAGEMENT
// 第3章：社區管理教學版
// 功能：Master Registry / 社區建立 / admin-user / 主門禁綁定 / 登入 / 社區推播群組 / 門鈴社區推播
const express=require('express');
const cors=require('cors');
const fs=require('fs');
const path=require('path');
let webpush=null; try{webpush=require('web-push');}catch(e){webpush=null;}

const app=express();
const PORT=process.env.PORT||3000;
const DATA_DIR=path.join(__dirname,'data');
app.use(cors());
app.use(express.json({limit:'2mb'}));
app.use(express.urlencoded({extended:true}));

function ensureFile(n,f){if(!fs.existsSync(DATA_DIR))fs.mkdirSync(DATA_DIR,{recursive:true});const p=path.join(DATA_DIR,n);if(!fs.existsSync(p))fs.writeFileSync(p,JSON.stringify(f,null,2));return p;}
function readJson(n,f){const p=ensureFile(n,f);try{return JSON.parse(fs.readFileSync(p,'utf8')||JSON.stringify(f));}catch{return f;}}
function writeJson(n,d){const p=ensureFile(n,Array.isArray(d)?[]:{});fs.writeFileSync(p,JSON.stringify(d,null,2));}
function nowIso(){return new Date().toISOString();}
function id(prefix){return prefix+'_'+Math.random().toString(16).slice(2)+Date.now().toString(16);}
function onlineStatus(t){return t&&(Date.now()-new Date(t).getTime())<300000?'ONLINE':'OFFLINE';}
function cleanMac(m){return String(m||'').toUpperCase().replace(/[^0-9A-F]/g,'').slice(0,12);}
function uidFromMac(m){const s=cleanMac(m);if(s.length!==12)return'';return'RT7-MASTER-'+(s.match(/../g)||[]).reverse().join('');}
function detectSource(mac){return String(mac||'').toUpperCase()==='AA:BB:CC:DD:EE:01'?'SIM':'ESP32';}
function sanitizeUser(u){const x={...u}; delete x.password; return x;}

function setupWebPush(){
 if(!webpush)return{ok:false,error:'web-push package missing'};
 const pub=process.env.RT7_VAPID_PUBLIC_KEY||'',pri=process.env.RT7_VAPID_PRIVATE_KEY||'';
 let keys=null;
 if(pub&&pri)keys={publicKey:pub,privateKey:pri,source:'env'};
 else{keys=readJson('vapid_keys.json',null); if(!keys||!keys.publicKey||!keys.privateKey){keys=webpush.generateVAPIDKeys();writeJson('vapid_keys.json',keys);} keys.source='data/vapid_keys.json';}
 try{webpush.setVapidDetails(process.env.RT7_VAPID_SUBJECT||'mailto:rt7-community@example.com',keys.publicKey,keys.privateKey);return{ok:true,publicKey:keys.publicKey,source:keys.source};}
 catch(e){return{ok:false,error:String(e.message||e)};}
}
async function sendPushToSubs(subs,payload){
 const setup=setupWebPush(), logs=readJson('push_log.json',[]);
 if(!setup.ok){const item={time:nowIso(),ok:false,sent:0,total:subs.length,status:'SETUP_FAILED',failures:[{message:setup.error}],payload};logs.push(item);writeJson('push_log.json',logs.slice(-200));return item;}
 let sent=0,failures=[],removed=0,keepMap=new Set();
 for(const wrap of subs){
   const sub=wrap.subscription||wrap;
   try{await webpush.sendNotification(sub,JSON.stringify(payload),{TTL:60,urgency:'high'});sent++;keepMap.add(sub.endpoint);}
   catch(e){const code=e.statusCode||e.status||0;failures.push({code,endpoint:String(sub.endpoint||'').slice(0,80),message:String(e.body||e.message||e).slice(0,300)}); if([400,403,404,410].includes(code))removed++;}
 }
 const item={time:nowIso(),ok:true,sent,total:subs.length,removed,status:sent>0?'SENT':(subs.length===0?'NO_SUBSCRIBERS':'FAILED'),failures,payload};
 logs.push(item);writeJson('push_log.json',logs.slice(-200));return item;
}

['master_registry.json','commands.json'].forEach(n=>ensureFile(n,{}));
['communities.json','users.json','events.json','push_subscriptions.json','community_push_groups.json','push_log.json'].forEach(n=>ensureFile(n,[]));

app.get('/',(_,res)=>res.redirect('/rt7_community_manager'));
app.get('/sw.js',(_,res)=>res.type('application/javascript').set('Service-Worker-Allowed','/').send(`
self.addEventListener('install',e=>self.skipWaiting());
self.addEventListener('activate',e=>e.waitUntil(self.clients.claim()));
self.addEventListener('push',e=>{let d={};try{d=e.data?e.data.json():{};}catch(x){};e.waitUntil(self.registration.showNotification(d.title||'RT7 社區推播',{body:d.body||'收到社區通知',tag:d.tag||'rt7-community',renotify:true,data:{url:d.url||'/rt7_community_manager'}}));});
self.addEventListener('notificationclick',e=>{e.notification.close();const u=(e.notification.data&&e.notification.data.url)||'/rt7_community_manager';e.waitUntil(clients.openWindow(u));});`));

app.get('/rt7_community_manager',(_,res)=>res.type('html').send(String.raw`<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>RT7 Community Management</title><style>
body{font-family:Arial,'Noto Sans TC',sans-serif;background:#eef4f6;margin:0;color:#10232e}.wrap{max-width:1120px;margin:20px auto;padding:16px}.card{background:#fff;border-radius:14px;padding:18px;margin:14px 0;box-shadow:0 2px 8px #0001}input,select,button{font-size:16px;padding:10px;border-radius:8px;border:1px solid #ccd6dc;margin:4px}button{background:#0b9b5a;color:#fff;border:0}.blue{background:#0b78d0}.red{background:#c0392b}.gray{background:#64748b}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:8px}table{width:100%;border-collapse:collapse}th,td{padding:8px;border-bottom:1px solid #e5edf1;text-align:left;vertical-align:top}pre{background:#f5f7f8;padding:10px;border-radius:8px;overflow:auto;white-space:pre-wrap;min-height:50px}.ok{color:#079b50;font-weight:bold}.bad{color:#d33;font-weight:bold}.hint{color:#607080;font-size:14px;line-height:1.6}.pill{display:inline-block;padding:3px 8px;border-radius:999px;background:#e9f7ef;color:#0b7a43;font-weight:bold}.warnp{background:#fef3c7;color:#92400e}.badp{background:#fee2e2;color:#b91c1c}</style></head><body><div class="wrap"><h1>RT7 COMMUNITY MANAGEMENT</h1><p>第3章：社區 / 帳號 / 主門禁綁定 / 社區推播群組。</p><div id="app">載入中...</div></div><script>
window.addEventListener('error',e=>show({ok:false,where:'window.onerror',message:e.message,line:e.lineno}));
window.addEventListener('unhandledrejection',e=>show({ok:false,where:'unhandledrejection',message:String(e.reason&&e.reason.message||e.reason)}));
async function api(p,o){const r=await fetch(p,Object.assign({headers:{'Content-Type':'application/json'}},o||{}));let t=await r.text();try{return JSON.parse(t)}catch{return{ok:false,status:r.status,text:t.slice(0,500)}}}
async function post(p,d){return api(p,{method:'POST',body:JSON.stringify(d)});}
function esc(s){return String(s==null?'':s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
function show(x){let e=document.getElementById('out'); if(e)e.textContent=JSON.stringify(x,null,2);}
async function safe(fn){try{show({running:true,fn:fn.name});await fn();}catch(e){show({ok:false,error:String(e.message||e),stack:String(e.stack||'').slice(0,600)});}}
function render(s){
 const masters=Object.values(s.masters||{}), communities=s.communities||[], users=s.users||[], groups=s.community_push_groups||[];
 let h='';
 h+='<div class="card"><h2>1. Master Registry</h2><table><tr><th>UID</th><th>IP</th><th>MAC</th><th>狀態</th><th>來源</th><th>最後 heartbeat</th></tr>';
 masters.forEach(m=>h+='<tr><td>'+esc(m.master_uid)+'</td><td>'+esc(m.ip)+'</td><td>'+esc(m.mac)+'</td><td class="'+(m.status==='ONLINE'?'ok':'bad')+'">'+esc(m.status)+'</td><td>'+esc(m.source)+'</td><td>'+esc(m.last_heartbeat)+'</td></tr>');h+='</table></div>';
 h+='<div class="card"><h2>2. 建立社區 + 綁定主門禁 + 建立 Admin</h2><p class="hint">選在線 Master UID，建立社區。第一個帳號會是 admin。</p><div class="grid"><input id="new_comm" placeholder="社區名稱，例如 A社區"><input id="new_admin" value="admin" placeholder="admin帳號"><input id="new_pass" value="1234" placeholder="密碼"><select id="new_master"><option value="">選擇在線主門禁</option>';
 masters.forEach(m=>h+='<option value="'+esc(m.master_uid)+'">'+esc(m.master_uid+' / '+(m.ip||''))+'</option>');h+='</select></div><button onclick="safe(createCommunity)">建立社區</button></div>';
 h+='<div class="card"><h2>3. 新增住戶 User</h2><div class="grid"><select id="user_comm"><option value="">選擇社區</option>';
 communities.forEach(c=>h+='<option value="'+esc(c.community_id)+'">'+esc(c.name)+'</option>');h+='</select><input id="user_name" placeholder="住戶帳號，例如 user01"><input id="user_pass" value="1111" placeholder="密碼"></div><button onclick="safe(addUser)">新增住戶</button></div>';
 h+='<div class="card"><h2>4. 登入測試</h2><div class="grid"><input id="login_comm" placeholder="社區名稱"><input id="login_user" value="admin"><input id="login_pass" value="1234"></div><button onclick="safe(login)">登入</button></div>';
 h+='<div class="card"><h2>5. 推播訂閱與社區綁定</h2><p class="hint">手機開此頁，按重新訂閱，並選擇社區加入該社區推播群組。</p><div class="grid"><select id="push_comm"><option value="">選擇社區</option>';
 communities.forEach(c=>h+='<option value="'+esc(c.community_id)+'">'+esc(c.name)+'</option>');h+='</select></div><button class="blue" onclick="safe(resubscribe)">重新訂閱推播</button><button onclick="safe(bindPush)">加入社區推播群組</button><button class="gray" onclick="safe(testCommunityPush)">測試社區推播</button><button class="red" onclick="safe(clearPush)">清除全部推播訂閱</button><p>全域訂閱數：<span class="pill">'+esc(s.push.count)+'</span>　社區群組數：<span class="pill">'+esc(groups.length)+'</span></p></div>';
 h+='<div class="card"><h2>6. 社區清單</h2><table><tr><th>社區ID</th><th>社區</th><th>Master UID</th><th>Master狀態</th><th>建立時間</th></tr>';communities.forEach(c=>h+='<tr><td>'+esc(c.community_id)+'</td><td>'+esc(c.name)+'</td><td>'+esc(c.master_uid)+'</td><td>'+esc(c.master_status||'-')+'</td><td>'+esc(c.created_at)+'</td></tr>');h+='</table></div>';
 h+='<div class="card"><h2>7. 帳號清單</h2><table><tr><th>社區</th><th>帳號</th><th>角色</th><th>狀態</th></tr>';users.forEach(u=>h+='<tr><td>'+esc(u.community_name||u.community_id)+'</td><td>'+esc(u.username)+'</td><td>'+esc(u.role)+'</td><td>'+esc(u.status||'ACTIVE')+'</td></tr>');h+='</table></div>';
 h+='<div class="card"><h2>8. 門鈴社區推播測試</h2><div class="grid"><input id="door_uid" placeholder="Master UID" value="'+esc((masters[0]&&masters[0].master_uid)||'')+'"><input id="door_msg" value="有人按門鈴"></div><button onclick="safe(doorbell)">送出門鈴事件 → 該社區推播</button></div>';
 h+='<div class="card"><h2>9. 即時結果</h2><pre id="out">READY</pre></div>';
 document.getElementById('app').innerHTML=h;
}
async function load(){render(await api('/api/community/state'));}
function b64ToUint8Array(b){const p='='.repeat((4-b.length%4)%4),base64=(b+p).replace(/-/g,'+').replace(/_/g,'/'),raw=atob(base64),out=new Uint8Array(raw.length);for(let i=0;i<raw.length;i++)out[i]=raw.charCodeAt(i);return out;}
async function createCommunity(){show(await post('/api/community/create',{name:new_comm.value,admin_username:new_admin.value,password:new_pass.value,master_uid:new_master.value}));await load();}
async function addUser(){show(await post('/api/community/user/add',{community_id:user_comm.value,username:user_name.value,password:user_pass.value,role:'USER'}));await load();}
async function login(){show(await post('/api/community/login',{community:login_comm.value,username:login_user.value,password:login_pass.value}));}
async function subscribe(){if(!('serviceWorker'in navigator))throw new Error('serviceWorker not supported');if(!('PushManager'in window))throw new Error('PushManager not supported');const kr=await api('/api/push/public-key');if(!kr.publicKey)throw new Error('no VAPID key');const reg=await navigator.serviceWorker.register('/sw.js',{scope:'/'});await navigator.serviceWorker.ready;let perm=Notification.permission;if(perm!=='granted')perm=await Notification.requestPermission();if(perm!=='granted')throw new Error('notification permission '+perm);let sub=await reg.pushManager.getSubscription();if(!sub)sub=await reg.pushManager.subscribe({userVisibleOnly:true,applicationServerKey:b64ToUint8Array(kr.publicKey)});return sub;}
async function resubscribe(){const reg=await navigator.serviceWorker.register('/sw.js',{scope:'/'});await navigator.serviceWorker.ready;const old=await reg.pushManager.getSubscription();if(old)await old.unsubscribe();await post('/api/push/clear',{});const sub=await subscribe();const r=await post('/api/push/subscribe',{subscription:sub});show(r);await load();}
async function bindPush(){const reg=await navigator.serviceWorker.ready;let sub=await reg.pushManager.getSubscription();if(!sub)sub=await subscribe();show(await post('/api/community/push/bind',{community_id:push_comm.value,subscription:sub}));await load();}
async function testCommunityPush(){show(await post('/api/community/push/test',{community_id:push_comm.value,title:'RT7 社區推播測試',body:'社區推播功能正常'}));await load();}
async function clearPush(){show(await post('/api/push/clear',{}));await load();}
async function doorbell(){show(await post('/api/rt7/community/doorbell',{master_uid:door_uid.value,message:door_msg.value}));await load();}
load();setInterval(async()=>{try{const s=await api('/api/community/state'),a=document.activeElement;if(!a||!['INPUT','SELECT','TEXTAREA'].includes(a.tagName))render(s);}catch(e){}},5000);
</script></body></html>`));

app.get('/api/community/state',(_,res)=>{
 const masters=readJson('master_registry.json',{});Object.keys(masters).forEach(uid=>masters[uid].status=onlineStatus(masters[uid].last_heartbeat));
 const communities=readJson('communities.json',[]).map(c=>({...c,master_status:masters[c.master_uid]?onlineStatus(masters[c.master_uid].last_heartbeat):'OFFLINE'}));
 const users=readJson('users.json',[]).map(u=>sanitizeUser(u));
 res.json({ok:true,masters,communities,users,community_push_groups:readJson('community_push_groups.json',[]),events:readJson('events.json',[]),push:{count:readJson('push_subscriptions.json',[]).length,logs:readJson('push_log.json',[]).slice(-10).reverse(),setup:setupWebPush()}});
});
app.post('/api/rt7/master/heartbeat',(req,res)=>{
 let{master_uid,ip,mac}=req.body||{};if(!master_uid)master_uid=uidFromMac(mac);if(!master_uid)return res.status(400).json({ok:false,error:'missing master_uid_or_valid_mac'});
 const masters=readJson('master_registry.json',{});masters[master_uid]={master_uid,ip:ip||req.ip,mac:mac||'',last_heartbeat:nowIso(),status:'ONLINE',source:detectSource(mac),uid_rule:'MAC_REVERSE_PAIRS'};writeJson('master_registry.json',masters);res.json({ok:true,master:masters[master_uid]});
});
app.post('/api/community/create',(req,res)=>{
 const{name,admin_username,password,master_uid}=req.body||{}; if(!name||!admin_username||!password||!master_uid)return res.status(400).json({ok:false,error:'missing name/admin/password/master_uid'});
 const masters=readJson('master_registry.json',{}); if(!masters[master_uid])return res.status(400).json({ok:false,error:'master not online'});
 const communities=readJson('communities.json',[]),users=readJson('users.json',[]);
 if(communities.some(c=>c.name===name))return res.status(409).json({ok:false,error:'community exists'});
 if(communities.some(c=>c.master_uid===master_uid))return res.status(409).json({ok:false,error:'master already bound'});
 const community={community_id:id('comm'),name,master_uid,master_ip:masters[master_uid].ip||'',created_at:nowIso()};
 const user={user_id:id('user'),community_id:community.community_id,community_name:name,username:admin_username,password,role:'ADMIN',status:'ACTIVE',created_at:nowIso()};
 communities.push(community);users.push(user);writeJson('communities.json',communities);writeJson('users.json',users);res.json({ok:true,community,user:sanitizeUser(user)});
});
app.post('/api/community/user/add',(req,res)=>{
 const{community_id,username,password,role}=req.body||{};if(!community_id||!username||!password)return res.status(400).json({ok:false,error:'missing community_id/username/password'});
 const communities=readJson('communities.json',[]),c=communities.find(x=>x.community_id===community_id);if(!c)return res.status(404).json({ok:false,error:'community not found'});
 const users=readJson('users.json',[]);if(users.some(u=>u.community_id===community_id&&u.username===username))return res.status(409).json({ok:false,error:'user exists in community'});
 const user={user_id:id('user'),community_id,community_name:c.name,username,password,role:role||'USER',status:'ACTIVE',created_at:nowIso()};users.push(user);writeJson('users.json',users);res.json({ok:true,user:sanitizeUser(user)});
});
app.post('/api/community/login',(req,res)=>{
 const{community,username,password}=req.body||{};const users=readJson('users.json',[]);const u=users.find(x=>(x.community_name===community||x.community_id===community)&&x.username===username&&x.password===password);
 if(!u)return res.status(401).json({ok:false,error:'login failed'});const communities=readJson('communities.json',[]),c=communities.find(x=>x.community_id===u.community_id);res.json({ok:true,user:sanitizeUser(u),community:c});
});
app.get('/api/push/public-key',(_,res)=>{const setup=setupWebPush();res.json({ok:!!setup.ok,publicKey:setup.publicKey||'',setup});});
app.post('/api/push/subscribe',(req,res)=>{
 const sub=req.body&&req.body.subscription;if(!sub||!sub.endpoint)return res.status(400).json({ok:false,error:'missing subscription'});
 const arr=readJson('push_subscriptions.json',[]),idx=arr.findIndex(x=>(x.subscription||x).endpoint===sub.endpoint),item={subscription:sub,created_at:nowIso(),user_agent:req.headers['user-agent']||''};if(idx>=0)arr[idx]=item;else arr.push(item);writeJson('push_subscriptions.json',arr);res.json({ok:true,count:arr.length,endpoint:String(sub.endpoint).slice(0,80)});
});
app.post('/api/push/clear',(_,res)=>{writeJson('push_subscriptions.json',[]);writeJson('community_push_groups.json',[]);res.json({ok:true,count:0});});
app.post('/api/community/push/bind',(req,res)=>{
 const{community_id,subscription}=req.body||{};if(!community_id||!subscription||!subscription.endpoint)return res.status(400).json({ok:false,error:'missing community_id/subscription'});
 const communities=readJson('communities.json',[]);if(!communities.find(c=>c.community_id===community_id))return res.status(404).json({ok:false,error:'community not found'});
 let subs=readJson('push_subscriptions.json',[]);if(!subs.some(s=>(s.subscription||s).endpoint===subscription.endpoint)){subs.push({subscription,created_at:nowIso(),user_agent:req.headers['user-agent']||''});writeJson('push_subscriptions.json',subs);}
 const groups=readJson('community_push_groups.json',[]);const idx=groups.findIndex(g=>g.community_id===community_id&&g.endpoint===subscription.endpoint);
 const item={community_id,endpoint:subscription.endpoint,subscription,created_at:nowIso()};if(idx>=0)groups[idx]=item;else groups.push(item);writeJson('community_push_groups.json',groups);res.json({ok:true,community_id,count:groups.filter(g=>g.community_id===community_id).length});
});
app.post('/api/community/push/test',async(req,res)=>{
 const{community_id,title,body}=req.body||{};if(!community_id)return res.status(400).json({ok:false,error:'missing community_id'});
 const groups=readJson('community_push_groups.json',[]).filter(g=>g.community_id===community_id);const result=await sendPushToSubs(groups,{type:'community_test',title:title||'RT7 社區推播測試',body:body||'社區推播功能正常',url:'/rt7_community_manager',community_id});
 res.json({ok:true,result});
});
app.post('/api/rt7/community/doorbell',async(req,res)=>{
 const{master_uid,message}=req.body||{};if(!master_uid)return res.status(400).json({ok:false,error:'missing master_uid'});
 const communities=readJson('communities.json',[]),c=communities.find(x=>x.master_uid===master_uid);const events=readJson('events.json',[]);
 const event={time:nowIso(),master_uid,community_id:c&&c.community_id,community_name:c&&c.name,event:'doorbell',message:message||'有人按門鈴'};
 let push={sent:0,total:0,status:'NO_COMMUNITY'};
 if(c){const groups=readJson('community_push_groups.json',[]).filter(g=>g.community_id===c.community_id);push=await sendPushToSubs(groups,{type:'doorbell',title:'🔔 '+c.name+' 有人按門鈴',body:event.message,url:'/rt7_community_manager',tag:'rt7-community-doorbell',master_uid,community_id:c.community_id});}
 event.push_sent=push.sent||0;event.push_status=push.status||'';events.push(event);writeJson('events.json',events.slice(-200));res.json({ok:true,event,push});
});
app.post('/edu/master/heartbeat',(req,res)=>{req.url='/api/rt7/master/heartbeat';app._router.handle(req,res,()=>{});});
app.post('/edu/event/doorbell',(req,res)=>{req.url='/api/rt7/community/doorbell';app._router.handle(req,res,()=>{});});
app.get('/edu/device/command',(req,res)=>res.json({ok:true,command:null,note:'community chapter keeps door command in previous chapter'}));

app.listen(PORT,()=>console.log('[RT7_COMMUNITY_MANAGEMENT] http://localhost:'+PORT+'/rt7_community_manager'));
