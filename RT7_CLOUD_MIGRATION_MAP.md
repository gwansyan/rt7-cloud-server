# RT7 Cloud Migration Map — V3

## 原則：一個功能一個功能搬移與測試

是，建議每次只搬一個功能。每個功能都保留：

1. Railway Node.js API
2. Node-RED 原始 flow 對照
3. ESP32 呼叫網址
4. 測試步驟
5. 回退方式

這樣日後可以直接比對 Node-RED flow 與 Railway Node.js API。

---

## V3 已包含功能

### A. Doorbell / 門鈴

#### Node-RED 原本
```text
POST /api/rt7/phase9n/doorbell/event
GET  /api/rt7/doorbell/state
```

#### Railway Node.js
```text
POST /api/rt7/phase9n/doorbell/event   舊路徑相容
POST /api/rt7/doorbell/ring            舊路徑相容
POST /api/doorbell                     新雲端路徑
GET  /api/rt7/doorbell/state           舊路徑相容
GET  /api/doorbell/state               新雲端路徑
GET  /rt7_cloud_doorbell_player        手機提示音頁
```

#### ESP32
```text
https://rt7-cloud-server-production.up.railway.app/api/rt7/phase9n/doorbell/event
```

#### 測試
```text
1. 手機開 /rt7_cloud_doorbell_player
2. 按「啟用提示音」
3. ESP32 開 /api/doorbell/test 或按 GPIO39
4. 手機顯示 🔔 有人按門鈴，播放兩聲
5. GET /api/rt7/doorbell/state 確認 count 增加
```

---

### B. Event Logger / 事件紀錄

#### Node-RED 原本
```text
C:/RT7_LOGS/rt7_event_log.jsonl
```

#### Railway Node.js
```text
POST /api/events/log
GET  /api/events/latest
GET  /api/events/clear
```

#### Railway 保存位置
```text
data/rt7_event_log.jsonl
```

注意：Railway 一般檔案系統重新部署可能清空。正式版建議改用 Railway Volume、PostgreSQL、Firebase 或 Supabase。

---

### C. Device Registry / 設備註冊

#### Node-RED 原本
```text
C:/RT7_LOGS/rt7_devices.json
```

#### Railway Node.js
```text
POST /api/device/register
GET  /api/devices
POST /api/devices/save
```

#### Railway 保存位置
```text
data/rt7_devices.json
```

#### ESP32 開機後可 POST
```json
{
  "device_id":"#1",
  "device_name":"前門",
  "ip":"192.168.0.179",
  "version":"RT7_PHASE10_CLOUD_DOORBELL_CLIENT"
}
```

---

## 下一批建議不要一次搬

| 階段 | 功能 | 狀態 |
|---|---|---|
| V3-1 | Doorbell Player | 已做 |
| V3-2 | Event Logger | 已做基本版 |
| V3-3 | Device Register | 已做基本版 |
| V3-4 | WebSocket Push | 已做基本版 |
| V4 | Remote Door Open | 建議下一步 |
| V5 | AI Chat Relay | 之後 |
| V6 | OpenAI Vision Relay | 之後 |
| V7 | Face Match | 之後 |

## 回退策略

ESP32 只要把 cloud URL 改回 Node-RED LAN URL，即可回到舊流程。

```text
Node-RED:
http://192.168.0.55:1880/api/rt7/phase9n/doorbell/event

Railway:
https://rt7-cloud-server-production.up.railway.app/api/rt7/phase9n/doorbell/event
```
