# RT7 COMMUNITY MANAGEMENT

第3章：社區管理教學版

## Railway 頁面
/rt7_community_manager

## 功能
- Master Registry
- 建立社區
- 綁定主門禁 Master UID
- 建立 admin
- 新增住戶 user
- 登入測試
- 推播訂閱
- 加入社區推播群組
- 門鈴依社區推播

## data 檔案
- master_registry.json
- communities.json
- users.json
- community_push_groups.json
- push_subscriptions.json
- push_log.json
- events.json
- commands.json

## Node-RED
分開 Flow：
- CH3_COMMUNITY_MONITOR.json
- CH3_COMMUNITY_TOOLS.json
- CH3_ALL_SEPARATED_TABS.json

## 測試流程
1. ESP32 或 Node-RED 送 heartbeat
2. /rt7_community_manager 建立 A社區 admin
3. 新增 user01
4. 手機重新訂閱推播
5. 加入社區推播群組
6. 按門鈴，手機收到「A社區 有人按門鈴」
