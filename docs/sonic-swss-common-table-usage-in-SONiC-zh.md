# sonic-swss-common 在 SONiC 中的 Table 使用方式

本文總結 SONiC 各元件如何使用 `sonic-swss-common` 的 table class、Redis DB
和 Redis channel。內容基於 `~/sonic-swss` 中觀察到的 table 行為以及本 repo 的
範例。

每個 flow 要回答的核心問題：

```text
誰 produce、用哪個 DB/table/channel、誰 consume、涉及哪個 sonic-swss-common table class？
```

本文刻意聚焦於 Redis 和 `sonic-swss-common` 機制，不描述每個 SAI object、
vendor SDK call 或 SONiC feature daemon。

## 心智模型

SONiC 並非到處使用同一種 Redis pattern。不同 DB path 使用不同的 table 契約：

```text
CONFIG_DB
  直接持久化的 config hash
  producer: CLI/config tooling 用 Table
  consumer: mgrd/orch Consumer backed by SubscriberStateTable

APPL_DB
  desired-state 發布
  producer: mgrd 用 ProducerStateTable
  consumer: orch 用 ConsumerStateTable

ASIC_DB SAI request path
  有序 SAI operation stream / object view
  producer: orch 透過 sairedis，實作為 ProducerTable-like ASIC_DB operations
  consumer: syncd 用 ConsumerTable

ASIC_DB SAI response path
  同步 SAI 結果的有序 response queue
  producer: syncd 用 ProducerTable(ASIC_DB, "GETRESPONSE")
  consumer: sairedis client side 用 ConsumerTable

ASIC_DB async notification path
  非請求的 SAI/vendor 事件
  producer: syncd 用 NotificationProducer on ASIC_DB:NOTIFICATIONS
  consumer: orch 用 NotificationConsumer

STATE_DB / APPL_STATE_DB / COUNTERS_DB
  直接的 state/stat hash
  producer: feature-specific daemon 用 Table-style writes
  consumer: feature-specific daemon/CLI 用 Table 或直接 Redis reads
```

最常見的錯誤是把每個 DB 都當成 `ProducerTable` / `ConsumerTable`。這只對有序
operation stream 正確，例如 ASIC request path 和部分傳統 flex counter 設定路徑。

## Table Class 規則

| swss-common 類型 | 主要角色 | 典型 DB | 關鍵行為 |
| --- | --- | --- | --- |
| `Table` | 直接 hash 讀寫 | `CONFIG_DB`, `STATE_DB`, `COUNTERS_DB`, `APPL_STATE_DB` | Producer 直接寫入最終 hash，不建立 queue。 |
| `SubscriberStateTable` | 訂閱直接 table 變更 | `CONFIG_DB`, `STATE_DB` | Consumer 透過 keyspace notification 看到直接 table writer 的更新。 |
| `ProducerStateTable` | 發布最新 desired state | `APPL_DB` | Producer 寫入 pending `_TABLE:key` 加 key set。同 key 更新可合併。 |
| `ConsumerStateTable` | 消費最新 desired state | `APPL_DB` | Consumer pop pending state 並 materialize 最終 `TABLE:key`。 |
| `ProducerTable` | 有序 operation stream | `ASIC_DB`, 傳統 `FLEX_COUNTER_DB` | Producer 將 operation append 到 queue。每個 operation 都重要。 |
| `ConsumerTable` | 消費有序 operation stream | `ASIC_DB` | Consumer pop queued operations 並 materialize 最終 object table 內容。 |
| `NotificationProducer` | 發布 channel 事件 | Response channels, `ASIC_DB:NOTIFICATIONS` | Producer 發送 channel message，不是持久 table row。 |
| `NotificationConsumer` | 消費 channel 事件 | Response channels, `ASIC_DB:NOTIFICATIONS` | Consumer 等待 channel message，不是 table pop。 |

`orchagent` 用自己的 `Consumer` executor 包裝許多 table consumer。backend table
class 取決於 DB：

```text
CONFIG_DB / STATE_DB / CHASSIS_APP_DB
  -> Consumer(new SubscriberStateTable(...))

其他 DB 如 APPL_DB
  -> Consumer(new ConsumerStateTable(...))
```

所以對 CONFIG_DB rows，精確的 consumer 描述通常是：

```text
<Orch class> / orch Consumer backed by SubscriberStateTable
```

## 設定與 Intent Flow

這是由北向南的 config path：

```text
CLI/config tooling
  -> CONFIG_DB
  -> mgrd
  -> APPL_DB
  -> orch
```

| Flow | Producer / Table | DB / Table | Consumer / Table |
| --- | --- | --- | --- |
| Config 寫入 | CLI/config tooling / `Table` | `CONFIG_DB:<table>\|<key>` | mgrd 或 orch / `Consumer` backed by `SubscriberStateTable` |
| App desired state | mgrd / `ProducerStateTable` | `APPL_DB:<table>:<key>` pending `_TABLE:key` 和 `<TABLE>_KEY_SET` | orch / `ConsumerStateTable` |

VLAN 範例：

| Flow | Producer / Table | DB / Table | Consumer / Table |
| --- | --- | --- | --- |
| VLAN config | config command / `Table` | `CONFIG_DB:VLAN\|Vlan100` | `vlanmgrd` / `SubscriberStateTable` 或啟動時 `Table.get` |
| VLAN app intent | `vlanmgrd` / `ProducerStateTable` | `APPL_DB:VLAN_TABLE:Vlan100` via pending `_VLAN_TABLE:Vlan100` | `PortsOrch` / `ConsumerStateTable` |

真正的 SONiC APPL_DB consumer for `VLAN_TABLE` 是 `PortsOrch`，不是獨立的
VLAN-specific orch class。

## SAI Request 與 Response

從 orchagent 程式碼看，一個 SAI operation 看起來像本地 C call：

```cpp
sai_status_t status = sai_vlan_api->create_vlan(...);
```

在 SONiC 中，orchagent 載入 sairedis SAI 實作。該實作是 Redis-backed SAI proxy，
將 SAI operation 序列化到 ASIC_DB，讓 `syncd` 呼叫真正的 vendor SAI 實作。

request/response path：

```text
orchagent / PortsOrch
  -> SAI API call through sairedis
  -> ASIC_DB request
  -> syncd
  -> real vendor SAI shared library
  -> vendor SDK / driver / firmware / ASIC
```

| Flow | Producer / Table | DB / Table / Channel | Consumer / Table |
| --- | --- | --- | --- |
| SAI object request | orchagent through sairedis / `ProducerTable`-style ASIC_DB operation | `ASIC_DB:ASIC_STATE:SAI_OBJECT_TYPE_*` | syncd / `ConsumerTable` |
| SAI sync response | syncd / `ProducerTable` | `ASIC_DB:GETRESPONSE` (key = SAI status, op = `getresponse`) | sairedis client side / `ConsumerTable` |

SAI request 和 response 都是 `ProducerTable` / `ConsumerTable` 有序 queue——不是
pub/sub channel。Request 走 `ASIC_STATE:SAI_OBJECT_TYPE_*_KEY_VALUE_OP_QUEUE`，
response 走 `GETRESPONSE_KEY_VALUE_OP_QUEUE`。這與 async notification 不同，
後者使用 `NotificationProducer` / `NotificationConsumer` 的 Redis pub/sub channel。

Vendor SAI shared library 不負責序列化 Redis response。分工是：

```text
syncd calls vendor libsai
  -> vendor libsai returns sai_status_t and output data
  -> syncd/sairedis serialization code formats the Redis response
  -> ProducerTable(ASIC_DB, "GETRESPONSE").set(SAI_STATUS_*, ..., "getresponse")
```

`ASIC_DB:ASIC_STATE:*` 同時是 SAI request transport 和 ASIC object state 的
Redis view。它不是 mgrd 的正常 northbound interface。

## 非同步通知 (Async Notifications)

非同步通知與 SAI request response 不同。它們是來自 vendor SAI callback path 的
非請求事件。

```text
ASIC / vendor SAI callback
  -> syncd
  -> ASIC_DB:NOTIFICATIONS
  -> orchagent NotificationConsumer
  -> feature orch handler
  -> STATE_DB / internal state / feature-specific output
```

orchagent 用 `swsscommon::NotificationConsumer` 處理 `ASIC_DB:NOTIFICATIONS`，
不是透過 SAI API call。

| Flow | Producer / Table | DB / Table / Channel | Consumer / Table |
| --- | --- | --- | --- |
| Async event 輸入 | syncd / `NotificationProducer` | `ASIC_DB:NOTIFICATIONS` | orchagent feature orch / `NotificationConsumer` |
| Port state 輸出 | `PortsOrch` / `Table` | `STATE_DB:PORT_TABLE\|<port>` | `portmgrd` / `Table`; `intfmgrd` 等 / `SubscriberStateTable` |
| FDB event 輸出 | `FdbOrch` / `Table` | `STATE_DB:FDB_TABLE` | `fdbsyncd` / `SubscriberStateTable` |

不要混淆這些 channel/table：

| Channel / Table | 用途 |
| --- | --- |
| `ASIC_DB:GETRESPONSE` | syncd 到 sairedis client 的 SAI request response（`ProducerTable` / `ConsumerTable`）。 |
| `ASIC_DB:NOTIFICATIONS` | syncd 到 orchagent 的非請求 async SAI/vendor event（`NotificationProducer` / `NotificationConsumer`）。 |
| `APPL_DB_<table>_RESPONSE_CHANNEL` on `APPL_STATE_DB` | APPL_DB intent 處理後的 northbound orch response（`NotificationProducer` / `NotificationConsumer`）。 |

### Notification Channel 使用總覽

SONiC 中所有 `NotificationProducer` / `NotificationConsumer` channel：

| Channel | DB | Producer | Consumer | 用途 |
| --- | --- | --- | --- | --- |
| `NOTIFICATIONS` | `ASIC_DB` | syncd | orchagent (`PortsOrch`, `FdbOrch`, `BfdOrch`, `TwampOrch`, `IcmpOrch`, `MACsecOrch`, `DashHaOrch`, `DashHaFlowOrch`, `PfcWdOrch`, `HFTelOrch`, `P4Orch`) | Async SAI events (port state, FDB, BFD 等) |
| `APPL_DB_<table>_RESPONSE_CHANNEL` | `APPL_STATE_DB` | orchagent `ResponsePublisher` | mgrd/sync daemons (如 `fpmsyncd`) | APPL_DB intent 處理後的 orch response |
| `RESTARTCHECK` | `STATE_DB` | `orchagent_restart_check` | `SwitchOrch` | Warm restart 就緒查詢 |
| `RESTARTCHECKREPLY` | `STATE_DB` | `SwitchOrch` | `orchagent_restart_check` | Warm restart 就緒回覆 |
| `SETTIMEOUTNAT` | `APPL_DB` | `NatOrch` | `natmgrd` | NAT entry timeout 通知 |
| `FLUSHNATENTRIES` | `APPL_DB` | `natmgrd` | `NatOrch` | NAT flush 請求 |
| `FLUSHFDBREQUEST` | `APPL_DB` | external | `FdbOrch` | FDB flush 請求 |
| `FLUSHNATSTATISTICS` | `APPL_DB` | external | `NatOrch` | NAT stats flush |
| `NAT_DB_CLEANUP_NOTIFICATION` | `APPL_DB` | external | `NatOrch` | NAT DB cleanup |
| `WM_CLEAR` | `APPL_DB` | CLI/test | `WatermarkOrch` | 清除 watermark counters |

兩個主要 pattern：

1. **Async ASIC events**：syncd → `ASIC_DB:NOTIFICATIONS` → orchagent（所有 orch
   consumer 共用同一 channel，依 event type dispatch）。
2. **Northbound APPL response**：orchagent `ResponsePublisher` →
   `APPL_DB_<table>_RESPONSE_CHANNEL` on `APPL_STATE_DB` → mgrd/sync daemon。

其餘是特定 feature 的 ad-hoc control channel。

## Northbound State 與 Response

處理 config 或 async event 後，orch 可能向北發布 state。這些通常是直接 table
write 或 response channel，不是 ASIC_DB object operation。

| Flow | Producer / Table | DB / Table / Channel | Consumer / Table |
| --- | --- | --- | --- |
| Operational state | orch / `Table` | `STATE_DB:<table>\|<key>` | mgrd/sync daemon / `Table` 或 `SubscriberStateTable` |
| Applied APPL intent state | orch `ResponsePublisher` / `Table` | `APPL_STATE_DB:<table>:<key>` | mgrd/sync daemon / `Table` |
| APPL intent response | orch `ResponsePublisher` / `NotificationProducer` | `APPL_DB_<table>_RESPONSE_CHANNEL` on `APPL_STATE_DB` | mgrd/sync daemon / `NotificationConsumer` |

`ResponsePublisher` 是 orchagent helper，連接到 `APPL_STATE_DB`。
DB write 端是 `Table`（寫入 applied state 到 `APPL_STATE_DB:<table>:<key>`）；
response channel 端是 `NotificationProducer`（發送到
`APPL_DB_<table>_RESPONSE_CHANNEL`，同樣在 `APPL_STATE_DB` 上）。

注意：P4Orch 是例外——它用 `"APPL_DB"` 建構 `ResponsePublisher`，所以 table
write 和 response channel 都走 `APPL_DB`。

## Route Flow Counters

Route flow counters 是最好的例子，說明 flow 不只是簡單的
`orch -> ASIC_DB -> syncd`。此 feature 用 CONFIG_DB 做控制、
FLEX_COUNTER_DB 或 sairedis extension attributes 做 polling 設定、
COUNTERS_DB 做顯示資料。

### 控制與設定路徑

| Flow | Producer / Table | DB / Table | Consumer / Table |
| --- | --- | --- | --- |
| 啟用 route flow counter | CLI / `Table` | `CONFIG_DB:FLEX_COUNTER_TABLE\|FLOW_CNT_ROUTE` | `FlexCounterOrch` / orch `Consumer` backed by `SubscriberStateTable` |
| Route pattern config | CLI / `Table` | `CONFIG_DB:FLOW_COUNTER_ROUTE_PATTERN_TABLE\|<vrf>\|<prefix>` | `FlowCounterRouteOrch` / orch `Consumer` backed by `SubscriberStateTable` |
| Polling 設定（傳統模式） | `FlowCounterRouteOrch` via `FlexCounterManager` / `ProducerTable` | `FLEX_COUNTER_DB:FLEX_COUNTER_TABLE:ROUTE_FLOW_COUNTER:<counter_oid>` field `FLOW_COUNTER_ID_LIST` | syncd flex counter logic |
| Route-to-counter mapping | `FlowCounterRouteOrch` / `Table` | `COUNTERS_DB:COUNTERS_ROUTE_NAME_MAP` | CLI/display / `Table` 或直接 Redis hash read |
| Route-to-pattern mapping | `FlowCounterRouteOrch` / `Table` | `COUNTERS_DB:COUNTERS_ROUTE_TO_PATTERN_MAP` | CLI/display / `Table` 或直接 Redis hash read |

CONFIG_DB consumer 透過 `Orch::addConsumer()` 註冊，所以 backend table 是
`SubscriberStateTable`。

傳統 flex counter 模式：

```text
FlowCounterRouteOrch
  -> FlexCounterManager::setCounterIdList()
  -> startFlexCounterPolling()
  -> gFlexCounterTable->set(...)
  -> ProducerTable(FLEX_COUNTER_DB, FLEX_COUNTER_TABLE)
```

key/field 為：

```text
FLEX_COUNTER_DB key:
  FLEX_COUNTER_TABLE:ROUTE_FLOW_COUNTER:<counter_oid>

field:
  FLOW_COUNTER_ID_LIST
```

非傳統 flex counter 模式中，`FlexCounterManager` 可能改用 sairedis switch
attributes 設定 syncd：

```text
SAI_REDIS_SWITCH_ATTR_FLEX_COUNTER
SAI_REDIS_SWITCH_ATTR_FLEX_COUNTER_GROUP
```

該非傳統路徑不是直接 `ProducerTable` write 到 `FLEX_COUNTER_DB`。

### Polling 與顯示路徑

實際 counter polling 由 syncd/flex counter logic 執行，不是 CLI，也不是
per-read orchagent SAI request。

```text
syncd flex counter logic
  -> vendor SAI get_counter_stats_ext or equivalent stats API
  -> COUNTERS_DB counter hash
  -> CLI/display reads COUNTERS_DB
```

| Flow | Producer / Table | DB / Table | Consumer / Table |
| --- | --- | --- | --- |
| Polled counter stats | syncd flex counter polling / `Table`-style hash write | `COUNTERS_DB:COUNTERS:<counter_oid>` | CLI/display / `Table` 或直接 Redis hash read |
| Route-to-counter mapping | `FlowCounterRouteOrch` / `Table` | `COUNTERS_DB:COUNTERS_ROUTE_NAME_MAP` | CLI/display / `Table` 或直接 Redis hash read |
| Route-to-pattern mapping | `FlowCounterRouteOrch` / `Table` | `COUNTERS_DB:COUNTERS_ROUTE_TO_PATTERN_MAP` | CLI/display / `Table` 或直接 Redis hash read |

`COUNTERS_DB` 是直接 state/stat 儲存。不要把它建模為
`ProducerTable` / `ConsumerTable`。

## 快速查表

| Flow | Producer / Table | DB / Table / Channel | Consumer / Table |
| --- | --- | --- | --- |
| Config source to mgrd/orch | CLI/config / `Table` | `CONFIG_DB:*` | mgrd/orch `Consumer` backed by `SubscriberStateTable` |
| mgrd intent to orch | mgrd / `ProducerStateTable` | `APPL_DB:*` pending state | orch / `ConsumerStateTable` |
| orch SAI request to syncd | orch through sairedis / `ProducerTable`-style ASIC operation | `ASIC_DB:ASIC_STATE:*` | syncd / `ConsumerTable` |
| syncd SAI response to orch | syncd / `ProducerTable` | `ASIC_DB:GETRESPONSE` | sairedis client side / `ConsumerTable` |
| syncd async event to orch | syncd / `NotificationProducer` | `ASIC_DB:NOTIFICATIONS` | orch / `NotificationConsumer` |
| orch state to mgrd | orch / `Table` | `STATE_DB:*` | mgrd / `Table` 或 `SubscriberStateTable` |
| orch APPL response to producer | orch `ResponsePublisher` / `NotificationProducer` | `APPL_DB_<table>_RESPONSE_CHANNEL` on `APPL_STATE_DB` | mgrd/sync daemon / `NotificationConsumer` |
| route flow counter enable | CLI / `Table` | `CONFIG_DB:FLEX_COUNTER_TABLE\|FLOW_CNT_ROUTE` | `FlexCounterOrch` / `Consumer` backed by `SubscriberStateTable` |
| route pattern config | CLI / `Table` | `CONFIG_DB:FLOW_COUNTER_ROUTE_PATTERN_TABLE\|<vrf>\|<prefix>` | `FlowCounterRouteOrch` / `Consumer` backed by `SubscriberStateTable` |
| route polling setup（傳統模式） | `FlowCounterRouteOrch` via `FlexCounterManager` / `ProducerTable` | `FLEX_COUNTER_DB:FLEX_COUNTER_TABLE:ROUTE_FLOW_COUNTER:<counter_oid>` | syncd flex counter logic |
| counter mappings | `FlowCounterRouteOrch` / `Table` | `COUNTERS_DB:COUNTERS_ROUTE_NAME_MAP`, `COUNTERS_ROUTE_TO_PATTERN_MAP` | CLI/display / `Table` 或直接 Redis read |
| counter stats | syncd flex counter polling / `Table`-style hash write | `COUNTERS_DB:COUNTERS:<counter_oid>` | CLI/display / `Table` 或直接 Redis read |

## 總結規則

- `CONFIG_DB` source rows 通常是 `Table` producer 和
  `SubscriberStateTable` consumer。
- `APPL_DB` intent rows 通常是 `ProducerStateTable` producer 和
  `ConsumerStateTable` consumer。
- SAI object programming 使用 sairedis 作為 Redis-backed SAI 實作：
  orch 呼叫 SAI，sairedis 序列化到 ASIC_DB，syncd 呼叫真正的 vendor SAI。
- SAI request response 使用 `ASIC_DB:GETRESPONSE`，搭配 `ProducerTable`（syncd）
  / `ConsumerTable`（sairedis client side）。
- `ASIC_DB:NOTIFICATIONS` 用於非請求 async event，由 orchagent 用
  `NotificationConsumer` 消費，不是透過 sairedis client API。
- Redis response 序列化屬於 syncd/sairedis-side code，在 vendor SAI 回傳
  SAI status/result 之後。
- 傳統 `FLEX_COUNTER_DB` 設定可用 `ProducerTable`；非傳統 flex counter 設定
  可用 sairedis switch attributes。
- `COUNTERS_DB` 是直接 state/stat 儲存，通常用 `Table` 或直接 Redis hash
  reads/writes 存取。
