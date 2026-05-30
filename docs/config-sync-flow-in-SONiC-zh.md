# VLAN 設定如何從 CONFIG_DB 同步到 ASIC

這份文件用 `config vlan add 100` 當主線，說明設定如何從 `CONFIG_DB`
一路流到 `APPL_DB`、`ASIC_DB`，最後由 `syncd` 套用到 ASIC。重點不是
Redis table 名稱本身，而是每一段 component 為什麼選不同的
`sonic-swss-common` table API。

讀者目標是能看懂 VLAN add/delete 在 SONiC-style DB pipeline 中的責任邊界，並能
用本 repo 的 `vlan_table` 範例驗證每個 materialization point。本文範圍限於教學版
control-plane flow；不涵蓋真實 orchagent、syncd 或 ASIC SDK 行為。

如果要看更完整的 SONiC DB producer/consumer table 類型整理，包括 SAI
request/response、async notification、route flow counter 與 COUNTERS_DB，請看
[sonic-swss-common-table-usage-in-SONiC.md](sonic-swss-common-table-usage-in-SONiC.md)。

## 系統模型

`sonic-swss-common` 是 SONiC control-plane components 讀寫 Redis-backed
SONiC DB 的 shared client library。它不是 Redis server，也不是 generic
Redis client；它把 SONiC DB name、table separator、producer/consumer queue、
latest-state coalescing 與 notification pattern 包成 component 可以共用的 API。

用 raw `redis-py` 也能寫 Redis，但要自己處理 DB ID、separator、pending hash、
key set、queue key 與 Lua atomicity。用 `sonic-swss-common` 的好處是行為更接近
production SONiC component；代價是 runtime 需要 SONiC DB config 與 swsscommon
library，而且必須選對 table family。

這個 VLAN flow 會用到四類 table family：

| Table family | 本例用途 |
| --- | --- |
| `Table` | config command 直接 materialize `CONFIG_DB VLAN|Vlan100`。 |
| `SubscriberStateTable` | `vlanmgrd` watch `CONFIG_DB VLAN` updates。 |
| `ProducerStateTable` / `ConsumerStateTable` | `vlanmgrd` 寫 APPL pending state，`PortsOrch` consume 並 materialize final APPL_DB hash。 |
| `ProducerTable` / `ConsumerTable` | `PortsOrch` enqueue ordered ASIC operation，`syncd` consume 並 materialize final ASIC_DB hash。 |

## End-To-End VLAN Flow

### Flow Behavior

```text
config_vlan_command.py
  Table(CONFIG_DB, "VLAN").set("Vlan100", {"vlanid": "100"})
  -> DB 4 hash: VLAN|Vlan100

vlanmgrd.py
  Table(CONFIG_DB, "VLAN").get("Vlan100") 或 SubscriberStateTable.pop()
  ProducerStateTable(APPL_DB, "VLAN_TABLE").set("Vlan100", {"vlanid": "100"})
  -> DB 0 pending hash: _VLAN_TABLE:Vlan100
  -> DB 0 pending set:  VLAN_TABLE_KEY_SET

portorch.py
  ConsumerStateTable(APPL_DB, "VLAN_TABLE").pop()
  -> materialize DB 0 final hash: VLAN_TABLE:Vlan100
  ProducerTable(ASIC_DB, "ASIC_STATE:SAI_OBJECT_TYPE_VLAN").set(...)
  -> enqueue DB 1 list: ASIC_STATE:SAI_OBJECT_TYPE_VLAN_KEY_VALUE_OP_QUEUE

syncd.py
  ConsumerTable(ASIC_DB, "ASIC_STATE:SAI_OBJECT_TYPE_VLAN").pop()
  -> materialize DB 1 final hash:
     ASIC_STATE:SAI_OBJECT_TYPE_VLAN:oid:0x26000000000100
  -> pretend write ASIC
  ProducerTable(ASIC_DB, "GETRESPONSE").set("SAI_STATUS_SUCCESS", ..., "getresponse")
  -> sairedis GETRESPONSE table 回到 PortsOrch / sairedis requester

portorch.py
  NotificationProducer(APPL_STATE_DB, "APPL_DB_VLAN_TABLE_RESPONSE_CHANNEL").send(...)
  -> APPL response channel 回到 vlanmgrd / northbound producer

syncd.py async notification
  NotificationProducer(ASIC_DB, "NOTIFICATIONS").send("port_state_change", ...)
  -> PortsOrch NotificationConsumer
  -> STATE_DB PORT_TABLE|Ethernet0
  -> vlanmgrd / other manager daemons
```

端到端關係：

| Step | Component | Source | API | Output | 為什麼用這個 API |
| --- | --- | --- | --- | --- | --- |
| 1 | config command | user intent | `Table.set` | `CONFIG_DB VLAN|Vlan100` | config 是 durable desired config，直接寫 hash；不需要 queue。 |
| 2 | vlanmgrd | `CONFIG_DB VLAN` | `Table.get` / `SubscriberStateTable.pop` | 讀到 `Vlan100 SET` | 啟動時可 replay 既有 config；watch 模式可訂閱後續 config change。 |
| 3 | vlanmgrd | config event | `ProducerStateTable.set` | `_VLAN_TABLE:Vlan100` + `VLAN_TABLE_KEY_SET` | APPL_DB 是 desired state；同 key 多次更新可 coalesce，只需要最新狀態。 |
| 4 | PortsOrch | APPL_DB pending | `ConsumerStateTable.pop` | `VLAN_TABLE:Vlan100` | APPL table owner 消費 pending state，並 materialize final APPL_DB hash。 |
| 5 | PortsOrch | APPL_DB final/update | `ProducerTable.set` | ASIC_DB queue | ASIC operation 需要有序傳給 syncd；每個 create/remove/set operation 都重要。 |
| 6 | syncd | ASIC_DB queue | `ConsumerTable.pop` | ASIC_DB final hash + fake ASIC write | syncd 是 ASIC_DB operation stream 的 consumer；pop 時 materialize final ASIC_DB hash。 |
| 7 | syncd | SAI operation result | `ProducerTable.set` | `ASIC_DB GETRESPONSE` with op `getresponse` | SAI create/remove 的同步結果回給 requester；這是 sairedis/syncd 的實際 Redis sync response path。 |
| 8 | PortsOrch | SAI operation result | `NotificationProducer.send` | `APPL_DB_VLAN_TABLE_RESPONSE_CHANNEL` on `APPL_STATE_DB` | `PortsOrch` 把 ASIC result propagate 回 vlanmgrd/northbound producer。 |
| 9 | syncd | ASIC async event | `NotificationProducer.send` | ASIC_DB `NOTIFICATIONS` channel | port state change 這類 async event 不是 table operation queue。 |
| 10 | PortsOrch | ASIC async notification | `NotificationConsumer.pop` + `Table.set` | `STATE_DB PORT_TABLE|Ethernet0` | `PortsOrch` 把 syncd notification 轉成可被 mgrd/其他 daemon 讀取的 state table。 |

最重要的結論：

```text
CONFIG_DB final hash:
  config command 的 Table.set 直接 materialize

APPL_DB final hash:
  不是 vlanmgrd materialize
  是 PortsOrch 的 ConsumerStateTable.pop materialize

ASIC_DB final hash:
  不是 PortsOrch materialize
  是 syncd 的 ConsumerTable.pop materialize

SAI response:
  不是另一個 ASIC_STATE table
  是 syncd 透過 ASIC_DB `GETRESPONSE` table 回覆 requester，op 是 `getresponse`
  然後 PortsOrch 透過 APPL response channel 回覆 vlanmgrd / northbound producer

Async notification:
  不是 vlanmgrd 直接從 syncd 收到
  是 syncd -> ASIC_DB NOTIFICATIONS -> PortsOrch -> STATE_DB -> vlanmgrd
```

### Redis Contract

SONiC Redis table 本質上不是 Redis 需要預先宣告的 schema，而是「key 命名慣例」加上「producer / consumer 合約」。

以 VLAN 建立流程為例，至少要明確知道：

- 使用哪個 DB：`CONFIG_DB` 是 DB 4，`APPL_DB` 是 DB 0，`ASIC_DB` 是 DB 1，`STATE_DB` 是 DB 6，`APPL_STATE_DB` 是 DB 14。
- table 名稱：`CONFIG_DB` 使用 `VLAN`，`APPL_DB` 使用 `VLAN_TABLE`。
- `ASIC_DB` 的 VLAN 範例使用 `ASIC_STATE:SAI_OBJECT_TYPE_VLAN`。
- SAI response 用 ASIC_DB `GETRESPONSE` table；APPL response 用 `APPL_DB_<table>_RESPONSE_CHANNEL` on `APPL_STATE_DB`；async event 用 ASIC_DB `NOTIFICATIONS` channel。
- async port state 最後會被 PortsOrch materialize 到 `STATE_DB PORT_TABLE|Ethernet0`。
- key separator：`CONFIG_DB` 用 `|`，`APPL_DB` / `ASIC_DB` 用 `:`。
- 欄位語意：`vlanid` 表示 VLAN ID。
- 寫入權：CLI/config tooling 寫 `CONFIG_DB`，`vlanmgrd` 寫 `APPL_DB`
  pending state，`PortsOrch` 消費 `APPL_DB` update 並 enqueue `ASIC_DB`
  operation，`syncd` 消費 `ASIC_DB` queue 並假裝寫 ASIC。
- 事件語意：`CONFIG_DB` 用 direct hash；`APPL_DB` 用 latest-state pending
  update；`ASIC_DB` 用 ordered operation queue。

用 Redis key 表示的簡化流程是：

```text
config vlan add 100
    -> CONFIG_DB VLAN|Vlan100
    -> vlanmgrd SubscriberStateTable
    -> APPL_DB _VLAN_TABLE:Vlan100
    -> APPL_DB VLAN_TABLE_KEY_SET
    -> PortsOrch ConsumerStateTable
    -> APPL_DB VLAN_TABLE:Vlan100
    -> ASIC_DB ASIC_STATE:SAI_OBJECT_TYPE_VLAN_KEY_VALUE_OP_QUEUE
    -> syncd ConsumerTable
    -> ASIC_DB ASIC_STATE:SAI_OBJECT_TYPE_VLAN:oid:0x26000000000100
```

`_VLAN_TABLE:Vlan100` 和 `VLAN_TABLE_KEY_SET` 是 `ProducerStateTable` 產生的 pending state。`PortsOrch` 會用 `ConsumerStateTable` 消費 pending state，並 materialize 或 apply 最終的 `VLAN_TABLE:Vlan100`。
`ASIC_DB` 的 queue 是 `ProducerTable` 產生的 operation stream；最終
`ASIC_STATE:...` hash 是 `syncd` 的 `ConsumerTable.pop()` materialize。

## Component Responsibilities

### config command -> CONFIG_DB：`Table`

`config vlan add 100` 是使用者的 desired configuration。這份 config 要直接存在
`CONFIG_DB`，讓後續 daemon 或重新啟動後的 replay 都能讀到。所以使用最直接的
`Table.set()`：

```python
config_db = swsscommon.DBConnector("CONFIG_DB", 0, False)
config_table = swsscommon.Table(config_db, "VLAN")
config_table.set("Vlan100", [("vlanid", "100")])
```

Redis 實際狀態：

```text
DB 4 hash: VLAN|Vlan100
  vlanid = 100
```

這段不用 `ProducerTable` 或 `ProducerStateTable`，因為 CONFIG_DB 不是一個
daemon-to-daemon work queue；它是 configuration source of truth。

### CONFIG_DB -> vlanmgrd：`Table.get` / `SubscriberStateTable`

`vlanmgrd` 有兩種讀 CONFIG_DB 的方式：

- 啟動/replay 既有 config：`Table.get("Vlan100")`
- watch 後續變更：`SubscriberStateTable.pop()`

在本專案的一鍵驗證中，流程刻意先跑 config command，再跑 `vlanmgrd.py`，所以
`vlanmgrd.py` 會先用 `Table.get()` replay 既有 `CONFIG_DB VLAN|Vlan100`。
在長駐模式下，`--watch` 則會用 `SubscriberStateTable` 等後續 keyspace event。

這段不用 `ConsumerStateTable`，因為 CONFIG_DB 是 direct hash + keyspace
subscription，不是 `ProducerStateTable` 產生的 pending state。

### vlanmgrd -> APPL_DB：`ProducerStateTable`

`vlanmgrd` 把 config 轉成 application desired state：

```python
appl_db = swsscommon.DBConnector("APPL_DB", 0, False)
appl_table = swsscommon.ProducerStateTable(appl_db, "VLAN_TABLE")
appl_table.set("Vlan100", [("vlanid", "100")])
```

Redis `MONITOR` 可看到：

```text
SADD VLAN_TABLE_KEY_SET Vlan100
HSET _VLAN_TABLE:Vlan100 vlanid 100
```

它不會直接寫 `VLAN_TABLE:Vlan100`。選 `ProducerStateTable` 的原因是
APPL_DB 這段是 desired state publication：如果短時間對同一個 VLAN 做多次更新，
PortsOrch 通常只需要最新狀態，不需要每一個 intermediate edit。

### APPL_DB -> PortsOrch：`ConsumerStateTable`

`PortsOrch` 是 `VLAN_TABLE` 的 owner。它消費 pending state：

```python
consumer = swsscommon.ConsumerStateTable(appl_db, "VLAN_TABLE")
key, op, field_values = consumer.pop()
```

Redis `MONITOR` 可看到：

```text
SPOP VLAN_TABLE_KEY_SET
HGETALL _VLAN_TABLE:Vlan100
HSET VLAN_TABLE:Vlan100 vlanid 100
DEL _VLAN_TABLE:Vlan100
```

這就是 APPL_DB final hash materialize 的時間點。`vlanmgrd` 只是 producer；
真正把 `_VLAN_TABLE:Vlan100` 轉成 `VLAN_TABLE:Vlan100` 的是
`ConsumerStateTable.pop()`。

### PortsOrch -> ASIC_DB：`ProducerTable`

PortsOrch 根據 APPL_DB desired state 產生 ASIC operation：

```python
asic_db = swsscommon.DBConnector("ASIC_DB", 0, False)
asic_producer = swsscommon.ProducerTable(
    asic_db,
    "ASIC_STATE:SAI_OBJECT_TYPE_VLAN",
)
asic_producer.set("oid:0x26000000000100", [
    ("SAI_VLAN_ATTR_VLAN_ID", "100"),
    ("source", "PortsOrch"),
])
```

Redis `MONITOR` 可看到：

```text
LPUSH ASIC_STATE:SAI_OBJECT_TYPE_VLAN_KEY_VALUE_OP_QUEUE \
  oid:0x26000000000100 \
  ["SAI_VLAN_ATTR_VLAN_ID","100","source","PortsOrch"] \
  SSET
```

這段選 `ProducerTable`，不是 `ProducerStateTable`，因為 ASIC_DB 是傳給 syncd
的 ordered operation stream。create/remove/set 的順序會影響硬體狀態，不能只保留
latest state。

### ASIC_DB -> syncd -> ASIC：`ConsumerTable`

`syncd` 消費 PortsOrch enqueue 的 ASIC operation：

```python
asic_consumer = swsscommon.ConsumerTable(
    asic_db,
    "ASIC_STATE:SAI_OBJECT_TYPE_VLAN",
)
key, op, field_values = asic_consumer.pop()
```

Redis `MONITOR` 可看到：

```text
LRANGE ASIC_STATE:SAI_OBJECT_TYPE_VLAN_KEY_VALUE_OP_QUEUE
LTRIM ASIC_STATE:SAI_OBJECT_TYPE_VLAN_KEY_VALUE_OP_QUEUE
HSET ASIC_STATE:SAI_OBJECT_TYPE_VLAN:oid:0x26000000000100 SAI_VLAN_ATTR_VLAN_ID 100
HSET ASIC_STATE:SAI_OBJECT_TYPE_VLAN:oid:0x26000000000100 source PortsOrch
```

這就是 ASIC_DB final hash materialize 的時間點。接著本專案的 tiny
`syncd.py` 只印出 `pretend write ASIC`；真實 SONiC `syncd` 會把這個 SAI
operation 轉成 ASIC SDK/SAI call。

## Implementation Context

### Table 名稱定義在哪裡

VLAN 是 SONiC upstream 已存在的 table，不是 project-local custom table。正式 SONiC 程式通常會使用 upstream schema 常數，例如：

```c
#include "sonic-swss-common/common/schema.h"

#define APP_VLAN_TABLE_NAME "VLAN_TABLE"
```

`CONFIG_DB` 的 VLAN table 名稱是 `VLAN`。在本教學專案中，Python 範例把需要的常數集中在：

```python
CFG_VLAN_TABLE_NAME = "VLAN"
APP_VLAN_TABLE_NAME = "VLAN_TABLE"
ASIC_VLAN_TABLE_NAME = "ASIC_STATE:SAI_OBJECT_TYPE_VLAN"
VLAN_PREFIX = "Vlan"
```

Redis 不會檢查這些 table 名稱是否存在於 `schema.h`。常數的價值是讓 CLI 模擬程式、`vlanmgrd`、`portorch` 使用同一份命名合約。

### 是否需要 YANG 或 gen_cfg_schema.py

這個 VLAN_TABLE 教學例子不需要新增 YANG model 或跑 `gen_cfg_schema.py`。

原因是：

- `VLAN` / `VLAN_TABLE` 已經是 SONiC 既有概念。
- 範例目標是示範 Redis table API 與資料流，不是新增一個正式 SONiC schema。
- 我們只需要寫入 `CONFIG_DB VLAN|Vlan100`，再觀察 `APPL_DB VLAN_TABLE`
  和 `ASIC_DB ASIC_STATE` update。

若要把新的 config table 變成正式 SONiC platform feature，才需要考慮：

- CONFIG_DB schema validation。
- SONiC CLI / config tooling 整合。
- YANG model。
- upstream-style generated schema header。

### database_config.json

`database_config.json` 告訴 `swsscommon`：

- logical DB 名稱對應哪個 Redis instance。
- DB index 是多少。
- table key separator 是什麼。
- Unix socket 或 TCP endpoint 在哪裡。

範例：

```json
{
    "INSTANCES": {
        "redis": {
            "hostname": "database",
            "port": 6379,
            "unix_socket_path": "/var/run/redis/redis.sock"
        }
    },
    "DATABASES": {
        "APPL_DB": {
            "id": 0,
            "separator": ":",
            "instance": "redis"
        },
        "ASIC_DB": {
            "id": 1,
            "separator": ":",
            "instance": "redis"
        },
        "CONFIG_DB": {
            "id": 4,
            "separator": "|",
            "instance": "redis"
        }
    },
    "VERSION": "1.0"
}
```

在 SONiC 系統中，常見位置是：

```text
/var/run/redis/sonic-db/database_config.json
```

本專案把 host `/var/run/redis` 同時掛到 `database` 和 `runner` container。
`runner` 的 `entrypoint.sh` 會把專案根目錄的 `database_config.json` copy 到
`/var/run/redis/sonic-db/database_config.json`，並提供
`/var/run/redis/redis.sock` 給 `swsscommon` 使用。這樣避免在
`/var/run/redis` 底下做 nested bind mount。

Python 也可以明確載入 config：

```python
swsscommon.SonicDBConfig.load_sonic_db_config("/tmp/database_config.json")
```

然後用 logical DB name 連線：

```python
config_db = swsscommon.DBConnector("CONFIG_DB", 0, False)
appl_db = swsscommon.DBConnector("APPL_DB", 0, False)
```

第三個參數 `False` 表示使用 SONiC Unix socket，不走 TCP。

## Table API Reference For The VLAN Flow

選 API 時，核心問題是：你需要直接 hash access、CONFIG_DB change subscription、有序 operation stream，還是 latest-state coalescing。

### Table

`Table` 是直接 Redis hash access，並套用 SONiC table naming rule。

在 VLAN add/delete 流程中，`config vlan` 類型的 command 會直接修改 `CONFIG_DB`：

```python
config_db = swsscommon.DBConnector("CONFIG_DB", 0, False)
table = swsscommon.Table(config_db, "VLAN")

table.set("Vlan100", swsscommon.FieldValuePairs([
    ("vlanid", "100"),
]))
```

因為 `CONFIG_DB` separator 是 `|`，實際 Redis key 是：

```text
DB 4 hash: VLAN|Vlan100
```

對應到 command：

```text
config vlan add 100
```

寫入內容：

```text
VLAN|Vlan100 -> {"vlanid": "100"}
```

刪除 VLAN 時：

```python
table.delete("Vlan100")
```

對應到 command：

```text
config vlan del 100
```

`Table` 不會產生 producer / consumer message queue。若其他 daemon 要反應 `CONFIG_DB` 變化，應該用 `SubscriberStateTable` 訂閱。

### SubscriberStateTable

`SubscriberStateTable` 用來 watch table update，最常用在 `CONFIG_DB`。在 VLAN 流程中，`vlanmgrd` 會訂閱 `CONFIG_DB VLAN`。

基本 loop：

```python
config_db = swsscommon.DBConnector("CONFIG_DB", 0, False)
subscriber = swsscommon.SubscriberStateTable(config_db, "VLAN")

selector = swsscommon.Select()
selector.addSelectable(subscriber)

while True:
    state, selectable = selector.select()
    if state != swsscommon.Select.OBJECT:
        continue

    key, op, field_values = subscriber.pop()
```

當使用者執行：

```text
config vlan add 100
```

`vlanmgrd` 會看到類似：

```text
key = "Vlan100"
op = "SET"
field_values = [("vlanid", "100")]
```

當使用者執行：

```text
config vlan del 100
```

`vlanmgrd` 會看到：

```text
key = "Vlan100"
op = "DEL"
field_values = []
```

local Redis 必須啟用 keyspace notifications：

```text
--notify-keyspace-events KEA
```

`SubscriberStateTable` 只是觀察變化，不是 ownership boundary。`CONFIG_DB VLAN` 仍應由 CLI/config tooling 這類 config owner 寫入。

### ProducerTable 與 ConsumerTable

`ProducerTable` / `ConsumerTable` 是有序 operation stream。producer 送出每一個 operation，consumer 按順序 pop 並處理。

適合用在：

- 每一個 operation 都重要。
- table 內 operation order 重要。
- 只保留 latest state 會造成語意錯誤。

概念上的 Redis 結構：

```text
producer:
  push key/op/field-values into <TABLE>_KEY_VALUE_OP_QUEUE
  publish <TABLE>_CHANNEL

consumer:
  pop list in order
  decode key/op/field-values
  apply each operation
```

VLAN_TABLE 的常見 desired-state 流程通常不需要這組，因為 `PortsOrch` 多半只需要某個 VLAN key 的最新 desired state，而不是每個 intermediate edit。

### ProducerStateTable 與 ConsumerStateTable

`ProducerStateTable` / `ConsumerStateTable` 是 latest desired state 模型。producer 寫 pending state，consumer 負責 materialize 或 apply state。

在 VLAN 流程中，`vlanmgrd` 收到 `CONFIG_DB VLAN|Vlan100 SET` 後，會發布 APPL_DB desired state：

```python
appl_db = swsscommon.DBConnector("APPL_DB", 0, False)
vlan_table = swsscommon.ProducerStateTable(appl_db, "VLAN_TABLE")

vlan_table.set("Vlan100", swsscommon.FieldValuePairs([
    ("vlanid", "100"),
]))
```

概念上的 Redis 結構：

```text
producer:
  HSET _VLAN_TABLE:Vlan100 vlanid 100
  SADD VLAN_TABLE_KEY_SET Vlan100
  publish VLAN_TABLE_CHANNEL

consumer:
  SPOP VLAN_TABLE_KEY_SET
  HGETALL _VLAN_TABLE:Vlan100
  apply or materialize latest state
  DEL _VLAN_TABLE:Vlan100
```

`vlanmgrd` 收到 `CONFIG_DB VLAN|Vlan100 DEL` 後，會發布 delete：

```python
vlan_table.delete("Vlan100")
```

`PortsOrch` 則用 `ConsumerStateTable` 消費：

```python
appl_db = swsscommon.DBConnector("APPL_DB", 0, False)
consumer = swsscommon.ConsumerStateTable(appl_db, "VLAN_TABLE")

selector = swsscommon.Select()
selector.addSelectable(consumer)

while True:
    state, selectable = selector.select()
    if state != swsscommon.Select.OBJECT:
        continue

    key, op, field_values = consumer.pop()
    print(key, op, field_values)
```

本例中可能印出：

```text
Vlan100 SET [("vlanid", "100")]
Vlan100 DEL []
```

coalescing 規則：

- 相同 key、相同 field：最後 pending value wins。
- 相同 key、不同 fields：合併成同一份 latest snapshot。
- 不同 keys：處理順序不保證。

所以 `ProducerStateTable` 很適合 `CONFIG_DB VLAN -> vlanmgrd -> APPL_DB VLAN_TABLE` 的 desired state publication。`PortsOrch` 通常只需要最新 APPL_DB desired state，不需要每一次 config edit 的 intermediate event。

## API 選擇與 Ownership

```text
config vlan add/del:
  Table

vlanmgrd 觀察 CONFIG_DB VLAN:
  SubscriberStateTable

vlanmgrd 發布 APPL_DB VLAN_TABLE:
  ProducerStateTable

PortsOrch 消費 APPL_DB VLAN_TABLE:
  ConsumerStateTable

PortsOrch 發布 ASIC_DB operation:
  ProducerTable

syncd 消費 ASIC_DB operation:
  ConsumerTable
```

完整 VLAN pipeline：

```text
CONFIG_DB writer:
  config vlan add 100
  config vlan del 100

CONFIG_DB app reader:
  vlanmgrd SubscriberStateTable("VLAN")

APPL_DB app writer:
  vlanmgrd ProducerStateTable("VLAN_TABLE")

APPL_DB table owner:
  PortsOrch ConsumerStateTable("VLAN_TABLE")

ASIC_DB operation producer:
  PortsOrch ProducerTable("ASIC_STATE:SAI_OBJECT_TYPE_VLAN")

ASIC_DB operation consumer:
  syncd ConsumerTable("ASIC_STATE:SAI_OBJECT_TYPE_VLAN")
```

### Atomicity 與 Lock

producer / consumer table APIs 內部會使用 Redis Lua，讓它們負責的 multi-command Redis operation 在 Redis server 裡 atomic 執行。

這能避免：

- pending state 寫到一半。
- message publish 和資料更新之間 race。
- consumer 看到 partial table operation。

但這不等於整個 VLAN workflow 都被 mutual exclusive 保護。

這種是 table operation atomic：

```text
ProducerStateTable.set("Vlan100", [("vlanid", "100")])
```

這種不是自動整體 atomic：

```text
read CONFIG_DB VLAN
read another CONFIG_DB table
compute vlan state
write APPL_DB VLAN_TABLE
write another APPL_DB table
```

如果 workflow 需要全域互斥，所有參與者都必須遵守同一套設計。優先考慮：

- single writer / table owner。
- 不同 app 不共享 input/output tables。
- version field 或 generation ID。
- apply / commit marker。
- idempotent convergence from CONFIG_DB to APPL_DB。

只有真的存在 shared resource，而且所有 writer / reader 都會遵守時，才加 explicit Redis lock。

### Ownership Pattern

VLAN_TABLE 的 ownership 可以理解成：

```text
CONFIG_DB VLAN:
  owner = config CLI / config reload / config management

CONFIG_DB VLAN consumer:
  owner = vlanmgrd

APPL_DB VLAN_TABLE pending state:
  producer = vlanmgrd

APPL_DB VLAN_TABLE consumer:
  owner = PortsOrch in orchagent

ASIC_DB ASIC_STATE operation queue:
  producer = PortsOrch / orchagent
  consumer = syncd

ASIC_DB final ASIC_STATE hash:
  materialized by syncd ConsumerTable.pop()
```

要避免：

```text
multiple daemons -> same APPL_DB VLAN_TABLE key range
```

除非有明確 owner、allocator、version check 或 explicit lock。

## Running The Example

### Test environment

本機測試環境使用 repo 的 Compose stack：

- `database`：Redis，socket 在 `/var/run/redis/redis.sock`。
- `runner`：build/install local `sonic-swss-common` 的 container。
- `swss-common-install`：掛在 `/usr/local` 的 named volume。

初始化：

```bash
cd /home/ubuntu/swss-common-example
sudo mkdir -p /var/run/redis
docker compose build runner
docker compose up -d database
```

### Method 1: Pure bash commands

在三個 terminal 啟動 watch components：

```bash
cd /home/ubuntu/swss-common-example
UID=$(id -u) GID=$(id -g) docker compose run --rm -T runner \
  src/swss/vlan_table/syncd.py --vlan-id 100 --watch
```

```bash
cd /home/ubuntu/swss-common-example
UID=$(id -u) GID=$(id -g) docker compose run --rm -T runner \
  src/swss/vlan_table/portorch.py --vlan-id 100 --watch
```

```bash
cd /home/ubuntu/swss-common-example
UID=$(id -u) GID=$(id -g) docker compose run --rm -T runner \
  src/swss/vlan_table/vlanmgrd.py --vlan-id 100 --watch
```

第四個 terminal 模擬 `config vlan add 100`：

```bash
cd /home/ubuntu/swss-common-example
UID=$(id -u) GID=$(id -g) docker compose run --rm -T runner \
  src/swss/vlan_table/config_vlan_command.py add 100
```

驗證 add path 後，可再測 delete：

```bash
cd /home/ubuntu/swss-common-example
UID=$(id -u) GID=$(id -g) docker compose run --rm -T runner \
  src/swss/vlan_table/config_vlan_command.py del 100
```

### Method 2: Helper scripts

長時間 watch commands 建議用 helper scripts。它們會啟動 `database`、export
`UID`/`GID`、把參數傳給 component，並在 `Ctrl-C` 時移除 runner container。

```bash
cd /home/ubuntu/swss-common-example
scripts/run_vlan_table_example.sh syncd --vlan-id 100 --watch
```

```bash
cd /home/ubuntu/swss-common-example
scripts/run_vlan_table_example.sh portorch --vlan-id 100 --watch
```

```bash
cd /home/ubuntu/swss-common-example
scripts/run_vlan_table_example.sh mgrd --vlan-id 100 --watch
```

```bash
cd /home/ubuntu/swss-common-example
scripts/run_vlan_table_example.sh config-add 100
```

驗證 add path 後，可再測 delete：

```bash
cd /home/ubuntu/swss-common-example
scripts/run_vlan_table_example.sh config-del 100
```

## Route Flow Counter Table Split

VLAN flow 是 `CONFIG_DB -> APPL_DB -> ASIC_DB` 的設定同步範例；route flow
counter 則展示 counter 類功能常見的另一種資料流。重點是：

- `CONFIG_DB` 還是 CLI/config tooling 直接用 `Table` 寫入。
- orchagent 內的 CONFIG_DB consumer 是 `Consumer` wrapper，底層是
  `SubscriberStateTable`。
- traditional flex counter setup 用 `ProducerTable` 寫 `FLEX_COUNTER_DB`。
- `COUNTERS_DB` mapping 與 stats 是 direct hash/state，通常用 `Table` 或
  direct Redis hash read/write，不是 `ConsumerTable.pop()` pipeline。

| Flow | Producer / Table | DB / Table | Consumer / Table |
| --- | --- | --- | --- |
| Enable route flow counter | CLI / `Table` | `CONFIG_DB:FLEX_COUNTER_TABLE|FLOW_CNT_ROUTE` | `FlexCounterOrch` / orch `Consumer` backed by `SubscriberStateTable` |
| Route pattern config | CLI / `Table` | `CONFIG_DB:FLOW_COUNTER_ROUTE_PATTERN_TABLE|<vrf>|<prefix>` | `FlowCounterRouteOrch` / orch `Consumer` backed by `SubscriberStateTable` |
| Polling setup, traditional mode | `FlowCounterRouteOrch` via `FlexCounterManager` / `ProducerTable` | `FLEX_COUNTER_DB:FLEX_COUNTER_TABLE:ROUTE_FLOW_COUNTER:<counter_oid>` field `FLOW_COUNTER_ID_LIST` | `syncd` flex counter logic |
| Route-to-counter mapping | `FlowCounterRouteOrch` / `Table` | `COUNTERS_DB:COUNTERS_ROUTE_NAME_MAP` | CLI/display / `Table` or direct Redis hash read |
| Route-to-pattern mapping | `FlowCounterRouteOrch` / `Table` | `COUNTERS_DB:COUNTERS_ROUTE_TO_PATTERN_MAP` | CLI/display / `Table` or direct Redis hash read |
| Polled counter stats | `syncd` flex counter polling / `Table`-style hash write | `COUNTERS_DB:COUNTERS:<counter_oid>` | CLI/display / `Table` or direct Redis hash read |

non-traditional flex counter mode 可能透過 sairedis extension attribute，例如
`SAI_REDIS_SWITCH_ATTR_FLEX_COUNTER`，設定 syncd。這種模式不是直接用
`ProducerTable` produce `FLEX_COUNTER_DB` table。

## Verification

最快的驗證方式是跑一鍵 script：

```bash
cd /home/ubuntu/swss-common-example
scripts/run_vlan_table_example.sh verify 100
```

這個 script 會清掉 DB 0、DB 1、DB 4、DB 6，依序執行：

```text
config command -> CONFIG_DB check -> vlanmgrd -> APPL_DB check
  -> syncd/portorch SAI request-response check
  -> syncd async notification -> portorch -> STATE_DB -> vlanmgrd check
```

它會同時啟動 Redis `MONITOR`，並輸出兩份檔案：

```text
/tmp/swss_vlan_monitor_*.log  # raw Redis MONITOR log
/tmp/swss_vlan_pretty_*.log   # 依 __VERIFY_MARKER 分組的 pretty log
```

檢查 Redis：

```bash
docker exec database redis-cli -s /var/run/redis/redis.sock -n 4 HGETALL 'VLAN|Vlan100'
docker exec database redis-cli -s /var/run/redis/redis.sock -n 0 HGETALL '_VLAN_TABLE:Vlan100'
docker exec database redis-cli -s /var/run/redis/redis.sock -n 0 SMEMBERS 'VLAN_TABLE_KEY_SET'
docker exec database redis-cli -s /var/run/redis/redis.sock -n 0 HGETALL 'VLAN_TABLE:Vlan100'
docker exec database redis-cli -s /var/run/redis/redis.sock -n 1 LRANGE 'ASIC_STATE:SAI_OBJECT_TYPE_VLAN_KEY_VALUE_OP_QUEUE' 0 -1
docker exec database redis-cli -s /var/run/redis/redis.sock -n 1 HGETALL 'ASIC_STATE:SAI_OBJECT_TYPE_VLAN:oid:0x26000000000100'
docker exec database redis-cli -s /var/run/redis/redis.sock -n 6 HGETALL 'PORT_TABLE|Ethernet0'
```

## Appendix: Final Table Materialization

目前用 `MONITOR` 驗證到的 materialization 結論是：

```text
CONFIG_DB final VLAN|Vlan100:
  config_vlan_command.py Table.set 直接 HSET

APPL_DB pending _VLAN_TABLE:Vlan100 / VLAN_TABLE_KEY_SET:
  vlanmgrd.py ProducerStateTable.set 寫入

APPL_DB final VLAN_TABLE:Vlan100:
  portorch.py ConsumerStateTable.pop 裡的 Lua HSET 寫入

ASIC_DB queue ASIC_STATE:SAI_OBJECT_TYPE_VLAN_KEY_VALUE_OP_QUEUE:
  portorch.py ProducerTable.set 裡的 Lua LPUSH 寫入

ASIC_DB final ASIC_STATE:SAI_OBJECT_TYPE_VLAN:oid:0x26000000000100:
  syncd.py ConsumerTable.pop 裡的 Lua HSET 寫入
```

`syncd.py` 的核心邏輯是：

```python
asic_db = swsscommon.DBConnector("ASIC_DB", 0, False)
asic_consumer = swsscommon.ConsumerTable(
    asic_db,
    "ASIC_STATE:SAI_OBJECT_TYPE_VLAN",
)

selector = swsscommon.Select()
selector.addSelectable(asic_consumer)

state, _ = selector.select()
if state == swsscommon.Select.OBJECT:
    key, op, field_values = asic_consumer.pop()
```

當 `portorch.py` 已經用 `ProducerTable.set()` 寫入 queue 後，Redis 裡會先有：

```text
DB 1 list: ASIC_STATE:SAI_OBJECT_TYPE_VLAN_KEY_VALUE_OP_QUEUE
  SSET
  ["SAI_VLAN_ATTR_VLAN_ID","100","source","PortsOrch"]
  oid:0x26000000000100
```

`syncd.py` 呼叫 `ConsumerTable.pop()` 時，`swsscommon` 內部 Lua 會：

```text
LRANGE ASIC_STATE:SAI_OBJECT_TYPE_VLAN_KEY_VALUE_OP_QUEUE
LTRIM ASIC_STATE:SAI_OBJECT_TYPE_VLAN_KEY_VALUE_OP_QUEUE
HSET ASIC_STATE:SAI_OBJECT_TYPE_VLAN:oid:0x26000000000100 SAI_VLAN_ATTR_VLAN_ID 100
HSET ASIC_STATE:SAI_OBJECT_TYPE_VLAN:oid:0x26000000000100 source PortsOrch
```

然後 Python 端拿到：

```text
key = "oid:0x26000000000100"
op = "SET"
field_values = [
  ("SAI_VLAN_ATTR_VLAN_ID", "100"),
  ("source", "PortsOrch"),
]
```

本專案的 tiny syncd 只會印出這些欄位並輸出 `pretend write ASIC`，用來代表真正 SONiC
`syncd` 接下來會把 SAI object operation 套到 ASIC。
