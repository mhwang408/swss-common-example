# 用 sonic-swss-common 讀寫 SONiC Redis Tables

這份文件說明小型 SONiC-style app 如何透過 `sonic-swss-common` 定義、寫入與觀察
Redis-backed SONiC DB tables。

讀者目標是選對 table API、理解它產生的 Redis key，並能跑完本 repo 的
`custom_tables` 與 `vlan_table` 範例。本文範圍限於本機教學專案；不涵蓋完整
SONiC schema upstream、CLI 整合或真實 ASIC programming。

如果要看 CONFIG_DB、APPL_DB、ASIC_DB、STATE_DB、FLEX_COUNTER_DB、
COUNTERS_DB 之間更完整的 producer/consumer table split，請看
[sonic-swss-common-table-usage-in-SONiC.md](sonic-swss-common-table-usage-in-SONiC.md)。

## 系統模型

`sonic-swss-common` 是 SONiC 用來讀寫 Redis-backed SONiC DB 的 shared client
library。和 raw Redis client（例如 `redis-py`）相比，它理解 SONiC DB name、
table separator、producer/consumer queue、pending-state key set，以及
`Select` event loop。raw Redis client 很適合 inspection 和 support tooling；
但如果要模擬或實作 production-like SONiC component，應優先使用
`sonic-swss-common`，避免自己手刻 table semantics。

### 核心模型

SONiC Redis table 本質上不是 Redis 需要預先宣告的 schema，而是「key 命名慣例」加上「producer / consumer 合約」。

定義一個 custom table 時，至少要明確定義：

- 使用哪個 DB，例如 `CONFIG_DB` 或 `APPL_DB`。
- table 名稱，例如 `CUSTOM_CONFIG_TABLE`。
- key separator，來自 `database_config.json`，例如 `CONFIG_DB` 用 `|`，`APPL_DB` 用 `:`。
- 欄位名稱與值的語意。
- 誰擁有寫入權，也就是 table owner / single producer。
- 事件語意：直接讀寫 hash、訂閱 config change、有序 operation queue、或 latest-state coalescing。

本專案的簡化流程是：

```text
CONFIG_DB CUSTOM_CONFIG_TABLE|demo
    -> config_to_appl_bridge.py
APPL_DB _CUSTOM_APPL_TABLE:demo
APPL_DB CUSTOM_APPL_TABLE_KEY_SET
```

`_CUSTOM_APPL_TABLE:demo` 和 `CUSTOM_APPL_TABLE_KEY_SET` 是 `ProducerStateTable` 產生的 pending state。真正的 APPL table owner 會用 `ConsumerStateTable` 消費 pending state，並 materialize 最終的 `CUSTOM_APPL_TABLE:demo`。

### Table 名稱定義在哪裡

custom table 不需要改 `sonic-swss-common` submodule。

建議把 project-local table 名稱放在專案自己的檔案，例如：

- C/C++ 使用 project-local header。
- Python 使用一個 mirror module。

只有 upstream SONiC 已經存在的 table 名稱才使用 `sonic-swss-common/common/schema.h`。

範例 header：

```c
#include "sonic-swss-common/common/schema.h"

#define EXAMPLE_CFG_CUSTOM_CONFIG_TABLE_NAME "CUSTOM_CONFIG_TABLE"
#define EXAMPLE_APP_CUSTOM_APPL_TABLE_NAME   "CUSTOM_APPL_TABLE"
```

Python 可以 mirror 相同常數：

```python
EXAMPLE_CFG_CUSTOM_CONFIG_TABLE_NAME = "CUSTOM_CONFIG_TABLE"
EXAMPLE_APP_CUSTOM_APPL_TABLE_NAME = "CUSTOM_APPL_TABLE"
```

Redis 不會檢查這些 table 名稱是否存在於 `schema.h`。這些常數的價值是讓 producer / consumer 使用同一份命名合約。

### 是否需要 YANG 或 gen_cfg_schema.py

最小 custom Redis table example 不需要。

YANG model 和 `sonic-swss-common/gen_cfg_schema.py` 適合用在更完整的 SONiC platform integration，例如：

- 需要 CONFIG_DB schema validation。
- 需要和 SONiC CLI / config tooling 整合。
- 要讓 table 成為 upstream-style 的正式 SONiC config schema。
- 要產生 upstream schema header 常數。

對 standalone app 或 proof of concept：

- 用 project-local header / Python module 定義 table 名稱。
- 文件化欄位與 ownership contract。
- 用 `swsscommon` API 讀寫 Redis。
- 不修改 `sonic-swss-common` submodule。

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
`runner` 的 `entrypoint.sh` 會在啟動時把專案根目錄的
`database_config.json` copy 到
`/var/run/redis/sonic-db/database_config.json`，避免在 `/var/run/redis`
底下做 nested bind mount。

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

## Table API Families

選 API 時，核心問題是：你需要直接 hash access、CONFIG_DB change subscription、有序 operation stream，還是 latest-state coalescing。

### Table

`Table` 是直接 Redis hash access，並套用 SONiC table naming rule。

適合用在：

- 寫 desired config 到 `CONFIG_DB`。
- 從任意 DB 讀目前狀態。
- 列出 keys 或直接操作 table，不需要 message queue semantics。

範例：

```python
config_db = swsscommon.DBConnector("CONFIG_DB", 0, False)
table = swsscommon.Table(config_db, "CUSTOM_CONFIG_TABLE")

table.set("demo", swsscommon.FieldValuePairs([
    ("enabled", "true"),
    ("interval", "10"),
]))
```

因為 `CONFIG_DB` separator 是 `|`，實際 Redis key 是：

```text
DB 4 hash: CUSTOM_CONFIG_TABLE|demo
```

`Table` 不會產生 producer / consumer message queue。若其他程式要反應 `CONFIG_DB` 變化，應該用 `SubscriberStateTable` 訂閱。

### SubscriberStateTable

`SubscriberStateTable` 用來 watch table update，最常用在 `CONFIG_DB`。它適合 daemon 讀取 config change，然後計算 derived state。

適合用在：

- app 需要 startup state 加 live config updates。
- source table 是由 `Table`、config reload、CLI 或其他 config owner 直接寫入。
- Redis keyspace notification 已啟用。

基本 loop：

```python
config_db = swsscommon.DBConnector("CONFIG_DB", 0, False)
subscriber = swsscommon.SubscriberStateTable(config_db, "CUSTOM_CONFIG_TABLE")

selector = swsscommon.Select()
selector.addSelectable(subscriber)

while True:
    state, selectable = selector.select()
    if state != swsscommon.Select.OBJECT:
        continue

    key, op, field_values = subscriber.pop()
```

local Redis 必須啟用 keyspace notifications：

```text
--notify-keyspace-events KEA
```

`SubscriberStateTable` 只是觀察變化，不是 ownership boundary。single writer / table owner 原則仍然要靠系統設計保證。

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

本專案的 VLAN ASIC_DB 範例中，`portorch.py` 用 `ProducerTable` 把 ASIC_DB
operation 放進 queue：

```text
LPUSH ASIC_STATE:SAI_OBJECT_TYPE_VLAN_KEY_VALUE_OP_QUEUE key value op
```

這一步不會 materialize 最終 ASIC_DB hash。最終 hash 是 `syncd.py` 呼叫
`ConsumerTable.pop()` 時，由 Redis Lua 寫入：

```text
LRANGE ASIC_STATE:SAI_OBJECT_TYPE_VLAN_KEY_VALUE_OP_QUEUE
LTRIM ASIC_STATE:SAI_OBJECT_TYPE_VLAN_KEY_VALUE_OP_QUEUE
HSET ASIC_STATE:SAI_OBJECT_TYPE_VLAN:oid:0x26000000000100 SAI_VLAN_ATTR_VLAN_ID 100
```

tiny `syncd.py` 的程式流程是：

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

以 VLAN create 為例，`pop()` 回傳：

```text
key = "oid:0x26000000000100"
op = "SET"
field_values = [
  ("SAI_VLAN_ATTR_VLAN_ID", "100"),
  ("source", "PortsOrch"),
]
```

`ConsumerTable.pop()` 就是 queued operation 變成 final ASIC_DB hash 的時間點。
本專案的 `syncd.py` 接著只會印出 tuple，並記錄「假裝寫 ASIC」；它不會真的操作 ASIC。

它比較像 operation log，不是 state snapshot。若 consumer 必須看到 `SET A`、`DEL A`、`SET A` 三個獨立事件，就用這組。

如果 consumer 只需要某個 key 的最終 desired state，就不要用這組，通常 `ProducerStateTable` 更簡單也更適合。

### ProducerStateTable 與 ConsumerStateTable

`ProducerStateTable` / `ConsumerStateTable` 是 latest desired state 模型。producer 寫 pending state，consumer 負責 materialize 或 apply state。

適合用在：

- producer 發布 APPL_DB desired state。
- consumer 不需要每個 intermediate update。
- 同一個 key 的重複 update 可以 coalesce。
- 不要求不同 keys 之間的處理順序。

概念上的 Redis 結構：

```text
producer:
  HSET _<TABLE>:<key> field value
  SADD <TABLE>_KEY_SET key
  publish <TABLE>_CHANNEL

consumer:
  SPOP <TABLE>_KEY_SET
  HGETALL _<TABLE>:<key>
  apply or materialize latest state
  DEL _<TABLE>:<key>
```

VLAN 範例用 Redis `MONITOR` 驗證了實際時間點：`vlanmgrd.py`
`ProducerStateTable.set()` 只寫 APPL_DB pending state：

```text
SADD VLAN_TABLE_KEY_SET Vlan100
HSET _VLAN_TABLE:Vlan100 vlanid 100
```

最終 APPL_DB hash 是 `portorch.py` 的 `ConsumerStateTable.pop()` materialize：

```text
SPOP VLAN_TABLE_KEY_SET
HGETALL _VLAN_TABLE:Vlan100
HSET VLAN_TABLE:Vlan100 vlanid 100
DEL _VLAN_TABLE:Vlan100
```

本專案例子：

```text
DB 0 hash: _CUSTOM_APPL_TABLE:demo
DB 0 set:  CUSTOM_APPL_TABLE_KEY_SET
```

coalescing 規則：

- 相同 key、相同 field：最後 pending value wins。
- 相同 key、不同 fields：合併成同一份 latest snapshot。
- 不同 keys：處理順序不保證。

所以 `ProducerStateTable` 很適合 `CONFIG_DB -> app -> APPL_DB` 的 desired state publication。consumer 通常只需要最新 APPL_DB desired state，不需要每一次 config edit 的 intermediate event。

## API 選擇與 Ownership

```text
需要直接 hash read/write:
  Table

需要觀察 CONFIG_DB changes:
  SubscriberStateTable

需要每個 operation 且要 in-order:
  ProducerTable + ConsumerTable

需要 latest desired state，允許 coalescing:
  ProducerStateTable + ConsumerStateTable
```

常見 custom SONiC pipeline：

```text
CONFIG_DB writer:
  Table

CONFIG_DB app reader:
  SubscriberStateTable

APPL_DB app writer:
  ProducerStateTable

APPL_DB table owner:
  ConsumerStateTable
```

### Atomicity 與 Lock

producer / consumer table APIs 內部會使用 Redis Lua，讓它們負責的 multi-command Redis operation 在 Redis server 裡 atomic 執行。

這能避免：

- queue / pending state 寫到一半。
- message publish 和資料更新之間 race。
- consumer 看到 partial table operation。

但這不等於整個 app workflow 都被 mutual exclusive 保護。

這種是 table operation atomic：

```text
ProducerStateTable.set("demo", fields)
```

這種不是自動整體 atomic：

```text
read CONFIG_DB table A
read CONFIG_DB table B
compute using program cache
write APPL_DB table X
write APPL_DB table Y
```

如果 workflow 需要全域互斥，所有參與者都必須遵守同一套設計。優先考慮：

- single writer / table owner。
- 不同 app 不共享 input/output tables。
- version field 或 generation ID。
- apply / commit marker。
- idempotent convergence from CONFIG_DB to APPL_DB。

只有真的存在 shared resource，而且所有 writer / reader 都會遵守時，才加 explicit Redis lock。

### Ownership Pattern

好的切分：

```text
CONFIG_DB: table A1 -> app 1 -> APPL_DB: table B1
CONFIG_DB: table A2 -> app 2 -> APPL_DB: table B2
```

如果 app 1 和 app 2 沒有共享 input tables、output tables、keys 或外部 side effects，通常不需要 workflow-level mutex。

要避免：

```text
CONFIG_DB: same table/key range -> multiple apps -> same APPL_DB table
```

除非有明確 owner、allocator、version check 或 explicit lock。

## Route Flow Counter Table Split

route flow counter 同時用到多個 DB，因此很適合用來避免把所有流程都誤解成
`ProducerTable` / `ConsumerTable`。config 是 direct `CONFIG_DB` hash；
route pattern 是 CONFIG_DB subscription；traditional polling setup 使用
`FLEX_COUNTER_DB`；CLI/display 資料則從 `COUNTERS_DB` 讀取。

| Flow | Producer / Table | DB / Table | Consumer / Table |
| --- | --- | --- | --- |
| Enable route flow counter | CLI / `Table` | `CONFIG_DB:FLEX_COUNTER_TABLE|FLOW_CNT_ROUTE` | `FlexCounterOrch` / orch `Consumer` backed by `SubscriberStateTable` |
| Route pattern config | CLI / `Table` | `CONFIG_DB:FLOW_COUNTER_ROUTE_PATTERN_TABLE|<vrf>|<prefix>` | `FlowCounterRouteOrch` / orch `Consumer` backed by `SubscriberStateTable` |
| Polling setup, traditional mode | `FlowCounterRouteOrch` via `FlexCounterManager` / `ProducerTable` | `FLEX_COUNTER_DB:FLEX_COUNTER_TABLE:ROUTE_FLOW_COUNTER:<counter_oid>` field `FLOW_COUNTER_ID_LIST` | `syncd` flex counter logic |
| Route-to-counter mapping | `FlowCounterRouteOrch` / `Table` | `COUNTERS_DB:COUNTERS_ROUTE_NAME_MAP` | CLI/display / `Table` or direct Redis hash read |
| Route-to-pattern mapping | `FlowCounterRouteOrch` / `Table` | `COUNTERS_DB:COUNTERS_ROUTE_TO_PATTERN_MAP` | CLI/display / `Table` or direct Redis hash read |
| Polled counter stats | `syncd` flex counter polling / `Table`-style hash write | `COUNTERS_DB:COUNTERS:<counter_oid>` | CLI/display / `Table` or direct Redis hash read |

non-traditional flex counter mode 中，`FlexCounterManager` 可以透過 sairedis
switch attributes，例如 `SAI_REDIS_SWITCH_ATTR_FLEX_COUNTER`，設定 syncd。
這條路徑不是直接用 swsscommon `ProducerTable` 寫 `FLEX_COUNTER_DB`。

## Example Flows

### custom_tables

`custom_tables` 是最小的 `CONFIG_DB -> APPL_DB` flow。它適合用來理解 custom
table naming、config observation，以及沒有 APPL table owner 時的 APPL_DB pending
state。

`custom_tables` 是最小的 `CONFIG_DB -> APPL_DB` desired-state flow：

```text
config_db_producer.py
  -> CONFIG_DB CUSTOM_CONFIG_TABLE|demo
  -> config_to_appl_bridge.py
  -> APPL_DB pending _CUSTOM_APPL_TABLE:demo
```

bridge 使用 `ProducerStateTable`，所以它只會寫 pending APPL state 與 key set。
這個例子刻意沒有 APPL table owner，因此 final
`CUSTOM_APPL_TABLE:demo` hash 不會被 materialize。

實作把命名合約保留在本 repo：

- `src/custom_tables/example_schema.py` 定義 Python table constants。
- `src/custom_tables/example_schema.h` 定義 C/C++ table constants。
- `src/custom_tables/config_db_producer.py` 用 `Table` 寫 CONFIG_DB。
- `src/custom_tables/config_to_appl_bridge.py` 用 `SubscriberStateTable`
  watch CONFIG_DB，並用 `ProducerStateTable` 發布 APPL pending state。

### vlan_table

`vlan_table` 補上 APPL_DB 與 ASIC_DB 的 consumer，讓同一組 table API 可以用完整
`CONFIG_DB -> APPL_DB -> ASIC_DB` chain 觀察。

`vlan_table` 模擬完整 SONiC-style VLAN chain：

```text
config vlan add 100
  -> CONFIG_DB VLAN|Vlan100
  -> vlanmgrd
  -> APPL_DB pending _VLAN_TABLE:Vlan100
  -> PortsOrch
  -> ASIC_DB queue ASIC_STATE:SAI_OBJECT_TYPE_VLAN_KEY_VALUE_OP_QUEUE
  -> syncd
  -> ASIC_DB final ASIC_STATE:SAI_OBJECT_TYPE_VLAN:oid:0x26000000000100
  -> ASIC_DB SAI_RESPONSE channel
  -> PortsOrch
```

它也包含 syncd 反方向送出的 async notification path。真實 SONiC 中，port
oper status 這類事件不是 vlanmgrd 直接從 syncd 收到，而是：

```text
syncd
  -> ASIC_DB NOTIFICATIONS channel
  -> PortsOrch
  -> STATE_DB PORT_TABLE|Ethernet0
  -> vlanmgrd / other manager daemons
```

實作把每個 SONiC component role 對應到小型 Python script：

- `src/vlan_table/config_vlan_command.py` 模擬 `config vlan add/del`。
- `src/vlan_table/vlanmgrd.py` 把 `CONFIG_DB VLAN` 轉成 APPL pending state，也可觀察 `STATE_DB PORT_TABLE`。
- `src/vlan_table/portorch.py` 模擬真實 `PortsOrch`：consume APPL pending state、enqueue ASIC operation、讀 SAI response、處理 async ASIC notification。
- `src/vlan_table/syncd.py` consume ASIC operation、送出 fake SAI response，也可送出 async port notification。

## Running The Examples

### Test environment

本機測試環境使用：

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

啟動 bridge：

```bash
cd /home/ubuntu/swss-common-example
docker compose up -d database
UID=$(id -u) GID=$(id -g) docker compose run --rm -T runner \
  src/custom_tables/config_to_appl_bridge.py --key demo --watch
```

另一個 terminal 寫入 CONFIG_DB：

```bash
cd /home/ubuntu/swss-common-example
UID=$(id -u) GID=$(id -g) docker compose run --rm -T runner \
  src/custom_tables/config_db_producer.py --key demo --enabled true --interval 10
```

VLAN flow 可在三個 terminal 啟動 watch components，再送 config command：

```bash
cd /home/ubuntu/swss-common-example
UID=$(id -u) GID=$(id -g) docker compose run --rm -T runner \
  src/vlan_table/syncd.py --vlan-id 100 --watch
```

```bash
cd /home/ubuntu/swss-common-example
UID=$(id -u) GID=$(id -g) docker compose run --rm -T runner \
  src/vlan_table/portorch.py --vlan-id 100 --watch
```

```bash
cd /home/ubuntu/swss-common-example
UID=$(id -u) GID=$(id -g) docker compose run --rm -T runner \
  src/vlan_table/vlanmgrd.py --vlan-id 100 --watch
```

```bash
cd /home/ubuntu/swss-common-example
UID=$(id -u) GID=$(id -g) docker compose run --rm -T runner \
  src/vlan_table/config_vlan_command.py add 100
```

### Method 2: Helper scripts

長時間 watch commands 建議用 helper scripts。它們會啟動 `database`、export
`UID`/`GID`、把參數傳給 component，並在 `Ctrl-C` 時移除 runner container。

Custom table flow：

```bash
cd /home/ubuntu/swss-common-example
scripts/run_custom_tables_example.sh bridge --key demo --watch
```

```bash
cd /home/ubuntu/swss-common-example
scripts/run_custom_tables_example.sh producer --key demo --enabled true --interval 10
```

VLAN flow：

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

Full VLAN verification：

```bash
cd /home/ubuntu/swss-common-example
scripts/run_vlan_table_example.sh verify 100
```

## Verification

Custom table checks：

```bash
docker exec database redis-cli -s /var/run/redis/redis.sock -n 4 HGETALL 'CUSTOM_CONFIG_TABLE|demo'
docker exec database redis-cli -s /var/run/redis/redis.sock -n 0 HGETALL '_CUSTOM_APPL_TABLE:demo'
docker exec database redis-cli -s /var/run/redis/redis.sock -n 0 SMEMBERS 'CUSTOM_APPL_TABLE_KEY_SET'
```

VLAN checks：

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

`Table` 會直接寫 final hash。`ProducerStateTable` 與 `ProducerTable` 不會；
它們要由 matching consumer materialize final hash：

```text
ProducerStateTable.set:
  writes _<TABLE>:<key> and <TABLE>_KEY_SET

ConsumerStateTable.pop:
  materializes <TABLE>:<key>

ProducerTable.set:
  writes <TABLE>_KEY_VALUE_OP_QUEUE

ConsumerTable.pop:
  materializes <TABLE>:<key>
```
