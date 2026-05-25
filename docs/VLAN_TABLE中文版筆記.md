# 筆記：用 sonic-swss-common 追蹤 VLAN_TABLE 流程

這份筆記用 VLAN_TABLE 當例子，整理 `config vlan`、`vlanmgrd`、`VlanOrch` 之間如何透過 SONiC Redis DB 和 `sonic-swss-common` table API 串起來。

## 核心模型

SONiC Redis table 本質上不是 Redis 需要預先宣告的 schema，而是「key 命名慣例」加上「producer / consumer 合約」。

以 VLAN 建立流程為例，至少要明確知道：

- 使用哪個 DB：`CONFIG_DB` 是 DB 4，`APPL_DB` 是 DB 0。
- table 名稱：`CONFIG_DB` 使用 `VLAN`，`APPL_DB` 使用 `VLAN_TABLE`。
- key separator：`CONFIG_DB` 用 `|`，`APPL_DB` 用 `:`。
- 欄位語意：`vlanid` 表示 VLAN ID。
- 寫入權：CLI/config tooling 寫 `CONFIG_DB`，`vlanmgrd` 寫 `APPL_DB` pending state，`VlanOrch` 消費 `APPL_DB` update。
- 事件語意：`CONFIG_DB` 用 direct hash + subscription；`APPL_DB` 用 latest-state pending update。

簡化流程是：

```text
config vlan add 100
    -> CONFIG_DB VLAN|Vlan100
    -> vlanmgrd SubscriberStateTable
    -> APPL_DB _VLAN_TABLE:Vlan100
    -> APPL_DB VLAN_TABLE_KEY_SET
    -> VlanOrch ConsumerStateTable
    -> APPL_DB VLAN_TABLE:Vlan100
```

`_VLAN_TABLE:Vlan100` 和 `VLAN_TABLE_KEY_SET` 是 `ProducerStateTable` 產生的 pending state。`VlanOrch` 會用 `ConsumerStateTable` 消費 pending state，並 materialize 或 apply 最終的 `VLAN_TABLE:Vlan100`。

## Table 名稱定義在哪裡

VLAN 是 SONiC upstream 已存在的 table，不是 project-local custom table。正式 SONiC 程式通常會使用 upstream schema 常數，例如：

```c
#include "sonic-swss-common/common/schema.h"

#define APP_VLAN_TABLE_NAME "VLAN_TABLE"
```

`CONFIG_DB` 的 VLAN table 名稱是 `VLAN`。在本教學專案中，Python 範例把需要的常數集中在：

```python
CFG_VLAN_TABLE_NAME = "VLAN"
APP_VLAN_TABLE_NAME = "VLAN_TABLE"
VLAN_PREFIX = "Vlan"
```

Redis 不會檢查這些 table 名稱是否存在於 `schema.h`。常數的價值是讓 CLI 模擬程式、`vlanmgrd`、`vlanorch` 使用同一份命名合約。

## 是否需要 YANG 或 gen_cfg_schema.py

這個 VLAN_TABLE 教學例子不需要新增 YANG model 或跑 `gen_cfg_schema.py`。

原因是：

- `VLAN` / `VLAN_TABLE` 已經是 SONiC 既有概念。
- 範例目標是示範 Redis table API 與資料流，不是新增一個正式 SONiC schema。
- 我們只需要寫入 `CONFIG_DB VLAN|Vlan100`，再觀察 `APPL_DB VLAN_TABLE` update。

若要把新的 config table 變成正式 SONiC platform feature，才需要考慮：

- CONFIG_DB schema validation。
- SONiC CLI / config tooling 整合。
- YANG model。
- upstream-style generated schema header。

## database_config.json

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

本專案的 `database` container 會把專案根目錄的 `database_config.json` 掛到這個路徑，並提供 `/var/run/redis/redis.sock`。

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

## 四種 Table API

選 API 時，核心問題是：你需要直接 hash access、CONFIG_DB change subscription、有序 operation stream，還是 latest-state coalescing。

## 1. Table

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

## 2. SubscriberStateTable

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

## 3. ProducerTable / ConsumerTable

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

VLAN_TABLE 的常見 desired-state 流程通常不需要這組，因為 `VlanOrch` 多半只需要某個 VLAN key 的最新 desired state，而不是每個 intermediate edit。

## 4. ProducerStateTable / ConsumerStateTable

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

`VlanOrch` 則用 `ConsumerStateTable` 消費：

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

所以 `ProducerStateTable` 很適合 `CONFIG_DB VLAN -> vlanmgrd -> APPL_DB VLAN_TABLE` 的 desired state publication。`VlanOrch` 通常只需要最新 APPL_DB desired state，不需要每一次 config edit 的 intermediate event。

## 選擇總結

```text
config vlan add/del:
  Table

vlanmgrd 觀察 CONFIG_DB VLAN:
  SubscriberStateTable

vlanmgrd 發布 APPL_DB VLAN_TABLE:
  ProducerStateTable

VlanOrch 消費 APPL_DB VLAN_TABLE:
  ConsumerStateTable
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
  VlanOrch ConsumerStateTable("VLAN_TABLE")
```

## Atomicity 與 Lock

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

## Ownership Pattern

VLAN_TABLE 的 ownership 可以理解成：

```text
CONFIG_DB VLAN:
  owner = config CLI / config reload / config management

CONFIG_DB VLAN consumer:
  owner = vlanmgrd

APPL_DB VLAN_TABLE pending state:
  producer = vlanmgrd

APPL_DB VLAN_TABLE consumer:
  owner = VlanOrch in orchagent
```

要避免：

```text
multiple daemons -> same APPL_DB VLAN_TABLE key range
```

除非有明確 owner、allocator、version check 或 explicit lock。

## 本專案 VLAN_TABLE 最小流程

啟動 tiny VlanOrch：

```bash
cd /home/ubuntu/swss-common-example
docker compose run --rm \
  --entrypoint python3 \
  runner \
  src/vlan_table/vlanorch.py \
  --vlan-id 100 \
  --watch
```

啟動 tiny vlanmgrd：

```bash
cd /home/ubuntu/swss-common-example
docker compose run --rm \
  --entrypoint python3 \
  runner \
  src/vlan_table/vlanmgrd.py \
  --vlan-id 100 \
  --watch
```

模擬 `config vlan add 100`：

```bash
cd /home/ubuntu/swss-common-example
docker compose run --rm \
  --entrypoint python3 \
  runner \
  src/vlan_table/config_vlan_command.py \
  add 100
```

模擬 `config vlan del 100`：

```bash
cd /home/ubuntu/swss-common-example
docker compose run --rm \
  --entrypoint python3 \
  runner \
  src/vlan_table/config_vlan_command.py \
  del 100
```

檢查 Redis：

```bash
docker exec database redis-cli -s /var/run/redis/redis.sock -n 4 HGETALL 'VLAN|Vlan100'
docker exec database redis-cli -s /var/run/redis/redis.sock -n 0 HGETALL '_VLAN_TABLE:Vlan100'
docker exec database redis-cli -s /var/run/redis/redis.sock -n 0 SMEMBERS 'VLAN_TABLE_KEY_SET'
docker exec database redis-cli -s /var/run/redis/redis.sock -n 0 HGETALL 'VLAN_TABLE:Vlan100'
```
