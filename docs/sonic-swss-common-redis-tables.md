# SONiC Redis Table Access With sonic-swss-common

This document explains how a small SONiC-style application defines, writes, and
observes Redis-backed SONiC DB tables through `sonic-swss-common`.

The reader goal is to choose the right table API, understand the Redis keys it
creates, and run the two examples in this repository. The scope is the local
`custom_tables` and `vlan_table` examples; it does not cover full SONiC schema
upstreaming, CLI integration, or real ASIC programming.

For the broader SONiC producer/consumer table split across CONFIG_DB, APPL_DB,
ASIC_DB, STATE_DB, FLEX_COUNTER_DB, and COUNTERS_DB, see
[sonic-swss-common-table-usage-in-SONiC.md](sonic-swss-common-table-usage-in-SONiC.md).

## System Model

`sonic-swss-common` is SONiC's shared client library for Redis-backed SONiC
DBs. Compared with raw Redis clients such as `redis-py`, it understands
SONiC DB names, table separators, producer/consumer queues, pending-state key
sets, and `Select`-based event loops. Raw Redis clients are still useful for
inspection and support tooling, but production-like component behavior should
use `sonic-swss-common` so the table semantics match SONiC.

### Mental Model

SONiC DB tables are Redis key naming conventions plus producer/consumer
contracts. Redis itself does not require a table to be declared before use.

For a custom table, the minimum contract is:

- DB name and DB index, for example `CONFIG_DB` is DB 4 and `APPL_DB` is DB 0.
- Table name, for example `CUSTOM_CONFIG_TABLE`.
- Key separator from `database_config.json`, for example `|` for `CONFIG_DB`
  and `:` for `APPL_DB`.
- Field names and values stored in the Redis hash.
- Ownership: which program is allowed to write this table.
- Event semantics: direct hash access, ordered queue, or latest-state update.

In this project:

```text
CONFIG_DB CUSTOM_CONFIG_TABLE|demo
    -> config_to_appl_bridge.py
APPL_DB _CUSTOM_APPL_TABLE:demo
APPL_DB CUSTOM_APPL_TABLE_KEY_SET
```

The underscore-prefixed APPL_DB hash and key set are produced by
`ProducerStateTable`. A real APPL table owner would consume them with
`ConsumerStateTable` and materialize the final `CUSTOM_APPL_TABLE:demo` hash.

### Where To Define A Table

For a project-local custom table, do not modify the `sonic-swss-common`
submodule. Keep the table names in project-owned files, for example a local
schema header for C/C++ and a small Python module that mirrors the same names.

Use upstream `sonic-swss-common/common/schema.h` only for upstream SONiC table
names that already exist in SONiC.

A project-local header can include upstream schema constants:

```c
#include "sonic-swss-common/common/schema.h"

#define EXAMPLE_CFG_CUSTOM_CONFIG_TABLE_NAME "CUSTOM_CONFIG_TABLE"
#define EXAMPLE_APP_CUSTOM_APPL_TABLE_NAME   "CUSTOM_APPL_TABLE"
```

Python can mirror those constants:

```python
EXAMPLE_CFG_CUSTOM_CONFIG_TABLE_NAME = "CUSTOM_CONFIG_TABLE"
EXAMPLE_APP_CUSTOM_APPL_TABLE_NAME = "CUSTOM_APPL_TABLE"
```

That is enough because Redis does not validate table names against
`schema.h`.

### Do You Need YANG Or gen_cfg_schema.py?

Not for a minimal custom Redis table example.

YANG models and `sonic-swss-common/gen_cfg_schema.py` are useful when you want
to integrate with SONiC's config schema validation and generated upstream
schema headers. That is a stronger SONiC platform integration path.

For a standalone custom app or proof of concept:

- Define local table names in project code or a project-local header.
- Document the fields and ownership contract.
- Use `swsscommon` APIs to read/write Redis.
- Avoid editing the `sonic-swss-common` submodule.

Use YANG/schema generation later if the table must become a first-class SONiC
configuration schema with validation, CLI/config tooling integration, and
upstream-style generated constants.

### database_config.json

`database_config.json` tells `swsscommon` how to map logical DB names to Redis
instances, DB indexes, separators, and socket paths.

Example:

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

In a real SONiC system, this normally exists at:

```text
/var/run/redis/sonic-db/database_config.json
```

In this project, `/var/run/redis` is shared between the `database` and
`runner` containers. The runner entrypoint copies the project
`database_config.json` to `/var/run/redis/sonic-db/database_config.json` before
running the example, avoiding a nested bind mount under `/var/run/redis`.
The Python examples can also explicitly load the project config:

```python
swsscommon.SonicDBConfig.load_sonic_db_config("/tmp/database_config.json")
```

Then connect by logical DB name:

```python
config_db = swsscommon.DBConnector("CONFIG_DB", 0, False)
appl_db = swsscommon.DBConnector("APPL_DB", 0, False)
```

The third argument is `False` here because this example uses the SONiC Unix
socket, not TCP.

## Table API Families

These are the four table patterns discussed in this project. The important
choice is whether the app needs direct hash access, config-change subscription,
an ordered operation stream, or latest-state coalescing.

### Table

`Table` is direct Redis hash access with SONiC table naming rules.

Use it when:

- Writing desired config into `CONFIG_DB`.
- Reading current data from any DB.
- Enumerating keys or manipulating a table without message queue semantics.

It writes the actual DB table hash directly:

```python
config_db = swsscommon.DBConnector("CONFIG_DB", 0, False)
table = swsscommon.Table(config_db, "CUSTOM_CONFIG_TABLE")

table.set("demo", swsscommon.FieldValuePairs([
    ("enabled", "true"),
    ("interval", "10"),
]))
```

With the `CONFIG_DB` separator `|`, the Redis key is:

```text
DB 4 hash: CUSTOM_CONFIG_TABLE|demo
```

`Table` does not publish a producer/consumer message queue. If another program
needs to react to `CONFIG_DB` changes, it should subscribe with
`SubscriberStateTable`.

### SubscriberStateTable

`SubscriberStateTable` watches table updates, most commonly in `CONFIG_DB`.
It is the right API for an app that reacts to config changes and then computes
derived state.

Use it when:

- A daemon needs startup state plus live config changes.
- The source table is directly written by `Table`, config reload, CLI, or
  another config owner.
- Redis keyspace notifications are enabled.

Basic loop:

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

Local Redis must enable notifications:

```text
--notify-keyspace-events KEA
```

`SubscriberStateTable` is not an ownership boundary by itself. It observes table
changes; the single-writer/table-owner rule still has to come from the system
design.

### ProducerTable And ConsumerTable

`ProducerTable` and `ConsumerTable` implement an ordered operation stream. The
producer sends operations; the consumer pops and applies them in order.

Use them when:

- Every operation matters.
- Per-table operation order matters.
- Replacing intermediate operations with only the latest state would be wrong.

Conceptual Redis shape:

```text
producer:
  push key/op/field-values into <TABLE>_KEY_VALUE_OP_QUEUE
  publish <TABLE>_CHANNEL

consumer:
  pop the list in order
  decode key/op/field-values
  apply each operation
```

For the VLAN ASIC_DB example in this repo, `portorch.py` uses `ProducerTable`
to enqueue an ASIC_DB operation:

```text
LPUSH ASIC_STATE:SAI_OBJECT_TYPE_VLAN_KEY_VALUE_OP_QUEUE key value op
```

That does not materialize the final ASIC_DB hash. The final hash appears only
when `syncd.py` calls `ConsumerTable.pop()`, whose Redis Lua performs:

```text
LRANGE ASIC_STATE:SAI_OBJECT_TYPE_VLAN_KEY_VALUE_OP_QUEUE
LTRIM ASIC_STATE:SAI_OBJECT_TYPE_VLAN_KEY_VALUE_OP_QUEUE
HSET ASIC_STATE:SAI_OBJECT_TYPE_VLAN:oid:0x26000000000100 SAI_VLAN_ATTR_VLAN_ID 100
```

The tiny `syncd.py` code path is:

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

For the VLAN create case, `pop()` returns:

```text
key = "oid:0x26000000000100"
op = "SET"
field_values = [
  ("SAI_VLAN_ATTR_VLAN_ID", "100"),
  ("source", "PortsOrch"),
]
```

`ConsumerTable.pop()` is the point where the queued operation becomes a final
ASIC_DB hash. In the example, `syncd.py` then prints the tuple and logs a fake
ASIC write; it does not touch a real ASIC.

This is closer to an operation log than a state snapshot. It is useful when the
consumer must see `SET A`, then `DEL A`, then `SET A` as three separate events.

Do not use it when the final state per key is enough. In that case,
`ProducerStateTable` is usually simpler and cheaper.

### ProducerStateTable And ConsumerStateTable

`ProducerStateTable` and `ConsumerStateTable` implement latest desired state.
The producer writes pending state; the consumer owns materialization or applying
that state.

Use them when:

- A producer publishes desired APPL_DB state.
- Consumers do not need every intermediate update.
- Coalescing repeated updates to the same key is acceptable.
- Ordering across different keys is not required.

Conceptual Redis shape:

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

The VLAN example verifies this with Redis `MONITOR`: `vlanmgrd.py`
`ProducerStateTable.set()` writes only pending APPL_DB state:

```text
SADD VLAN_TABLE_KEY_SET Vlan100
HSET _VLAN_TABLE:Vlan100 vlanid 100
```

The final APPL_DB hash is materialized later by `portorch.py`
`ConsumerStateTable.pop()`:

```text
SPOP VLAN_TABLE_KEY_SET
HGETALL _VLAN_TABLE:Vlan100
HSET VLAN_TABLE:Vlan100 vlanid 100
DEL _VLAN_TABLE:Vlan100
```

For this project:

```text
DB 0 hash: _CUSTOM_APPL_TABLE:demo
DB 0 set:  CUSTOM_APPL_TABLE_KEY_SET
```

Coalescing rules:

- Same key and same field: the last pending value wins.
- Same key and different fields: pending fields merge into one latest snapshot.
- Different keys: processing order is not guaranteed.

This is why `ProducerStateTable` fits `CONFIG_DB -> app -> APPL_DB` desired
state publication: the consumer usually wants the latest desired APPL_DB state,
not every intermediate config edit.

## API Selection And Ownership

```text
Need direct hash read/write:
  Table

Need to observe CONFIG_DB changes:
  SubscriberStateTable

Need every operation, in order:
  ProducerTable + ConsumerTable

Need latest desired state, coalescing is OK:
  ProducerStateTable + ConsumerStateTable
```

The common custom SONiC pipeline is:

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

### Atomicity And Locks

The producer/consumer table APIs use Redis Lua internally for the multi-command
Redis operations they own. This prevents partial queue/state updates and lost
wakeup races inside one table operation.

That does not make an entire app workflow mutually exclusive.

This is atomic as a table operation:

```text
ProducerStateTable.set("demo", fields)
```

This is not automatically atomic as a whole workflow:

```text
read CONFIG_DB table A
read CONFIG_DB table B
compute using program cache
write APPL_DB table X
write APPL_DB table Y
```

If the workflow must be globally exclusive, all participants must respect the
same design. Prefer these before adding a broad lock:

- Single writer/table owner.
- No overlapping tables between independent apps.
- Version fields or generation IDs.
- Apply/commit markers.
- Idempotent convergence from CONFIG_DB to APPL_DB.

Add an explicit Redis lock only when there is a real shared resource and every
writer/reader that matters will obey the lock.

### Ownership Pattern

A clean split is:

```text
CONFIG_DB: table A1 -> app 1 -> APPL_DB: table B1
CONFIG_DB: table A2 -> app 2 -> APPL_DB: table B2
```

If app 1 and app 2 do not share input tables, output tables, keys, or external
side effects, they usually do not need a workflow-level mutex.

Avoid:

```text
CONFIG_DB: same table/key range -> multiple apps -> same APPL_DB table
```

unless there is a clear owner, allocator, version check, or explicit lock.

## Route Flow Counter Table Split

Route flow counters use several DBs at once, so they are a useful check against
overusing the `ProducerTable` / `ConsumerTable` model. Configuration is direct
`CONFIG_DB` hash state, route pattern consumption is a CONFIG_DB subscription,
traditional polling setup uses `FLEX_COUNTER_DB`, and display data is read from
`COUNTERS_DB`.

| Flow | Producer / Table | DB / Table | Consumer / Table |
| --- | --- | --- | --- |
| Enable route flow counter | CLI / `Table` | `CONFIG_DB:FLEX_COUNTER_TABLE|FLOW_CNT_ROUTE` | `FlexCounterOrch` / orch `Consumer` backed by `SubscriberStateTable` |
| Route pattern config | CLI / `Table` | `CONFIG_DB:FLOW_COUNTER_ROUTE_PATTERN_TABLE|<vrf>|<prefix>` | `FlowCounterRouteOrch` / orch `Consumer` backed by `SubscriberStateTable` |
| Polling setup, traditional mode | `FlowCounterRouteOrch` via `FlexCounterManager` / `ProducerTable` | `FLEX_COUNTER_DB:FLEX_COUNTER_TABLE:ROUTE_FLOW_COUNTER:<counter_oid>` field `FLOW_COUNTER_ID_LIST` | `syncd` flex counter logic |
| Route-to-counter mapping | `FlowCounterRouteOrch` / `Table` | `COUNTERS_DB:COUNTERS_ROUTE_NAME_MAP` | CLI/display / `Table` or direct Redis hash read |
| Route-to-pattern mapping | `FlowCounterRouteOrch` / `Table` | `COUNTERS_DB:COUNTERS_ROUTE_TO_PATTERN_MAP` | CLI/display / `Table` or direct Redis hash read |
| Polled counter stats | `syncd` flex counter polling / `Table`-style hash write | `COUNTERS_DB:COUNTERS:<counter_oid>` | CLI/display / `Table` or direct Redis hash read |

In non-traditional flex counter mode, `FlexCounterManager` can call sairedis
switch attributes such as `SAI_REDIS_SWITCH_ATTR_FLEX_COUNTER`. That path is
not a direct swsscommon `ProducerTable` write to `FLEX_COUNTER_DB`.

## Example Flows

### custom_tables

The `custom_tables` example is the minimal `CONFIG_DB -> APPL_DB` flow. It is
useful when the goal is to understand custom table naming, config observation,
and APPL_DB pending state without adding an APPL table owner.

`custom_tables` demonstrates a minimal `CONFIG_DB -> APPL_DB` desired-state
flow:

```text
config_db_producer.py
  -> CONFIG_DB CUSTOM_CONFIG_TABLE|demo
  -> config_to_appl_bridge.py
  -> APPL_DB pending _CUSTOM_APPL_TABLE:demo
```

The bridge uses `ProducerStateTable`, so it writes pending APPL state and the
key set. This example intentionally has no APPL table owner, so the final
`CUSTOM_APPL_TABLE:demo` hash is not materialized.

The implementation keeps the naming contract local to this repository:

- `src/swss/common/custom_schema.py` defines Python table constants shared by the
  examples.
- `src/swss/custom_tables/example_schema.h` mirrors those constants for C/C++.
- `src/swss/custom_tables/config_db_producer.py` writes config with `Table`.
- `src/swss/custom_tables/config_to_appl_bridge.py` watches config with
  `SubscriberStateTable` and publishes APPL pending state with
  `ProducerStateTable`.

### vlan_table

The `vlan_table` example adds the missing consumers so the reader can see the
same table APIs across the full `CONFIG_DB -> APPL_DB -> ASIC_DB` chain.

`vlan_table` models the full SONiC-style chain:

```text
config vlan add 100
  -> CONFIG_DB VLAN|Vlan100
  -> vlanmgrd
  -> APPL_DB pending _VLAN_TABLE:Vlan100
  -> PortsOrch
  -> ASIC_DB queue ASIC_STATE:SAI_OBJECT_TYPE_VLAN_KEY_VALUE_OP_QUEUE
  -> syncd
  -> ASIC_DB final ASIC_STATE:SAI_OBJECT_TYPE_VLAN:oid:0x26000000000100
  -> ASIC_DB GETRESPONSE table with op getresponse
  -> PortsOrch
  -> APPL_DB_VLAN_TABLE_RESPONSE_CHANNEL on APPL_STATE_DB
  -> vlanmgrd
```

It also models the async notification direction used for events such as port
state changes:

```text
syncd
  -> ASIC_DB NOTIFICATIONS channel
  -> PortsOrch
  -> STATE_DB PORT_TABLE|Ethernet0
  -> vlanmgrd / other manager daemons
```

The implementation maps each SONiC component role to a small Python script:

- `src/swss/vlan_table/config_vlan_command.py` emulates `config vlan add/del`.
- `src/swss/vlan_table/vlanmgrd.py` bridges `CONFIG_DB VLAN` to APPL pending state
  consumes APPL response notifications, and can observe `STATE_DB PORT_TABLE`.
- `src/swss/vlan_table/portorch.py` consumes APPL pending state and queues ASIC
  operations, reads sairedis GETRESPONSE entries, publishes APPL responses, and
  handles async ASIC notifications.
- `src/swss/vlan_table/syncd.py` consumes ASIC operations, logs a fake ASIC write,
  writes sairedis GETRESPONSE entries, and can emit async port notifications.

`portorch.py` is intentionally object-oriented while the smaller scripts remain
function-based. `PortsOrchDemo` owns the APPL/ASIC DB connections, table objects,
GETRESPONSE consumer, APPL response producer, async notification consumer, and
in-flight VLAN request map. This keeps the three flows distinct:

- `handle_vlan_update`: `APPL_DB VLAN_TABLE` -> `ASIC_DB` operation queue.
- `handle_sai_response`: `ASIC_DB GETRESPONSE` -> APPL response channel.
- `handle_notification`: `ASIC_DB:NOTIFICATIONS` -> `STATE_DB PORT_TABLE`.

Only the VLAN request/response path has explicit state:

```text
APPL_RECEIVED -> ASIC_SENT -> ASIC_RESPONDED -> APPL_RESPONDED
```

The async notification path is not part of that state machine because it is not a
request lifecycle.

## Running The Examples

### Test Environment

The local environment uses:

- `database`: Redis with `/var/run/redis/redis.sock`.
- `runner`: a container that builds and installs local `sonic-swss-common`.
- `swss-common-install`: a named volume for compiled output under
  `/usr/local`.

Initial setup:

```bash
cd /home/ubuntu/swss-common-example
sudo mkdir -p /var/run/redis
docker compose build runner
docker compose up -d database
```

### Method 1: Pure Bash Commands

Run bridge:

```bash
cd /home/ubuntu/swss-common-example
docker compose up -d database
UID=$(id -u) GID=$(id -g) docker compose run --rm -T runner \
  src/swss/custom_tables/config_to_appl_bridge.py --key demo --watch
```

In another terminal, write config:

```bash
cd /home/ubuntu/swss-common-example
UID=$(id -u) GID=$(id -g) docker compose run --rm -T runner \
  src/swss/custom_tables/config_db_producer.py --key demo --enabled true --interval 10
```

For the VLAN flow, start `syncd`, `portorch`, and `vlanmgrd` in separate
terminals, then run the config command:

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

```bash
cd /home/ubuntu/swss-common-example
UID=$(id -u) GID=$(id -g) docker compose run --rm -T runner \
  src/swss/vlan_table/config_vlan_command.py add 100
```

### Method 2: Helper Scripts

Prefer helper scripts for long-running watch commands. They start `database`,
export `UID`/`GID`, pass extra arguments through, and remove their runner
container on `Ctrl-C`.

Custom table flow:

```bash
cd /home/ubuntu/swss-common-example
scripts/run_custom_tables_example.sh bridge --key demo --watch
```

```bash
cd /home/ubuntu/swss-common-example
scripts/run_custom_tables_example.sh producer --key demo --enabled true --interval 10
```

VLAN flow:

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

Full VLAN verification:

```bash
cd /home/ubuntu/swss-common-example
scripts/run_vlan_table_example.sh verify 100
```

## Verification

Custom table checks:

```bash
docker exec database redis-cli -s /var/run/redis/redis.sock -n 4 HGETALL 'CUSTOM_CONFIG_TABLE|demo'
docker exec database redis-cli -s /var/run/redis/redis.sock -n 0 HGETALL '_CUSTOM_APPL_TABLE:demo'
docker exec database redis-cli -s /var/run/redis/redis.sock -n 0 SMEMBERS 'CUSTOM_APPL_TABLE_KEY_SET'
```

VLAN checks:

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

`Table` writes final hashes directly. `ProducerStateTable` and `ProducerTable`
do not. Their matching consumers materialize final hashes:

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
