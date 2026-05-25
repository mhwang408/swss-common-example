# Notes: Define And Access SONiC Redis Tables With sonic-swss-common

This note summarizes how to define and read/write Redis tables when building a
small SONiC-style app on top of `sonic-swss-common`.

## Mental Model

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

## Where To Define A Table

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

## Do You Need YANG Or gen_cfg_schema.py?

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

## database_config.json

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

## Four Table API Patterns

These are the four table patterns discussed in this project. The important
choice is whether the app needs direct hash access, config-change subscription,
an ordered operation stream, or latest-state coalescing.

### 1. Table

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

### 2. SubscriberStateTable

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

### 3. ProducerTable / ConsumerTable

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

For the VLAN ASIC_DB example in this repo, `vlanorch.py` uses `ProducerTable`
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
  ("source", "VlanOrch"),
]
```

`ConsumerTable.pop()` is the point where the queued operation becomes a final
ASIC_DB hash. In the example, `syncd.py` then prints the tuple and logs a fake
ASIC write; it does not touch a real ASIC.

This is closer to an operation log than a state snapshot. It is useful when the
consumer must see `SET A`, then `DEL A`, then `SET A` as three separate events.

Do not use it when the final state per key is enough. In that case,
`ProducerStateTable` is usually simpler and cheaper.

### 4. ProducerStateTable / ConsumerStateTable

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

The final APPL_DB hash is materialized later by `vlanorch.py`
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

## Selection Summary

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

## Atomicity And Locks

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

## Ownership Pattern

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

## Minimal Project Flow

Run bridge:

```bash
docker compose run --rm runner
```

Write config:

```bash
docker compose run --rm \
  --entrypoint python3 \
  runner \
  config_db_producer.py \
  --key demo \
  --enabled true \
  --interval 10 \
  --db-config /tmp/database_config.json
```

Inspect Redis:

```bash
docker exec database redis-cli -s /var/run/redis/redis.sock -n 4 HGETALL 'CUSTOM_CONFIG_TABLE|demo'
docker exec database redis-cli -s /var/run/redis/redis.sock -n 0 HGETALL '_CUSTOM_APPL_TABLE:demo'
docker exec database redis-cli -s /var/run/redis/redis.sock -n 0 SMEMBERS 'CUSTOM_APPL_TABLE_KEY_SET'
```
