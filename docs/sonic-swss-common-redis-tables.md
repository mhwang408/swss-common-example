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
submodule. Keep the table names in project code, for example:

- `src/custom_tables/ows_schema.h`
- Python constants in `src/custom_tables/*.py`

Use upstream `sonic-swss-common/common/schema.h` only for upstream SONiC table
names that already exist in SONiC.

`ows_schema.h` can include upstream schema constants:

```c
#include "sonic-swss-common/common/schema.h"

#define OWS_CFG_CUSTOM_CONFIG_TABLE_NAME "CUSTOM_CONFIG_TABLE"
#define OWS_APP_CUSTOM_APPL_TABLE_NAME   "CUSTOM_APPL_TABLE"
```

For this minimal example, Python uses string constants directly:

```python
CONFIG_TABLE = "CUSTOM_CONFIG_TABLE"
APPL_TABLE = "CUSTOM_APPL_TABLE"
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

In this project, the `database` container copies the project
`database_config.json` to that path and exposes `/var/run/redis/redis.sock`.
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

## Direct CONFIG_DB Writes

For normal config writes, use `Table`.

```python
config_db = swsscommon.DBConnector("CONFIG_DB", 0, False)
table = swsscommon.Table(config_db, "CUSTOM_CONFIG_TABLE")

values = swsscommon.FieldValuePairs([
    ("enabled", "true"),
    ("interval", "10"),
])

table.set("demo", values)
```

With the `CONFIG_DB` separator `|`, this writes:

```text
DB 4 hash key: CUSTOM_CONFIG_TABLE|demo
```

Use `Table` when you want direct hash access and do not need queue semantics.
This is appropriate for a config producer writing desired configuration into
`CONFIG_DB`.

## Subscribing To CONFIG_DB

To react to config changes, use `SubscriberStateTable`.

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

Local Redis must enable keyspace notifications for this to work:

```text
--notify-keyspace-events KEA
```

The compose Redis container enables this flag.

## Writing APPL_DB Desired State

Use `ProducerStateTable` when the app publishes latest desired state to
`APPL_DB`.

```python
appl_db = swsscommon.DBConnector("APPL_DB", 0, False)
appl_table = swsscommon.ProducerStateTable(appl_db, "CUSTOM_APPL_TABLE")

values = swsscommon.FieldValuePairs([
    ("admin_status", "up"),
    ("poll_interval", "10"),
])

appl_table.set("demo", values)
```

This does not directly materialize the final table hash. It writes pending
state for the APPL table owner:

```text
DB 0 hash: _CUSTOM_APPL_TABLE:demo
DB 0 set:  CUSTOM_APPL_TABLE_KEY_SET
```

The consumer side uses `ConsumerStateTable` to pop the changed key, read the
pending hash, and materialize or apply the state.

## Table API Selection

Use `Table` when:

- You need direct Redis hash access.
- You are writing config into `CONFIG_DB`.
- You are reading current state without queue semantics.

Use `SubscriberStateTable` when:

- You need to watch `CONFIG_DB` changes.
- You want startup state plus live updates.
- Redis keyspace notifications are available.

Use `ProducerStateTable` and `ConsumerStateTable` when:

- The producer publishes desired latest state.
- Intermediate updates can be coalesced.
- Ordering across keys is not required.
- Same key and same field can use last-write-wins semantics.

Use `ProducerTable` and `ConsumerTable` when:

- Every operation must be delivered.
- In-order processing matters.
- You want an operation queue rather than latest-state coalescing.

## ProducerTable vs ProducerStateTable

`ProducerTable` is operation-log-like:

```text
producer:
  push operation into a Redis list
  publish notification

consumer:
  pop list in order
  process each operation
```

Use it when operation order is meaningful and every operation matters.

`ProducerStateTable` is latest-state-like:

```text
producer:
  HSET _<TABLE>:<key> field value
  SADD <TABLE>_KEY_SET key
  publish notification

consumer:
  SPOP <TABLE>_KEY_SET
  HGETALL _<TABLE>:<key>
  apply latest state
```

Use it when the final desired state matters more than every intermediate
operation.

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
