# sonic-swss-common Examples

This repo is a small Python playground for understanding how SONiC components
use `sonic-swss-common` to exchange state through Redis-backed SONiC databases.

It contains two examples:

- `custom_tables`: a minimal custom `CONFIG_DB -> APPL_DB` flow.
- `vlan_table`: a four-stage VLAN flow modeled after
  `config vlan`, `vlanmgrd`, `VlanOrch`, and `syncd`.

Detailed table/API notes are in
[docs/sonic-swss-common-redis-tables.md](docs/sonic-swss-common-redis-tables.md).
Chinese notes are available in
[docs/sonic-swss-common-redis-tables-zh.md](docs/sonic-swss-common-redis-tables-zh.md)
and [docs/config-sync-flow-in-SONiC-zh.md](docs/config-sync-flow-in-SONiC-zh.md).

## 1. sonic-swss-common Basics

### Role In SONiC

`sonic-swss-common` is the shared client library used by SONiC control-plane
components to read and write SONiC databases. Those databases are Redis DBs
with SONiC-specific names, table separators, key conventions, producer/consumer
queues, and notification patterns.

Typical SONiC flows look like this:

```text
CONFIG_DB -> manager daemon -> APPL_DB -> orchagent -> ASIC_DB -> syncd -> ASIC
```

The library does not replace Redis. It gives SONiC components a higher-level
API over Redis so they can speak in terms of SONiC DB names and table types
instead of raw Redis commands.

### sonic-swss-common vs redis-py

| Choice | Pros | Cons |
| --- | --- | --- |
| `sonic-swss-common` | Understands SONiC DB names from `database_config.json`; applies SONiC table separators; provides `Table`, `ProducerTable`, `ConsumerTable`, `ProducerStateTable`, `ConsumerStateTable`, `SubscriberStateTable`, and `Select`; matches production SONiC component behavior. | Requires the SONiC library and runtime DB config; less general-purpose than direct Redis clients; queue/materialization behavior is easy to misuse if the table type is wrong. |
| `redis-py` / raw Redis | Simple dependency; good for direct inspection, one-off scripts, tests, and generic Redis operations. | You must manually choose DB IDs, separators, key-set names, queue keys, and Lua/atomic behavior; easy to produce data that looks plausible but is not what SONiC daemons consume. |

Use `sonic-swss-common` when modeling or implementing SONiC component behavior.
Use `redis-cli` or `redis-py` when inspecting DB state or writing support tools
that do not need SONiC table semantics.

### Basic Concepts

- `DBConnector` connects to a SONiC DB by logical name, such as `CONFIG_DB`,
  `APPL_DB`, or `ASIC_DB`.
- `database_config.json` maps logical DB names to Redis DB numbers and table
  separators. In this repo, `CONFIG_DB` uses `|`; `APPL_DB` and `ASIC_DB` use
  `:`.
- A table name plus object key becomes a Redis key. For example,
  `VLAN|Vlan100` in `CONFIG_DB` and `VLAN_TABLE:Vlan100` in `APPL_DB`.
- Some APIs write final table content immediately. Others write pending state
  or queue entries that a matching consumer materializes later.

### Four Table Families And Use Cases

| API | Typical DB | Behavior | Use Case |
| --- | --- | --- | --- |
| `Table` | `CONFIG_DB`, final state reads | Direct hash read/write/delete. | Durable config/state tables where the writer owns the final Redis hash. |
| `SubscriberStateTable` | `CONFIG_DB` | Receives update notifications for a table. | Manager daemons watching config changes. |
| `ProducerStateTable` / `ConsumerStateTable` | `APPL_DB` | Producer writes pending state under an underscore-prefixed hash and key set; consumer pops it and materializes/deletes the final table hash. | Managers publishing desired state and orch components consuming it. |
| `ProducerTable` / `ConsumerTable` | `ASIC_DB` and ordered queues | Producer enqueues ordered operations; consumer pops queue entries and materializes/deletes the final table hash. | Orch components sending ordered ASIC operations and `syncd` applying them. |

The common mistake is assuming every producer writes final table content. That
is true for `Table`, but not for `ProducerStateTable` or `ProducerTable`.

## 2. Examples

### Example A: custom_tables

#### Conceptual Behavior

This is the smallest custom table flow:

```text
config_db_producer.py
  -> CONFIG_DB CUSTOM_CONFIG_TABLE|demo
  -> config_to_appl_bridge.py
  -> APPL_DB pending _CUSTOM_APPL_TABLE:demo
```

`config_db_producer.py` writes one durable config entry:

```text
CUSTOM_CONFIG_TABLE|demo
  enabled=true
  interval=10
  updated_at=<unix timestamp>
```

`config_to_appl_bridge.py` subscribes to `CUSTOM_CONFIG_TABLE` and publishes a
desired APPL update:

```text
_CUSTOM_APPL_TABLE:demo
  admin_status=up
  poll_interval=10
  source_table=CUSTOM_CONFIG_TABLE
  source_key=demo
  published_at=<unix timestamp>
```

Because the bridge uses `ProducerStateTable`, the APPL update is pending. A
real APPL table owner would consume it with `ConsumerStateTable` and
materialize the final `CUSTOM_APPL_TABLE:demo` hash.

#### Coding Details

Files:

- `src/custom_tables/example_schema.py`: Python table-name constants.
- `src/custom_tables/example_schema.h`: C/C++ table-name constants.
- `src/custom_tables/config_db_producer.py`: writes `CONFIG_DB` with `Table`.
- `src/custom_tables/config_to_appl_bridge.py`: watches `CONFIG_DB` with
  `SubscriberStateTable` and publishes `APPL_DB` with `ProducerStateTable`.

Key APIs:

```python
config_db = swsscommon.DBConnector("CONFIG_DB", 0, False)
config_table = swsscommon.Table(config_db, CONFIG_TABLE)
config_table.set(args.key, field_value_pairs(values))
```

```python
config_subscriber = swsscommon.SubscriberStateTable(config_db, CONFIG_TABLE)
appl_table = swsscommon.ProducerStateTable(appl_db, APPL_TABLE)
key, op, field_values = config_subscriber.pop()
appl_table.set(key, field_value_pairs(appl_values))
```

### Example B: vlan_table

#### Conceptual Behavior

This example mirrors the SONiC VLAN path:

```text
config vlan add/del 100
  -> CONFIG_DB VLAN|Vlan100
  -> vlanmgrd
  -> APPL_DB pending _VLAN_TABLE:Vlan100
  -> VlanOrch
  -> ASIC_DB queue ASIC_STATE:SAI_OBJECT_TYPE_VLAN_KEY_VALUE_OP_QUEUE
  -> syncd
  -> ASIC_DB final ASIC_STATE:SAI_OBJECT_TYPE_VLAN:oid:...
```

The example has four components:

| Component | Role |
| --- | --- |
| `config_vlan_command.py` | Emulates `config vlan add 100` and `config vlan del 100`. |
| `vlanmgrd.py` | Reads/watches `CONFIG_DB VLAN` and publishes APPL desired state. |
| `vlanorch.py` | Consumes APPL desired state and enqueues ASIC operations. |
| `syncd.py` | Consumes ASIC operations and logs a fake ASIC write. |

#### Coding Details

Files:

- `src/vlan_table/vlan_schema.py`: table constants and fake ASIC OID helper.
- `src/vlan_table/config_vlan_command.py`: `Table.set/delete` in `CONFIG_DB`.
- `src/vlan_table/vlanmgrd.py`: `Table.get`, `SubscriberStateTable.pop`, and
  `ProducerStateTable.set/delete`.
- `src/vlan_table/vlanorch.py`: `ConsumerStateTable.pop` and
  `ProducerTable.set/delete`.
- `src/vlan_table/syncd.py`: `ConsumerTable.pop`.
- `src/vlan_table/vlan_log.py`: structured logs and verification markers.

Component/API mapping:

| Component | API | Effect |
| --- | --- | --- |
| `config_vlan_command.py` | `Table.set/delete` | Materializes/deletes `CONFIG_DB VLAN|Vlan100`. |
| `vlanmgrd.py` | `Table.get` | Replays existing `CONFIG_DB` config in one-shot mode. |
| `vlanmgrd.py` | `SubscriberStateTable.pop` | Reads later `CONFIG_DB` changes in watch mode. |
| `vlanmgrd.py` | `ProducerStateTable.set/delete` | Writes APPL pending state. |
| `vlanorch.py` | `ConsumerStateTable.pop` | Materializes/deletes final `APPL_DB VLAN_TABLE:Vlan100`. |
| `vlanorch.py` | `ProducerTable.set/delete` | Enqueues ordered ASIC operations. |
| `syncd.py` | `ConsumerTable.pop` | Materializes/deletes final `ASIC_DB` table content. |

## 3. Run The Examples

### Test Environment

You do not need the full SONiC stack. The repo uses a normal Redis container
plus a `runner` container that builds and installs the local
`src/sonic-swss-common` checkout.

The compose environment provides:

- `database`: Redis listening on `/var/run/redis/redis.sock`.
- `runner`: Ubuntu-based image with build dependencies and the repo mounted at
  `/home/ubuntu/swss-common-example`.
- `swss-common-install`: named volume mounted at `/usr/local` so the compiled
  library is reused across container runs.

Initial setup:

```bash
cd /home/ubuntu/swss-common-example
sudo mkdir -p /var/run/redis
docker compose build runner
docker compose up -d database
```

The `runner` service runs as `${UID}:${GID}`. The helper scripts export these
values automatically. If you run raw `docker compose` commands directly, either
use `direnv`:

```bash
cd /home/ubuntu/swss-common-example
direnv allow .
```

or prefix the command:

```bash
UID=$(id -u) GID=$(id -g) docker compose run --rm runner
```

The first `runner` start builds `sonic-swss-common` through `entrypoint.sh`.
To force a rebuild of the installed library:

```bash
docker volume rm swss-common-example_swss-common-install
```

To rebuild the runner image itself:

```bash
docker compose build --no-cache runner
```

### Method 1: Pure Bash Commands

These commands use `docker compose` directly. For long-running watch commands,
the helper-script method below has better interrupt cleanup.

Custom table flow:

```bash
cd /home/ubuntu/swss-common-example
docker compose up -d database
UID=$(id -u) GID=$(id -g) docker compose run --rm -T runner \
  src/custom_tables/config_to_appl_bridge.py --key demo --watch
```

In another terminal:

```bash
cd /home/ubuntu/swss-common-example
UID=$(id -u) GID=$(id -g) docker compose run --rm -T runner \
  src/custom_tables/config_db_producer.py --key demo --enabled true --interval 10
```

VLAN flow, four terminals:

```bash
cd /home/ubuntu/swss-common-example
UID=$(id -u) GID=$(id -g) docker compose run --rm -T runner \
  src/vlan_table/syncd.py --vlan-id 100 --watch
```

```bash
cd /home/ubuntu/swss-common-example
UID=$(id -u) GID=$(id -g) docker compose run --rm -T runner \
  src/vlan_table/vlanorch.py --vlan-id 100 --watch
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

After verifying the add path, optionally test delete:

```bash
cd /home/ubuntu/swss-common-example
UID=$(id -u) GID=$(id -g) docker compose run --rm -T runner \
  src/vlan_table/config_vlan_command.py del 100
```

### Method 2: Helper Scripts

The helper scripts start `database` when needed, export `UID`/`GID`, run the
selected component in `runner`, pass extra arguments through, and remove their
runner container on `Ctrl-C`.

Custom table flow:

```bash
cd /home/ubuntu/swss-common-example
scripts/run_custom_tables_example.sh bridge --key demo --watch
```

In another terminal:

```bash
cd /home/ubuntu/swss-common-example
scripts/run_custom_tables_example.sh producer --key demo --enabled true --interval 10
```

VLAN flow, four terminals:

```bash
cd /home/ubuntu/swss-common-example
scripts/run_vlan_table_example.sh syncd --vlan-id 100 --watch
```

```bash
cd /home/ubuntu/swss-common-example
scripts/run_vlan_table_example.sh orch --vlan-id 100 --watch
```

```bash
cd /home/ubuntu/swss-common-example
scripts/run_vlan_table_example.sh mgrd --vlan-id 100 --watch
```

```bash
cd /home/ubuntu/swss-common-example
scripts/run_vlan_table_example.sh config-add 100
```

After verifying the add path, optionally test delete:

```bash
cd /home/ubuntu/swss-common-example
scripts/run_vlan_table_example.sh config-del 100
```

One-shot VLAN verification:

```bash
cd /home/ubuntu/swss-common-example
scripts/run_vlan_table_example.sh verify 100
```

## 4. Verify The Result

### Custom Table Checks

```bash
docker exec database redis-cli -s /var/run/redis/redis.sock -n 4 HGETALL 'CUSTOM_CONFIG_TABLE|demo'
docker exec database redis-cli -s /var/run/redis/redis.sock -n 0 HGETALL '_CUSTOM_APPL_TABLE:demo'
docker exec database redis-cli -s /var/run/redis/redis.sock -n 0 SMEMBERS 'CUSTOM_APPL_TABLE_KEY_SET'
```

Expected `CONFIG_DB` fields:

```text
enabled
interval
updated_at
```

Expected pending `APPL_DB` fields:

```text
admin_status
poll_interval
source_table
source_key
published_at
```

### VLAN Checks

```bash
docker exec database redis-cli -s /var/run/redis/redis.sock -n 4 HGETALL 'VLAN|Vlan100'
docker exec database redis-cli -s /var/run/redis/redis.sock -n 0 HGETALL '_VLAN_TABLE:Vlan100'
docker exec database redis-cli -s /var/run/redis/redis.sock -n 0 SMEMBERS 'VLAN_TABLE_KEY_SET'
docker exec database redis-cli -s /var/run/redis/redis.sock -n 0 HGETALL 'VLAN_TABLE:Vlan100'
docker exec database redis-cli -s /var/run/redis/redis.sock -n 1 LRANGE 'ASIC_STATE:SAI_OBJECT_TYPE_VLAN_KEY_VALUE_OP_QUEUE' 0 -1
docker exec database redis-cli -s /var/run/redis/redis.sock -n 1 HGETALL 'ASIC_STATE:SAI_OBJECT_TYPE_VLAN:oid:0x26000000000100'
```

`scripts/verify_vlan_flow.sh 100` clears DB 0, DB 1, and DB 4, runs the full
VLAN flow, prints DB state after each phase, captures raw Redis `MONITOR`
output in `/tmp/swss_vlan_monitor_*.log`, and writes a filtered log to
`/tmp/swss_vlan_pretty_*.log`.

The expected materialization result is:

```text
CONFIG_DB VLAN|Vlan100:
  materialized by config_vlan_command.py Table.set

APPL_DB pending _VLAN_TABLE:Vlan100 and VLAN_TABLE_KEY_SET:
  written by vlanmgrd.py ProducerStateTable.set

APPL_DB final VLAN_TABLE:Vlan100:
  materialized by vlanorch.py ConsumerStateTable.pop

ASIC_DB queue ASIC_STATE:SAI_OBJECT_TYPE_VLAN_KEY_VALUE_OP_QUEUE:
  written by vlanorch.py ProducerTable.set

ASIC_DB final ASIC_STATE:SAI_OBJECT_TYPE_VLAN:oid:0x26000000000100:
  materialized by syncd.py ConsumerTable.pop
```

## 5. Appendix: Who Materializes The Final Table?

The answer depends on the table API.

| API | Producer Writes Final Hash? | Consumer Materializes Final Hash? |
| --- | --- | --- |
| `Table` | Yes. `set` writes the final hash and `delete` removes it. | No matching consumer is required. |
| `ProducerStateTable` | No. It writes pending state under `_TABLE:key` and a key set. | Yes. `ConsumerStateTable.pop` materializes or deletes `TABLE:key`. |
| `ProducerTable` | No. It enqueues operations in `TABLE_KEY_VALUE_OP_QUEUE`. | Yes. `ConsumerTable.pop` materializes or deletes `TABLE:key`. |
| `SubscriberStateTable` | No. It is a reader/subscriber. | No. It observes updates from a table writer. |

For the VLAN example, the Redis monitor trace confirms the behavior. Important
sections in the pretty log look like:

```text
## vlanmgrd ProducerStateTable.set APPL_DB:VLAN_TABLE
  HSET _VLAN_TABLE:Vlan100 vlanid 100
  SADD VLAN_TABLE_KEY_SET Vlan100

## vlanorch ConsumerStateTable.pop APPL_DB:VLAN_TABLE
  SPOP VLAN_TABLE_KEY_SET
  HGETALL _VLAN_TABLE:Vlan100
  HSET VLAN_TABLE:Vlan100 vlanid 100
  DEL _VLAN_TABLE:Vlan100

## vlanorch ProducerTable.set ASIC_DB:ASIC_STATE:SAI_OBJECT_TYPE_VLAN
  LPUSH ASIC_STATE:SAI_OBJECT_TYPE_VLAN_KEY_VALUE_OP_QUEUE ...

## syncd ConsumerTable.pop ASIC_DB:ASIC_STATE:SAI_OBJECT_TYPE_VLAN
  LRANGE ASIC_STATE:SAI_OBJECT_TYPE_VLAN_KEY_VALUE_OP_QUEUE
  LTRIM ASIC_STATE:SAI_OBJECT_TYPE_VLAN_KEY_VALUE_OP_QUEUE
  HSET ASIC_STATE:SAI_OBJECT_TYPE_VLAN:oid:0x26000000000100 SAI_VLAN_ATTR_VLAN_ID 100
```

This is why APPL and ASIC final hashes may remain empty until the corresponding
consumer runs, even though the producer component already published its update.
