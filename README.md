# Custom CONFIG_DB and APPL_DB Tables

This is a minimal Python example for creating one custom table in `CONFIG_DB`
and one custom table in `APPL_DB`.

The flow is:

1. `config_db_producer.py` writes `CUSTOM_CONFIG_TABLE|demo` into `CONFIG_DB`.
2. `config_to_appl_bridge.py` subscribes to that CONFIG_DB table with
   `SubscriberStateTable` and publishes `CUSTOM_APPL_TABLE:demo` into
   `APPL_DB` with `ProducerStateTable`.

`swsscommon.Table` uses the database separator from `database_config.json`, so
CONFIG_DB keys use `|` and APPL_DB keys use `:`.

Detailed notes on defining custom table names, choosing `swsscommon` table
APIs, and reasoning about Redis/Lua atomicity are in
[docs/sonic-swss-common-redis-tables.md](docs/sonic-swss-common-redis-tables.md).

## Run

You do not need to run the full SONiC stack for this minimal example. A normal
Redis container is enough because this example only uses Redis DB numbers and
SONiC table separators. The compose file provides a `runner` image with build
dependencies. The repo is bind-mounted into the runner, so edits to
`src/sonic-swss-common`, `entrypoint.sh`, and `database_config.json` take
effect on the next container run without rebuilding the image.

The `runner` service runs as `${UID}:${GID}` so Python bytecode caches created
inside the bind-mounted repo are owned by your host user, not by root or the
container fallback user. This repo includes an `.envrc` for `direnv`:

```bash
cd /home/ubuntu/swss-common-example
direnv allow .
```

After that, entering the repo automatically exports `UID` and `GID` for
`docker compose`. Without `direnv`, prefix compose commands explicitly:

```bash
UID=$(id -u) GID=$(id -g) docker compose run --rm runner
```

Build the runner and start a local container named `database`:

```bash
cd /home/ubuntu/swss-common-example
sudo mkdir -p /var/run/redis
docker compose build runner
docker compose up -d
```

The compose file bind-mounts host `/var/run/redis` into both `database` and
`runner`. Redis creates `/var/run/redis/redis.sock`, so the host and other
containers can use the same Unix socket. To avoid nested bind mounts under
`/var/run/redis`, `entrypoint.sh` copies the repo `database_config.json` into
`/var/run/redis/sonic-db/database_config.json`, which is the default path used
by `swsscommon`. TCP is disabled in this local setup.

The image only contains build dependencies. Compilation of `sonic-swss-common`
happens at container start via `entrypoint.sh`, which checks for
`/usr/local/lib/libswsscommon.so` and builds only if it is missing. The
compiled output is stored in a named volume (`swss-common-install`) mounted at
`/usr/local`, so `docker compose down && docker compose up` does **not**
trigger a rebuild. To force a rebuild after changing `src/sonic-swss-common`:

```bash
docker volume rm swss-common-example_swss-common-install
```

To rebuild the runner image itself (e.g. after changing `Dockerfile`):

```bash
docker compose build --no-cache runner
```

Then run the bridge in the runner container, using the shared Redis socket and
the compose DB config.

Terminal 1:

```bash
cd /home/ubuntu/swss-common-example
docker compose run --rm runner
```

In another terminal, write CONFIG_DB:

Terminal 2:

```bash
cd /home/ubuntu/swss-common-example
docker compose run --rm \
  --entrypoint python3 \
  runner \
  src/custom_tables/config_db_producer.py \
  --key demo \
  --enabled true \
  --interval 10
```

Verify through the container:

```bash
docker exec database redis-cli -s /var/run/redis/redis.sock -n 4 HGETALL 'CUSTOM_CONFIG_TABLE|demo'
docker exec database redis-cli -s /var/run/redis/redis.sock -n 0 HGETALL '_CUSTOM_APPL_TABLE:demo'
docker exec database redis-cli -s /var/run/redis/redis.sock -n 0 SMEMBERS 'CUSTOM_APPL_TABLE_KEY_SET'
```

`config_to_appl_bridge.py` uses `ProducerStateTable`, so it produces a pending
APPL_DB update for the APPL table owner. The pending data lives under the
underscore-prefixed hash plus the key set. A real APPL table owner would consume
that with `ConsumerStateTable` and materialize the final
`CUSTOM_APPL_TABLE:demo` hash.

To keep the bridge running and republish when config changes:

```bash
cd /home/ubuntu/swss-common-example
docker compose run --rm runner
```

## Run On SONiC Or Host With swsscommon

`sonic-swss-common` is only the client library. The Redis server for SONiC DBs
is normally owned by the SONiC `database` container, so direct execution needs:

- the Python `swsscommon` package available in the runtime environment
- `/var/run/redis/sonic-db/database_config.json`
- access to `/var/run/redis/redis.sock`

On a SONiC switch, SONiC VS image, or host that already has Python
`swsscommon` installed, run these from a container/namespace that has the Redis
socket mounted. The scripts use the Unix socket by default.

Terminal 1:

```bash
cd /home/ubuntu/swss-common-example
python3 src/custom_tables/config_to_appl_bridge.py --key demo --watch
```

Terminal 2:

```bash
cd /home/ubuntu/swss-common-example
python3 src/custom_tables/config_db_producer.py --key demo --enabled true --interval 10
```

On SONiC, `redis-cli` can also be run through the `database` container when the
host environment does not have direct socket access:

```bash
docker exec database redis-cli -s /var/run/redis/redis.sock -n 4 HGETALL 'CUSTOM_CONFIG_TABLE|demo'
docker exec database redis-cli -s /var/run/redis/redis.sock -n 0 HGETALL '_CUSTOM_APPL_TABLE:demo'
```

## Verify With redis-cli

```bash
redis-cli -s /var/run/redis/redis.sock -n 4 HGETALL 'CUSTOM_CONFIG_TABLE|demo'
redis-cli -s /var/run/redis/redis.sock -n 0 HGETALL '_CUSTOM_APPL_TABLE:demo'
redis-cli -s /var/run/redis/redis.sock -n 0 SMEMBERS 'CUSTOM_APPL_TABLE_KEY_SET'
```

Expected CONFIG_DB fields include:

```text
enabled
interval
updated_at
```

Expected pending APPL_DB fields include:

```text
admin_status
poll_interval
source_table
source_key
published_at
```

## VLAN_TABLE Example

This second example mirrors the SONiC VLAN flow:

```text
config vlan add/del 100
  -> CONFIG_DB VLAN|Vlan100
  -> vlanmgrd SubscriberStateTable
  -> APPL_DB VLAN_TABLE:Vlan100 through ProducerStateTable
  -> VlanOrch ConsumerStateTable
  -> ASIC_DB ASIC_STATE:SAI_OBJECT_TYPE_VLAN:oid:... through ProducerTable
  -> syncd ConsumerTable
  -> fake ASIC write log
```

Phase 1 emulates the config command. `config vlan add 100` writes this
CONFIG_DB entry:

```text
Database: CONFIG_DB (DB ID: 4)
Key:      VLAN|Vlan100
Value:    {"vlanid": "100"}
```

`config vlan del 100` deletes `VLAN|Vlan100` from CONFIG_DB.

Phase 2 is modeled by `vlanmgrd.py`. It subscribes to the CONFIG_DB `VLAN`
table with `SubscriberStateTable`. For `SET`, it calls
`ProducerStateTable.set("Vlan100", [("vlanid", "100")])`; for `DEL`, it calls
`ProducerStateTable.delete("Vlan100")`.

Phase 3 is modeled by `vlanorch.py`. It consumes APPL_DB `VLAN_TABLE` updates
with `ConsumerStateTable.pop()`, then queues ASIC_DB updates with
`ProducerTable`.

Phase 4 is modeled by `syncd.py`. It consumes ASIC_DB
`ASIC_STATE:SAI_OBJECT_TYPE_VLAN` updates with `ConsumerTable.pop()`, prints
the update, and logs a fake ASIC write.

All four scripts log DB table reads and writes to
`/var/run/redis/vlan_table_db.log` by default. Override it with `--log-file`.

Start the local Redis and runner as shown above, then use four terminals:

Terminal 1, tiny syncd:

```bash
cd /home/ubuntu/swss-common-example
docker compose run --rm \
  --entrypoint python3 \
  runner \
  src/vlan_table/syncd.py \
  --vlan-id 100 \
  --watch
```

Terminal 2, tiny VlanOrch:

```bash
cd /home/ubuntu/swss-common-example
docker compose run --rm \
  --entrypoint python3 \
  runner \
  src/vlan_table/vlanorch.py \
  --vlan-id 100 \
  --watch
```

Terminal 3, tiny vlanmgrd:

```bash
cd /home/ubuntu/swss-common-example
docker compose run --rm \
  --entrypoint python3 \
  runner \
  src/vlan_table/vlanmgrd.py \
  --vlan-id 100 \
  --watch
```

Terminal 4, emulate `config vlan add 100`:

```bash
cd /home/ubuntu/swss-common-example
docker compose run --rm \
  --entrypoint python3 \
  runner \
  src/vlan_table/config_vlan_command.py \
  add 100
```

Then emulate `config vlan del 100`:

```bash
docker compose run --rm \
  --entrypoint python3 \
  runner \
  src/vlan_table/config_vlan_command.py \
  del 100
```

Useful Redis checks:

```bash
docker exec database redis-cli -s /var/run/redis/redis.sock -n 4 HGETALL 'VLAN|Vlan100'
docker exec database redis-cli -s /var/run/redis/redis.sock -n 0 HGETALL '_VLAN_TABLE:Vlan100'
docker exec database redis-cli -s /var/run/redis/redis.sock -n 0 SMEMBERS 'VLAN_TABLE_KEY_SET'
docker exec database redis-cli -s /var/run/redis/redis.sock -n 0 HGETALL 'VLAN_TABLE:Vlan100'
docker exec database redis-cli -s /var/run/redis/redis.sock -n 1 LRANGE 'ASIC_STATE:SAI_OBJECT_TYPE_VLAN_KEY_VALUE_OP_QUEUE' 0 -1
docker exec database redis-cli -s /var/run/redis/redis.sock -n 1 HGETALL 'ASIC_STATE:SAI_OBJECT_TYPE_VLAN:oid:0x26000000000100'
cat /var/run/redis/vlan_table_db.log
```

The expected materialization result is:

```text
CONFIG_DB VLAN|Vlan100: materialized by the Table writer in config_vlan_command.py.
APPL_DB VLAN_TABLE:Vlan100: materialized by the ConsumerStateTable reader in vlanorch.py.
ASIC_DB ASIC_STATE:SAI_OBJECT_TYPE_VLAN:oid:...: materialized by the ConsumerTable reader in syncd.py.
```
