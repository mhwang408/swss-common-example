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

## Run

`sonic-swss-common` is only the client library. The Redis server for SONiC DBs
is normally owned by the SONiC `database` container, so the scripts also need:

- the Python `swsscommon` package available in the runtime environment
- `/var/run/redis/sonic-db/database_config.json`
- access to `/var/run/redis/redis.sock`

On a SONiC switch or SONiC VS image, run these from a container/namespace that
has the Redis socket mounted. The scripts use the Unix socket by default.

Terminal 1:

```bash
cd /home/ubuntu/ows-example
python3 examples/custom_tables/config_to_appl_bridge.py --key demo --watch
```

Terminal 2:

```bash
cd /home/ubuntu/ows-example
python3 examples/custom_tables/config_db_producer.py --key demo --enabled true --interval 10
```

## Run With Docker Only

You do not need to run the full SONiC stack for this minimal example. A normal
Redis container is enough because this example only uses Redis DB numbers and
SONiC table separators. The compose file also provides a `runner` image that
builds and installs `sonic-swss-common`, so the host does not need a Python venv
or native build dependencies.

Build the runner and start a local container named `database`:

```bash
cd /home/ubuntu/ows-example/examples/custom_tables
sudo mkdir -p /var/run/redis
docker compose build runner
docker compose up -d
```

The compose file bind-mounts host `/var/run/redis` into both `database` and
`runner`. Redis creates `/var/run/redis/redis.sock`, so the host and other
containers can use the same Unix socket. The `database` container also copies
the compose DB config into `/var/run/redis/sonic-db/database_config.json`, which
is the default path used by `swsscommon`. TCP is disabled in this local setup.

Then run the bridge in the runner container, using the shared Redis socket and
the compose DB config.

Terminal 1:

```bash
cd /home/ubuntu/ows-example
docker compose -f examples/custom_tables/docker-compose.yml run --rm runner \
  python3 examples/custom_tables/config_to_appl_bridge.py \
  --key demo \
  --watch
```

In another terminal, write CONFIG_DB:

Terminal 2:

```bash
cd /home/ubuntu/ows-example
docker compose -f examples/custom_tables/docker-compose.yml run --rm runner \
  python3 examples/custom_tables/config_db_producer.py \
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

If the host also has Python `swsscommon` installed, it can use the same socket:

```bash
python3 examples/custom_tables/config_db_producer.py --key demo
```

On SONiC, `redis-cli` can also be run through the `database` container when the
host environment does not have direct socket access:

```bash
docker exec database redis-cli -s /var/run/redis/redis.sock -n 4 HGETALL 'CUSTOM_CONFIG_TABLE|demo'
docker exec database redis-cli -s /var/run/redis/redis.sock -n 0 HGETALL '_CUSTOM_APPL_TABLE:demo'
```

To keep the bridge running and republish when config changes:

```bash
python3 examples/custom_tables/config_to_appl_bridge.py --key demo --watch
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
