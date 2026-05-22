# Custom CONFIG_DB and APPL_DB Tables

This is a minimal Python example for creating one custom table in `CONFIG_DB`
and one custom table in `APPL_DB`.

The flow is:

1. `config_db_producer.py` writes `CUSTOM_CONFIG_TABLE|demo` into `CONFIG_DB`.
2. `config_to_appl_bridge.py` reads that CONFIG_DB entry and writes
   `CUSTOM_APPL_TABLE:demo` into `APPL_DB`.

`swsscommon.Table` uses the database separator from `database_config.json`, so
CONFIG_DB keys use `|` and APPL_DB keys use `:`.

## Run

`sonic-swss-common` is only the client library. The Redis server for SONiC DBs
is normally owned by the SONiC `database` container, so the scripts also need:

- the Python `swsscommon` package available in the runtime environment
- `/var/run/redis/sonic-db/database_config.json`
- access to `/var/run/redis/redis.sock`, or TCP access to the Redis instance

On a SONiC switch or SONiC VS image, run these from a container/namespace that
has the Redis socket mounted. The scripts use the Unix socket by default.

```bash
cd /home/ubuntu/ows-example
python3 examples/custom_tables/config_db_producer.py --key demo --enabled true --interval 10
python3 examples/custom_tables/config_to_appl_bridge.py --key demo
```

If you intentionally run from an environment where Redis is reachable by TCP
according to `database_config.json`, add `--tcp`:

```bash
python3 examples/custom_tables/config_db_producer.py --tcp --key demo
python3 examples/custom_tables/config_to_appl_bridge.py --tcp --key demo
```

On SONiC, `redis-cli` can also be run through the `database` container when the
host environment does not have direct socket access:

```bash
docker exec database redis-cli -n 4 HGETALL 'CUSTOM_CONFIG_TABLE|demo'
docker exec database redis-cli -n 0 HGETALL 'CUSTOM_APPL_TABLE:demo'
```

To keep the bridge running and republish when config changes:

```bash
python3 examples/custom_tables/config_to_appl_bridge.py --key demo --watch
```

## Verify With redis-cli

```bash
redis-cli -n 4 HGETALL 'CUSTOM_CONFIG_TABLE|demo'
redis-cli -n 0 HGETALL 'CUSTOM_APPL_TABLE:demo'
```

Expected CONFIG_DB fields include:

```text
enabled
interval
updated_at
```

Expected APPL_DB fields include:

```text
admin_status
poll_interval
source_table
source_key
published_at
```
