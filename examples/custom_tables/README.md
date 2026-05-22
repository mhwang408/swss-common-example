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

Run on a SONiC image or an environment where `swsscommon` and Redis DB config
are available.

```bash
cd /home/ubuntu/ows-example
python3 examples/custom_tables/config_db_producer.py --key demo --enabled true --interval 10
python3 examples/custom_tables/config_to_appl_bridge.py --key demo
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
