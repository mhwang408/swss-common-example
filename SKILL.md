---
name: sonic-db-table-selection
description: Use when working on SONiC Redis DB flows, choosing between swsscommon Table, SubscriberStateTable, ProducerTable/ConsumerTable, and ProducerStateTable/ConsumerStateTable, or reasoning about CONFIG_DB to APPL_DB ownership, ordering, coalescing, and Redis Lua atomicity.
---

# SONiC DB Table Selection

Use this guidance when adding or reviewing code that reads/writes SONiC Redis
DB tables through `sonic-swss-common`.

## Core Model

Redis tables do not require pre-declaration. Table names are conventions and
contracts between producers and consumers. Keep project-local table names in
`ows_schema.h`; do not modify the `sonic-swss-common` submodule for local table
constants.

Use `sonic-swss-common/common/schema.h` constants for upstream SONiC tables.
Use `ows_schema.h` constants for this project's custom tables.

## Selection Rules

Use `Table` for direct DB hash access:

- Writing desired config into `CONFIG_DB`.
- Reading an entry or enumerating a table without queue semantics.
- Example: `config_db_producer.py` writes CONFIG_DB with `Table.set()`.

Use `SubscriberStateTable` for CONFIG_DB change consumption:

- A daemon needs to observe `CONFIG_DB` table updates.
- It should receive existing entries at startup and live keyspace events after
  subscription.
- Local Redis tests must enable keyspace notifications, for example
  `--notify-keyspace-events KEA`.

Use `ProducerStateTable` / `ConsumerStateTable` for APPL_DB desired state:

- The producer owns an APPL_DB table and publishes latest desired state.
- Consumers do not need every intermediate operation.
- Pending updates for the same key may coalesce.
- Same key + same field: last pending value wins.
- Same key + different fields: fields merge into one pending state.
- Different keys: no ordering guarantee.

Use `ProducerTable` / `ConsumerTable` for ordered operation streams:

- Every operation must be delivered and processed.
- Operation ordering matters.
- Do not use it when latest state per key is sufficient.

## Common SONiC Flow

For a simple custom pipeline:

```text
CONFIG_DB table A1 -> app1 -> APPL_DB table B1
```

Use:

```text
CONFIG_DB writer: Table
CONFIG_DB app reader: SubscriberStateTable
APPL_DB app writer: ProducerStateTable
APPL_DB downstream reader: ConsumerStateTable
```

Keep ownership boundaries explicit:

```text
A1 -> app1 -> B1
A2 -> app2 -> B2
```

If workflows have no shared input/output/side-effect tables, app-level mutual
exclusion is usually unnecessary. If two workflows share a table, key range, or
semantic resource, add a clear owner, central allocator, versioning, or explicit
lock.

## Producer/Consumer Semantics

`ProducerTable` producer:

```text
LPUSH <TABLE>_KEY_VALUE_OP_QUEUE key/value/op
PUBLISH <TABLE>_CHANNEL
```

`ConsumerTable` consumer:

```text
pop list in order
decode key/op/field-values
materialize final DB table when appropriate
```

This is an ordered operation log.

`ProducerStateTable` producer:

```text
HSET _<TABLE>:<key> field value
SADD <TABLE>_KEY_SET key
PUBLISH <TABLE>_CHANNEL
```

`ConsumerStateTable` consumer:

```text
SPOP <TABLE>_KEY_SET
HGETALL _<TABLE>:<key>
HSET/DEL <TABLE>:<key>
DEL _<TABLE>:<key>
```

This is an unordered changed-key set plus latest-state snapshot.

## Atomicity Rules

The producer/consumer table classes use Redis Lua to make multi-command Redis
operations atomic inside Redis. This avoids partial queue/state updates and
lost wakeups within those table operations.

Do not treat this as a workflow-level distributed lock. It does not protect a
multi-table flow such as:

```text
read CONFIG_DB A
read CONFIG_DB B
compute
write APPL_DB X
write APPL_DB Y
```

For workflow-level consistency, prefer single table ownership and convergence.
If strict consistency is required, add version checks, apply/commit markers,
or an explicit Redis lock that every writer respects.
