# sonic-swss-common Table Usage In SONiC

This note summarizes how SONiC components use `sonic-swss-common` table
classes, Redis DBs, and Redis channels. It is based on the table behavior
observed in `~/sonic-swss` and the local examples in this repository.

The goal is to answer one question for each flow:

```text
who produces, which DB/table/channel is used, who consumes, and which
sonic-swss-common table class is involved?
```

This document intentionally focuses on Redis and `sonic-swss-common` mechanics.
It does not describe every SAI object, every vendor SDK call, or every SONiC
feature daemon.

## Mental Model

SONiC does not use one generic Redis pattern everywhere. Different DB paths use
different table contracts:

```text
CONFIG_DB
  direct durable config hashes
  producer: CLI/config tooling with Table
  consumer: mgrd/orch Consumer backed by SubscriberStateTable

APPL_DB
  desired-state publication
  producer: mgrd with ProducerStateTable
  consumer: orch with ConsumerStateTable

ASIC_DB SAI request path
  ordered SAI operation stream/object view
  producer: orch through sairedis, implemented over ProducerTable-like ASIC_DB operations
  consumer: syncd with ConsumerTable

ASIC_DB SAI response path
  response channel for sync SAI results
  producer: syncd with NotificationProducer
  consumer: sairedis client side with NotificationConsumer

ASIC_DB async notification path
  unsolicited SAI/vendor events
  producer: syncd with NotificationProducer on ASIC_DB:NOTIFICATIONS
  consumer: orch with NotificationConsumer

STATE_DB / APPL_STATE_DB / COUNTERS_DB
  direct state/stat hashes
  producer: feature-specific daemon with Table-style writes
  consumer: feature-specific daemon/CLI with Table or direct Redis reads
```

The most common mistake is to treat every DB as `ProducerTable` /
`ConsumerTable`. That is only correct for ordered operation streams such as the
ASIC request path and some traditional flex counter setup paths.

## Table Class Rules

| swss-common type | Main role | Typical DBs | Key behavior |
| --- | --- | --- | --- |
| `Table` | Direct hash read/write | `CONFIG_DB`, `STATE_DB`, `COUNTERS_DB`, `APPL_STATE_DB` | Producer writes the final hash directly. No queue is created. |
| `SubscriberStateTable` | Subscribe to direct table changes | `CONFIG_DB`, `STATE_DB` | Consumer sees keyspace updates from a direct table writer. |
| `ProducerStateTable` | Publish latest desired state | `APPL_DB` | Producer writes pending `_TABLE:key` plus key set. Same-key updates can coalesce. |
| `ConsumerStateTable` | Consume latest desired state | `APPL_DB` | Consumer pops pending state and materializes final `TABLE:key`. |
| `ProducerTable` | Ordered operation stream | `ASIC_DB`, traditional `FLEX_COUNTER_DB` | Producer appends operations to a queue. Every operation matters. |
| `ConsumerTable` | Consume ordered operation stream | `ASIC_DB` | Consumer pops queued operations and materializes final object table content. |
| `NotificationProducer` | Publish channel event | Response channels, `ASIC_DB:NOTIFICATIONS` | Producer sends a channel message, not a durable table row. |
| `NotificationConsumer` | Consume channel event | Response channels, `ASIC_DB:NOTIFICATIONS` | Consumer waits on a channel message, not a table pop. |

`orchagent` wraps many table consumers in its own `Consumer` executor. The
backend table class depends on the DB:

```text
CONFIG_DB / STATE_DB / CHASSIS_APP_DB
  -> Consumer(new SubscriberStateTable(...))

other DBs such as APPL_DB
  -> Consumer(new ConsumerStateTable(...))
```

So for CONFIG_DB rows, the precise consumer description is usually:

```text
<Orch class> / orch Consumer backed by SubscriberStateTable
```

## Configuration And Intent Flow

This is the north-to-south config path:

```text
CLI/config tooling
  -> CONFIG_DB
  -> mgrd
  -> APPL_DB
  -> orch
```

| Flow | Producer / Table | DB / Table | Consumer / Table |
| --- | --- | --- | --- |
| Config write | CLI/config tooling / `Table` | `CONFIG_DB:<table>|<key>` | mgrd or orch / `Consumer` backed by `SubscriberStateTable` |
| App desired state | mgrd / `ProducerStateTable` | `APPL_DB:<table>:<key>` pending `_TABLE:key` and `<TABLE>_KEY_SET` | orch / `ConsumerStateTable` |

For the VLAN example:

| Flow | Producer / Table | DB / Table | Consumer / Table |
| --- | --- | --- | --- |
| VLAN config | config command / `Table` | `CONFIG_DB:VLAN|Vlan100` | `vlanmgrd` / `SubscriberStateTable` or startup `Table.get` |
| VLAN app intent | `vlanmgrd` / `ProducerStateTable` | `APPL_DB:VLAN_TABLE:Vlan100` via pending `_VLAN_TABLE:Vlan100` | `PortsOrch` / `ConsumerStateTable` |

The real SONiC APPL_DB consumer for `VLAN_TABLE` is `PortsOrch`, not a separate
VLAN-specific orch class.

## SAI Request And Response

From orchagent code, a SAI operation looks like a local C call:

```cpp
sai_status_t status = sai_vlan_api->create_vlan(...);
```

In SONiC, orchagent loads the sairedis SAI implementation. That implementation
is a Redis-backed SAI proxy. It serializes the SAI operation into ASIC_DB so
`syncd` can invoke the real vendor SAI implementation.

Use this wording:

```text
sairedis is a Redis-backed SAI implementation/proxy.
```

Avoid this wording:

```text
sairedis is the vendor SDK.
sairedis is just a generic remote-call layer.
vendor SAI serializes Redis responses.
```

The request/response path is:

```text
orchagent / PortsOrch
  -> SAI API call through sairedis
  -> ASIC_DB request
  -> syncd
  -> real vendor SAI shared library
  -> vendor SDK / driver / firmware / ASIC
```

| Flow | Producer / Table | DB / Table / Channel | Consumer / Table |
| --- | --- | --- | --- |
| SAI object request | orchagent through sairedis / `ProducerTable`-style ASIC_DB operation | `ASIC_DB:ASIC_STATE:SAI_OBJECT_TYPE_*` | syncd / `ConsumerTable` |
| SAI sync response | syncd / `NotificationProducer` | ASIC DB response channel, for example `<table>_RESPONSE_CHANNEL` | sairedis client side / `NotificationConsumer` |

The vendor SAI shared library does not serialize the Redis response. The split
is:

```text
syncd calls vendor libsai
  -> vendor libsai returns sai_status_t and output data
  -> syncd/sairedis serialization code formats the Redis response
  -> NotificationProducer sends it on the ASIC DB response channel
```

`ASIC_DB:ASIC_STATE:*` is therefore both the SAI request transport and the Redis
view of ASIC object state. It is not the normal northbound interface for mgrd.

## Async Notifications

Async notifications are different from SAI request responses. They are
unsolicited events from the vendor SAI callback path.

```text
ASIC / vendor SAI callback
  -> syncd
  -> ASIC_DB:NOTIFICATIONS
  -> orchagent NotificationConsumer
  -> feature orch handler
  -> STATE_DB / internal state / feature-specific output
```

orchagent handles `ASIC_DB:NOTIFICATIONS` with
`swsscommon::NotificationConsumer`, not with a SAI API call.

| Flow | Producer / Table | DB / Table / Channel | Consumer / Table |
| --- | --- | --- | --- |
| Async event input | syncd / `NotificationProducer` | `ASIC_DB:NOTIFICATIONS` | orchagent feature orch / `NotificationConsumer` |
| Port state output | `PortsOrch` / `Table` | `STATE_DB:PORT_TABLE|<port>` | `portmgrd` / `Table`; `intfmgrd` and others / `SubscriberStateTable` |
| FDB event output | `FdbOrch` / `Table` | `STATE_DB:FDB_TABLE` | `fdbsyncd` / `SubscriberStateTable` |

Do not confuse these channels:

| Channel | Purpose |
| --- | --- |
| ASIC DB response channel | Response for a SAI request initiated by orchagent through sairedis. |
| `ASIC_DB:NOTIFICATIONS` | Unsolicited async SAI/vendor event from syncd to orchagent. |
| `APPL_DB_<table>_RESPONSE_CHANNEL` | Northbound orch response after APPL_DB intent processing. |

## Northbound State And Responses

After processing config or async events, orch may publish state northbound.
These are usually direct table writes or response channels, not ASIC_DB object
operations.

| Flow | Producer / Table | DB / Table / Channel | Consumer / Table |
| --- | --- | --- | --- |
| Operational state | orch / `Table` | `STATE_DB:<table>|<key>` | mgrd/sync daemon / `Table` or `SubscriberStateTable` |
| Applied APPL intent state | orch `ResponsePublisher` / `Table` | `APPL_STATE_DB:<table>:<key>` | mgrd/sync daemon / `Table` |
| APPL intent response | orch `ResponsePublisher` / `NotificationProducer` | `APPL_DB_<table>_RESPONSE_CHANNEL` | mgrd/sync daemon / `NotificationConsumer` |

`ResponsePublisher` is an orchagent helper. Its DB write side is `Table`; its
response channel side is `NotificationProducer`.

## Route Flow Counters

Route flow counters are the best example where the flow is not simply
`orch -> ASIC_DB -> syncd`. The feature uses CONFIG_DB for control,
FLEX_COUNTER_DB or sairedis extension attributes for polling setup, and
COUNTERS_DB for display data.

### Control And Setup Path

| Flow | Producer / Table | DB / Table | Consumer / Table |
| --- | --- | --- | --- |
| Enable route flow counter | CLI / `Table` | `CONFIG_DB:FLEX_COUNTER_TABLE|FLOW_CNT_ROUTE` | `FlexCounterOrch` / orch `Consumer` backed by `SubscriberStateTable` |
| Route pattern config | CLI / `Table` | `CONFIG_DB:FLOW_COUNTER_ROUTE_PATTERN_TABLE|<vrf>|<prefix>` | `FlowCounterRouteOrch` / orch `Consumer` backed by `SubscriberStateTable` |
| Polling setup, traditional mode | `FlowCounterRouteOrch` via `FlexCounterManager` / `ProducerTable` | `FLEX_COUNTER_DB:FLEX_COUNTER_TABLE:ROUTE_FLOW_COUNTER:<counter_oid>` field `FLOW_COUNTER_ID_LIST` | syncd flex counter logic |
| Route-to-counter mapping | `FlowCounterRouteOrch` / `Table` | `COUNTERS_DB:COUNTERS_ROUTE_NAME_MAP` | CLI/display / `Table` or direct Redis hash read |
| Route-to-pattern mapping | `FlowCounterRouteOrch` / `Table` | `COUNTERS_DB:COUNTERS_ROUTE_TO_PATTERN_MAP` | CLI/display / `Table` or direct Redis hash read |

The CONFIG_DB consumers are registered through `Orch::addConsumer()`, so their
backend table is `SubscriberStateTable`.

For traditional flex counter mode:

```text
FlowCounterRouteOrch
  -> FlexCounterManager::setCounterIdList()
  -> startFlexCounterPolling()
  -> gFlexCounterTable->set(...)
  -> ProducerTable(FLEX_COUNTER_DB, FLEX_COUNTER_TABLE)
```

The key/field are:

```text
FLEX_COUNTER_DB key:
  FLEX_COUNTER_TABLE:ROUTE_FLOW_COUNTER:<counter_oid>

field:
  FLOW_COUNTER_ID_LIST
```

In non-traditional flex counter mode, `FlexCounterManager` may configure syncd
through sairedis switch attributes instead:

```text
SAI_REDIS_SWITCH_ATTR_FLEX_COUNTER
SAI_REDIS_SWITCH_ATTR_FLEX_COUNTER_GROUP
```

That non-traditional path is not a direct `ProducerTable` write to
`FLEX_COUNTER_DB`.

### Polling And Display Path

The actual counter polling is done by syncd/flex counter logic, not by CLI and
not by a per-read orchagent SAI request.

```text
syncd flex counter logic
  -> vendor SAI get_counter_stats_ext or equivalent stats API
  -> COUNTERS_DB counter hash
  -> CLI/display reads COUNTERS_DB
```

| Flow | Producer / Table | DB / Table | Consumer / Table |
| --- | --- | --- | --- |
| Polled counter stats | syncd flex counter polling / `Table`-style hash write | `COUNTERS_DB:COUNTERS:<counter_oid>` | CLI/display / `Table` or direct Redis hash read |
| Route-to-counter mapping | `FlowCounterRouteOrch` / `Table` | `COUNTERS_DB:COUNTERS_ROUTE_NAME_MAP` | CLI/display / `Table` or direct Redis hash read |
| Route-to-pattern mapping | `FlowCounterRouteOrch` / `Table` | `COUNTERS_DB:COUNTERS_ROUTE_TO_PATTERN_MAP` | CLI/display / `Table` or direct Redis hash read |

`COUNTERS_DB` is direct state/stat storage. Do not model it as
`ProducerTable` / `ConsumerTable`.

## Quick Lookup

| Flow | Producer / Table | DB / Table / Channel | Consumer / Table |
| --- | --- | --- | --- |
| Config source to mgrd/orch | CLI/config / `Table` | `CONFIG_DB:*` | mgrd/orch `Consumer` backed by `SubscriberStateTable` |
| mgrd intent to orch | mgrd / `ProducerStateTable` | `APPL_DB:*` pending state | orch / `ConsumerStateTable` |
| orch SAI request to syncd | orch through sairedis / `ProducerTable`-style ASIC operation | `ASIC_DB:ASIC_STATE:*` | syncd / `ConsumerTable` |
| syncd SAI response to orch | syncd / `NotificationProducer` | ASIC DB response channel | sairedis client side / `NotificationConsumer` |
| syncd async event to orch | syncd / `NotificationProducer` | `ASIC_DB:NOTIFICATIONS` | orch / `NotificationConsumer` |
| orch state to mgrd | orch / `Table` | `STATE_DB:*` | mgrd / `Table` or `SubscriberStateTable` |
| orch APPL response to producer | orch / `NotificationProducer` | `APPL_DB_<table>_RESPONSE_CHANNEL` | mgrd/sync daemon / `NotificationConsumer` |
| route flow counter enable | CLI / `Table` | `CONFIG_DB:FLEX_COUNTER_TABLE|FLOW_CNT_ROUTE` | `FlexCounterOrch` / `Consumer` backed by `SubscriberStateTable` |
| route pattern config | CLI / `Table` | `CONFIG_DB:FLOW_COUNTER_ROUTE_PATTERN_TABLE|<vrf>|<prefix>` | `FlowCounterRouteOrch` / `Consumer` backed by `SubscriberStateTable` |
| route polling setup, traditional mode | `FlowCounterRouteOrch` via `FlexCounterManager` / `ProducerTable` | `FLEX_COUNTER_DB:FLEX_COUNTER_TABLE:ROUTE_FLOW_COUNTER:<counter_oid>` | syncd flex counter logic |
| counter mappings | `FlowCounterRouteOrch` / `Table` | `COUNTERS_DB:COUNTERS_ROUTE_NAME_MAP`, `COUNTERS_ROUTE_TO_PATTERN_MAP` | CLI/display / `Table` or direct Redis read |
| counter stats | syncd flex counter polling / `Table`-style hash write | `COUNTERS_DB:COUNTERS:<counter_oid>` | CLI/display / `Table` or direct Redis read |

## Summary Rules

- `CONFIG_DB` source rows are usually `Table` producer and
  `SubscriberStateTable` consumer.
- `APPL_DB` intent rows are usually `ProducerStateTable` producer and
  `ConsumerStateTable` consumer.
- SAI object programming uses sairedis as a Redis-backed SAI implementation:
  orch calls SAI, sairedis serializes into ASIC_DB, syncd calls real vendor
  SAI.
- SAI request responses use an ASIC DB response channel with
  `NotificationProducer` / `NotificationConsumer`.
- `ASIC_DB:NOTIFICATIONS` is for unsolicited async events and is consumed by
  orchagent with `NotificationConsumer`, not by the sairedis client API.
- Redis response serialization belongs to syncd/sairedis-side code after
  vendor SAI returns the SAI status/result.
- Traditional `FLEX_COUNTER_DB` setup can use `ProducerTable`; non-traditional
  flex counter setup can use sairedis switch attributes.
- `COUNTERS_DB` is direct state/stat storage and is normally accessed with
  `Table` or direct Redis hash reads/writes.
