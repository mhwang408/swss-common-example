#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

export UID
export GID="${GID:-$(id -g)}"

vlan_id="${1:-100}"
vlan_key="Vlan${vlan_id}"
asic_key="$(printf 'oid:0x2600000000%04d' "$vlan_id")"
run_id="$(date +%Y%m%d_%H%M%S)"
monitor_log="${MONITOR_LOG:-/tmp/swss_vlan_monitor_${run_id}.log}"
pretty_log="${PRETTY_LOG:-/tmp/swss_vlan_pretty_${run_id}.log}"

compose() {
    docker compose "$@"
}

redis() {
    docker exec database redis-cli -s /var/run/redis/redis.sock "$@"
}

show_cmd() {
    printf '\n### %s\n' "$*"
}

show_redis() {
    local title="$1"
    shift
    show_cmd "$title"
    redis "$@" | sed 's/^/  /'
}

run_runner() {
    compose run --rm --entrypoint python3 runner "$@"
}

stop_monitor() {
    if [[ -n "${monitor_pid:-}" ]] && kill -0 "$monitor_pid" 2>/dev/null; then
        kill "$monitor_pid" 2>/dev/null || true
        wait "$monitor_pid" 2>/dev/null || true
    fi
}
trap stop_monitor EXIT

show_cmd "Start database and clear DBs"
compose up -d database
redis -n 0 FLUSHDB >/dev/null
redis -n 1 FLUSHDB >/dev/null
redis -n 4 FLUSHDB >/dev/null

show_cmd "Start Redis MONITOR"
: > "$monitor_log"
docker exec database redis-cli -s /var/run/redis/redis.sock MONITOR >"$monitor_log" &
monitor_pid=$!
sleep 0.3
printf '  monitor log: %s\n' "$monitor_log"

show_cmd "1. config command writes CONFIG_DB"
run_runner src/vlan_table/config_vlan_command.py add "$vlan_id"
show_redis "CONFIG_DB final VLAN|${vlan_key}" -n 4 HGETALL "VLAN|${vlan_key}"
show_redis "APPL_DB final before vlanmgrd, should be empty" -n 0 HGETALL "VLAN_TABLE:${vlan_key}"

show_cmd "2. vlanmgrd reads CONFIG_DB and writes APPL_DB pending state"
run_runner src/vlan_table/vlanmgrd.py --vlan-id "$vlan_id"
show_redis "APPL_DB final after vlanmgrd, should still be empty" -n 0 HGETALL "VLAN_TABLE:${vlan_key}"
show_redis "APPL_DB pending hash after vlanmgrd" -n 0 HGETALL "_VLAN_TABLE:${vlan_key}"
show_redis "APPL_DB pending key set after vlanmgrd" -n 0 SMEMBERS "VLAN_TABLE_KEY_SET"

show_cmd "3. vlanorch consumes APPL_DB pending state and writes ASIC_DB queue"
run_runner src/vlan_table/vlanorch.py --vlan-id "$vlan_id"
show_redis "APPL_DB final after vlanorch" -n 0 HGETALL "VLAN_TABLE:${vlan_key}"
show_redis "APPL_DB pending key set after vlanorch, should be empty" -n 0 SMEMBERS "VLAN_TABLE_KEY_SET"
show_redis "ASIC_DB final after vlanorch, should still be empty" -n 1 HGETALL "ASIC_STATE:SAI_OBJECT_TYPE_VLAN:${asic_key}"
show_redis "ASIC_DB ProducerTable queue after vlanorch" -n 1 LRANGE "ASIC_STATE:SAI_OBJECT_TYPE_VLAN_KEY_VALUE_OP_QUEUE" 0 -1

show_cmd "4. syncd consumes ASIC_DB queue and materializes ASIC_DB final table"
run_runner src/vlan_table/syncd.py --vlan-id "$vlan_id"
show_redis "ASIC_DB final after syncd" -n 1 HGETALL "ASIC_STATE:SAI_OBJECT_TYPE_VLAN:${asic_key}"
show_redis "ASIC_DB queue after syncd, should be empty" -n 1 LRANGE "ASIC_STATE:SAI_OBJECT_TYPE_VLAN_KEY_VALUE_OP_QUEUE" 0 -1

sleep 0.3
stop_monitor

show_cmd "Pretty Redis MONITOR operations"
scripts/format_vlan_monitor.py "$monitor_log" | tee "$pretty_log" | sed 's/^/  /'

show_cmd "Conclusion"
cat <<EOF
  CONFIG_DB final VLAN|${vlan_key}: written by config command Table.set.
  APPL_DB final VLAN_TABLE:${vlan_key}: written by vlanorch ConsumerStateTable.pop.
  ASIC_DB final ASIC_STATE:SAI_OBJECT_TYPE_VLAN:${asic_key}: written by syncd ConsumerTable.pop.

  Full monitor log: $monitor_log
  Pretty monitor log: $pretty_log
EOF
