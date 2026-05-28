#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

export UID
export GID="${GID:-$(id -g)}"

vlan_id="${1:-100}"
vlan_key="Vlan${vlan_id}"
asic_key="$(printf 'oid:0x2600000000%04d' "$vlan_id")"
port_name="${PORT_NAME:-Ethernet0}"
run_id="$(date +%Y%m%d_%H%M%S)"
monitor_log="${MONITOR_LOG:-/tmp/swss_vlan_monitor_${run_id}.log}"
pretty_log="${PRETTY_LOG:-/tmp/swss_vlan_pretty_${run_id}.log}"
bg_containers=()

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
    compose run --rm runner "$@"
}

run_runner_bg() {
    local name="$1"
    shift
    bg_containers+=("$name")
    compose run --rm --name "$name" -T runner "$@" &
}

stop_monitor() {
    if [[ -n "${monitor_pid:-}" ]] && kill -0 "$monitor_pid" 2>/dev/null; then
        kill "$monitor_pid" 2>/dev/null || true
        wait "$monitor_pid" 2>/dev/null || true
    fi
}

cleanup() {
    stop_monitor
    for name in "${bg_containers[@]}"; do
        docker rm -f "$name" >/dev/null 2>&1 || true
    done
}
trap cleanup EXIT

show_cmd "Start database and clear DBs"
compose up -d database
redis -n 0 FLUSHDB >/dev/null
redis -n 1 FLUSHDB >/dev/null
redis -n 4 FLUSHDB >/dev/null
redis -n 6 FLUSHDB >/dev/null

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

show_cmd "3. Start syncd, then portorch consumes APPL_DB and waits for SAI response"
syncd_name="swss-common-example-verify-syncd-$$"
run_runner_bg "$syncd_name" src/vlan_table/syncd.py --vlan-id "$vlan_id"
sleep 0.5
run_runner src/vlan_table/portorch.py --vlan-id "$vlan_id" --wait-sai-response
show_redis "APPL_DB final after portorch" -n 0 HGETALL "VLAN_TABLE:${vlan_key}"
show_redis "APPL_DB pending key set after portorch, should be empty" -n 0 SMEMBERS "VLAN_TABLE_KEY_SET"
show_redis "ASIC_DB final after syncd" -n 1 HGETALL "ASIC_STATE:SAI_OBJECT_TYPE_VLAN:${asic_key}"
show_redis "ASIC_DB queue after syncd, should be empty" -n 1 LRANGE "ASIC_STATE:SAI_OBJECT_TYPE_VLAN_KEY_VALUE_OP_QUEUE" 0 -1

show_cmd "4. Async notification: syncd -> ASIC_DB:NOTIFICATIONS -> portorch -> STATE_DB -> vlanmgrd"
vlanmgrd_state_name="swss-common-example-verify-vlanmgrd-state-$$"
run_runner_bg "$vlanmgrd_state_name" src/vlan_table/vlanmgrd.py --state-port "$port_name" --watch
sleep 0.5
portorch_notify_name="swss-common-example-verify-portorch-notify-$$"
run_runner_bg "$portorch_notify_name" src/vlan_table/portorch.py --notification-only --port "$port_name"
sleep 0.5
run_runner src/vlan_table/syncd.py --send-port-notification --port "$port_name" --oper-status ok
sleep 0.5
show_redis "STATE_DB port state after async notification" -n 6 HGETALL "PORT_TABLE|${port_name}"

sleep 0.3
stop_monitor

show_cmd "Pretty Redis MONITOR operations"
scripts/format_vlan_monitor.py "$monitor_log" | tee "$pretty_log" | sed 's/^/  /'

show_cmd "Conclusion"
cat <<EOF
  CONFIG_DB final VLAN|${vlan_key}: written by config command Table.set.
  APPL_DB final VLAN_TABLE:${vlan_key}: written by portorch ConsumerStateTable.pop.
  ASIC_DB final ASIC_STATE:SAI_OBJECT_TYPE_VLAN:${asic_key}: written by syncd ConsumerTable.pop.
  SAI response channel: syncd NotificationProducer -> portorch NotificationConsumer.
  Async notification path: syncd ASIC_DB:NOTIFICATIONS -> portorch -> STATE_DB PORT_TABLE -> vlanmgrd.

  Full monitor log: $monitor_log
  Pretty monitor log: $pretty_log
EOF
