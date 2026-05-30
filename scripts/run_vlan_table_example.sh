#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

export UID
export GID="${GID:-$(id -g)}"
run_pid=""
container_name="swss-common-example-vlan-${component:-run}-$$"

usage() {
    cat <<'EOF'
Usage:
  scripts/run_vlan_table_example.sh daemon [args...]
  scripts/run_vlan_table_example.sh config-add [vlan_id] [args...]
  scripts/run_vlan_table_example.sh config-del [vlan_id] [args...]
  scripts/run_vlan_table_example.sh mgrd [args...]
  scripts/run_vlan_table_example.sh portorch [args...]
  scripts/run_vlan_table_example.sh syncd [args...]
  scripts/run_vlan_table_example.sh verify [vlan_id]

Examples:
  scripts/run_vlan_table_example.sh daemon
  scripts/run_vlan_table_example.sh config-add 100
  scripts/run_vlan_table_example.sh verify 100
EOF
}

component="${1:-}"
if [[ -z "$component" || "$component" == "-h" || "$component" == "--help" ]]; then
    usage
    exit 0
fi
shift
container_name="swss-common-example-vlan-${component}-$$"

case "$component" in
    daemon)
        target=(src/swss/vlan_table/daemon.py)
        ;;
    config-add)
        vlan_id="${1:-100}"
        if [[ $# -gt 0 ]]; then
            shift
        fi
        target=(src/swss/vlan_table/config_vlan_command.py add "$vlan_id")
        ;;
    config-del)
        vlan_id="${1:-100}"
        if [[ $# -gt 0 ]]; then
            shift
        fi
        target=(src/swss/vlan_table/config_vlan_command.py del "$vlan_id")
        ;;
    mgrd)
        target=(src/swss/vlan_table/vlanmgrd.py)
        ;;
    portorch)
        target=(src/swss/vlan_table/portorch.py)
        ;;
    syncd)
        target=(src/swss/vlan_table/syncd.py)
        ;;
    verify)
        exec scripts/verify_vlan_flow.sh "${1:-100}"
        ;;
    *)
        printf 'Unknown component: %s\n\n' "$component" >&2
        usage >&2
        exit 2
        ;;
esac

cleanup() {
    if [[ -n "$run_pid" ]] && kill -0 "$run_pid" 2>/dev/null; then
        kill "$run_pid" 2>/dev/null || true
    fi
    docker rm -f "$container_name" >/dev/null 2>&1 || true
}

trap cleanup INT TERM EXIT

docker compose up -d database >/dev/null
docker compose run --rm --name "$container_name" -T runner "${target[@]}" "$@" &
run_pid=$!
wait "$run_pid"
