#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

export UID
export GID="${GID:-$(id -g)}"

usage() {
    cat <<'EOF'
Usage:
  scripts/run_vlan_table_example.sh config-add [vlan_id] [args...]
  scripts/run_vlan_table_example.sh config-del [vlan_id] [args...]
  scripts/run_vlan_table_example.sh mgrd [args...]
  scripts/run_vlan_table_example.sh orch [args...]
  scripts/run_vlan_table_example.sh syncd [args...]
  scripts/run_vlan_table_example.sh verify [vlan_id]

Examples:
  scripts/run_vlan_table_example.sh config-add 100
  scripts/run_vlan_table_example.sh mgrd --vlan-id 100 --watch
  scripts/run_vlan_table_example.sh orch --vlan-id 100 --watch
  scripts/run_vlan_table_example.sh syncd --vlan-id 100 --watch
  scripts/run_vlan_table_example.sh verify 100
EOF
}

component="${1:-}"
if [[ -z "$component" || "$component" == "-h" || "$component" == "--help" ]]; then
    usage
    exit 0
fi
shift

case "$component" in
    config-add)
        vlan_id="${1:-100}"
        if [[ $# -gt 0 ]]; then
            shift
        fi
        target=(src/vlan_table/config_vlan_command.py add "$vlan_id")
        ;;
    config-del)
        vlan_id="${1:-100}"
        if [[ $# -gt 0 ]]; then
            shift
        fi
        target=(src/vlan_table/config_vlan_command.py del "$vlan_id")
        ;;
    mgrd)
        target=(src/vlan_table/vlanmgrd.py)
        ;;
    orch)
        target=(src/vlan_table/vlanorch.py)
        ;;
    syncd)
        target=(src/vlan_table/syncd.py)
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

docker compose up -d database >/dev/null
exec docker compose run --rm runner "${target[@]}" "$@"
