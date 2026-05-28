#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

export UID
export GID="${GID:-$(id -g)}"

usage() {
    cat <<'EOF'
Usage:
  scripts/run_custom_tables_example.sh producer [args...]
  scripts/run_custom_tables_example.sh bridge [args...]

Examples:
  scripts/run_custom_tables_example.sh bridge --key demo --watch
  scripts/run_custom_tables_example.sh producer --key demo --enabled true --interval 10
EOF
}

component="${1:-}"
if [[ -z "$component" || "$component" == "-h" || "$component" == "--help" ]]; then
    usage
    exit 0
fi
shift

case "$component" in
    producer)
        target=(src/custom_tables/config_db_producer.py)
        ;;
    bridge)
        target=(src/custom_tables/config_to_appl_bridge.py)
        ;;
    *)
        printf 'Unknown component: %s\n\n' "$component" >&2
        usage >&2
        exit 2
        ;;
esac

docker compose up -d database >/dev/null
exec docker compose run --rm runner "${target[@]}" "$@"
