#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

export UID
export GID="${GID:-$(id -g)}"
run_pid=""
container_name="swss-common-example-custom-${component:-run}-$$"

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
container_name="swss-common-example-custom-${component}-$$"

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
