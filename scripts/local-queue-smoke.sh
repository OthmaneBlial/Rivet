#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
release_binary=${RIVET_QUEUE_SMOKE_BINARY:-$repo_root/target/release/rivet}

if [ ! -x "$release_binary" ]; then
    printf '%s\n' "local queue smoke refused: release binary is missing: $release_binary" >&2
    exit 1
fi

queue_root=$(mktemp -d "${TMPDIR:-/tmp}/rivet-queue.XXXXXX")
server_pid=
cleanup() {
    if [ -n "${server_pid:-}" ] && kill -0 "$server_pid" 2>/dev/null; then
        kill -TERM "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
    fi
    rm -rf -- "$queue_root"
}
trap cleanup EXIT HUP INT TERM
umask 077

data_dir="$queue_root/data"
repository="$queue_root/repository"
server_log="$queue_root/server.log"
mkdir -p "$repository"
printf '%s\n' \
    'version = 1' \
    'name = "queue-smoke"' \
    '' \
    '[[stages]]' \
    'name = "Queue"' \
    '' \
    '[[stages.steps]]' \
    'name = "wait"' \
    'program = "sh"' \
    'args = ["-c", "sleep 3; printf queue-ok"]' \
    > "$repository/Rivetfile.toml"
git -C "$repository" init -q -b main
git -C "$repository" config user.email rivet-queue-smoke@example.test
git -C "$repository" config user.name RivetQueueSmoke
git -C "$repository" add Rivetfile.toml
git -C "$repository" commit -qm "seed queue smoke repository"

server_port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
"$release_binary" --data-dir "$data_dir" server \
    --bind "127.0.0.1:$server_port" > "$server_log" 2>&1 &
server_pid=$!

attempt=0
while [ "$attempt" -lt 80 ]; do
    if curl -fsS "http://127.0.0.1:$server_port/api/v1/ready" >/dev/null 2>&1; then
        break
    fi
    attempt=$((attempt + 1))
    sleep 0.1
done
test "$attempt" -lt 80

project_payload=$(jq -cn \
    --arg name queue-smoke \
    --arg repository_path "$repository" \
    '{name: $name, repository_path: $repository_path}')
curl -fsS -X POST "http://127.0.0.1:$server_port/api/v1/projects" \
    -H 'content-type: application/json' \
    -d "$project_payload" >/dev/null

queue_build() {
    priority_value=$1
    build_payload=$(jq -cn --argjson priority "$priority_value" '{priority: $priority}')
    curl -fsS -X POST \
        "http://127.0.0.1:$server_port/api/v1/projects/queue-smoke/builds" \
        -H 'content-type: application/json' \
        -d "$build_payload"
}

first_response=$(queue_build 0)
first_number=$(printf '%s' "$first_response" | jq -r '.build.number')
attempt=0
first_state=
while [ "$attempt" -lt 80 ]; do
    first_state=$(curl -fsS \
        "http://127.0.0.1:$server_port/api/v1/projects/queue-smoke/builds/$first_number" \
        | jq -r '.build.status')
    if [ "$first_state" = "running" ]; then
        break
    fi
    attempt=$((attempt + 1))
    sleep 0.1
done
test "$first_state" = "running"

second_response=$(queue_build -10)
second_number=$(printf '%s' "$second_response" | jq -r '.build.number')
third_response=$(queue_build 10)
third_number=$(printf '%s' "$third_response" | jq -r '.build.number')

queue_items=$(curl -fsS "http://127.0.0.1:$server_port/api/v1/queue/items")
printf '%s\n' "$queue_items"
printf '%s' "$queue_items" | jq -e \
    'length == 2 and .[0].priority == 10 and .[1].priority == -10' >/dev/null

curl -fsS -X POST \
    "http://127.0.0.1:$server_port/api/v1/projects/queue-smoke/builds/$second_number/cancel" \
    -H 'content-type: application/json' -d '{}' >/dev/null

second_state=
attempt=0
while [ "$attempt" -lt 80 ]; do
    second_state=$(curl -fsS \
        "http://127.0.0.1:$server_port/api/v1/projects/queue-smoke/builds/$second_number" \
        | jq -r '.build.status')
    if [ "$second_state" = "cancelled" ]; then
        break
    fi
    attempt=$((attempt + 1))
    sleep 0.1
done
test "$second_state" = "cancelled"

wait_for_passed() {
    build_number=$1
    current_state=
    attempt=0
    while [ "$attempt" -lt 120 ]; do
        current_state=$(curl -fsS \
            "http://127.0.0.1:$server_port/api/v1/projects/queue-smoke/builds/$build_number" \
            | jq -r '.build.status')
        case "$current_state" in
            passed)
                return 0
                ;;
            failed|cancelled)
                printf 'local queue smoke: build #%s ended %s\n' "$build_number" "$current_state" >&2
                return 1
                ;;
        esac
        attempt=$((attempt + 1))
        sleep 0.1
    done
    printf 'local queue smoke: build #%s did not finish\n' "$build_number" >&2
    return 1
}

wait_for_passed "$first_number"
wait_for_passed "$third_number"
printf '%s\n' "local queue smoke passed"
