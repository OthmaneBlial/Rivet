#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
release_binary=${RIVET_DEPLOYMENT_SMOKE_BINARY:-$repo_root/target/release/rivet}

if [ ! -x "$release_binary" ]; then
    printf '%s\n' "local deployment smoke refused: release binary is missing: $release_binary" >&2
    exit 1
fi

deployment_dir=$(mktemp -d "${TMPDIR:-/tmp}/rivet-deployment.XXXXXX")
server_pid=""
cleanup() {
    if [ -n "$server_pid" ] && kill -0 "$server_pid" 2>/dev/null; then
        kill -TERM "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
    fi
    rm -rf -- "$deployment_dir"
}
trap cleanup EXIT HUP INT TERM
umask 077

data_dir="$deployment_dir/data"
token_file="$deployment_dir/server.token"
server_log="$deployment_dir/server.log"
health_headers="$deployment_dir/health.headers"
fallback_headers="$deployment_dir/fallback.headers"
health_body="$deployment_dir/health.json"
projects_body="$deployment_dir/projects.json"
unauthorized_body="$deployment_dir/unauthorized.json"
public_bind_log="$deployment_dir/public-bind.log"

printf 'local-deployment-token-%s\n' "$(od -An -N12 -tx1 /dev/urandom | tr -d ' \n')" > "$token_file"
token=$(tr -d '\r\n' < "$token_file")

auth_port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
RUST_LOG=info "$release_binary" --data-dir "$data_dir" server \
    --bind "127.0.0.1:$auth_port" --token-file "$token_file" \
    > "$server_log" 2>&1 &
server_pid=$!

attempt=0
while [ "$attempt" -lt 80 ]; do
    if curl -fsS "http://127.0.0.1:$auth_port/api/v1/health" >/dev/null 2>&1; then
        break
    fi
    attempt=$((attempt + 1))
    sleep 0.1
done
test "$attempt" -lt 80

curl -fsS -D "$health_headers" -o "$health_body" \
    -H 'origin: http://127.0.0.1:1420' \
    -H 'x-request-id: local-deployment-health' \
    "http://127.0.0.1:$auth_port/api/v1/health"
jq -e '.status == "ok" and .service == "rivet-server"' "$health_body" >/dev/null
rg -Fiq 'cache-control: no-store' "$health_headers"
rg -Fiq 'x-content-type-options: nosniff' "$health_headers"
rg -Fiq 'referrer-policy: no-referrer' "$health_headers"
rg -Fiq 'x-frame-options: DENY' "$health_headers"
rg -Fiq 'x-request-id: local-deployment-health' "$health_headers"
rg -Fiq 'access-control-allow-origin: http://127.0.0.1:1420' "$health_headers"

curl -fsS -D "$fallback_headers" -o /dev/null \
    -H 'origin: http://127.0.0.1:1421' \
    "http://127.0.0.1:$auth_port/api/v1/health"
rg -Fiq 'access-control-allow-origin: http://127.0.0.1:1421' "$fallback_headers"

curl -fsS "http://127.0.0.1:$auth_port/api/v1/ready" | jq -e \
    '.status == "ready" and .service == "rivet-server" and .storage == "ok"' >/dev/null

unauthorized_status=$(curl -sS -o "$unauthorized_body" -w '%{http_code}' \
    "http://127.0.0.1:$auth_port/api/v1/projects")
test "$unauthorized_status" = "401"
jq -e '.error == "authentication required"' "$unauthorized_body" >/dev/null

curl -fsS -o "$projects_body" \
    -H "authorization: Bearer $token" \
    "http://127.0.0.1:$auth_port/api/v1/projects"
jq -e 'type == "array"' "$projects_body" >/dev/null

if rg -a -F -q "$token" "$server_log"; then
    printf '%s\n' "local deployment smoke refused: token reached server logs" >&2
    exit 1
fi
if rg -a -F -q "$token" "$data_dir" 2>/dev/null; then
    printf '%s\n' "local deployment smoke refused: token reached persisted server data" >&2
    exit 1
fi

kill -TERM "$server_pid"
attempt=0
while kill -0 "$server_pid" 2>/dev/null && [ "$attempt" -lt 80 ]; do
    attempt=$((attempt + 1))
    sleep 0.1
done
if kill -0 "$server_pid" 2>/dev/null; then
    printf '%s\n' "local deployment smoke refused: server did not stop after SIGTERM" >&2
    exit 1
fi
wait "$server_pid"
server_pid=""
rg -Fq 'Rivet server shutdown requested' "$server_log"

public_port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
if "$release_binary" --data-dir "$deployment_dir/public-data" server \
    --bind "0.0.0.0:$public_port" > "$public_bind_log" 2>&1; then
    printf '%s\n' "local deployment smoke refused: unauthenticated public bind was accepted" >&2
    exit 1
fi
rg -Fq 'AuthRequired' "$public_bind_log"

printf '%s\n' "local deployment smoke passed"
