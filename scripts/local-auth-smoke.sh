#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
release_binary=${RIVET_AUTH_SMOKE_BINARY:-$repo_root/target/release/rivet}

if [ ! -x "$release_binary" ]; then
    printf '%s\n' "local auth smoke refused: release binary is missing: $release_binary" >&2
    exit 1
fi

auth_dir=$(mktemp -d "${TMPDIR:-/tmp}/rivet-auth.XXXXXX")
server_pid=""
cleanup() {
    if [ -n "$server_pid" ] && kill -0 "$server_pid" 2>/dev/null; then
        kill "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
    fi
    rm -rf -- "$auth_dir"
}
trap cleanup EXIT HUP INT TERM
umask 077

users_file="$auth_dir/users.json"
admin_password_file="$auth_dir/admin-password"
operator_password_file="$auth_dir/operator-password"
server_log="$auth_dir/server.log"
data_dir="$auth_dir/data"

write_password() {
    od -An -N24 -tx1 /dev/urandom | tr -d ' \n' > "$1"
    printf '\n' >> "$1"
}

write_password "$admin_password_file"
write_password "$operator_password_file"
admin_password=$(tr -d '\r\n' < "$admin_password_file")
operator_password=$(tr -d '\r\n' < "$operator_password_file")

"$release_binary" auth user create admin@example.test \
    --role admin --users-file "$users_file" --password-file "$admin_password_file"
"$release_binary" auth user create operator@example.test \
    --role operator --project rivet --users-file "$users_file" \
    --password-file "$operator_password_file"

user_list=$("$release_binary" auth user list --users-file "$users_file")
printf '%s\n' "$user_list" | rg -q 'admin@example\.test[[:space:]]+admin[[:space:]]+\*[[:space:]]+active'
printf '%s\n' "$user_list" | rg -q 'operator@example\.test[[:space:]]+operator[[:space:]]+rivet[[:space:]]+active'

"$release_binary" auth user disable operator@example.test --users-file "$users_file"
user_list=$("$release_binary" auth user list --users-file "$users_file")
printf '%s\n' "$user_list" | rg -q 'operator@example\.test[[:space:]]+operator[[:space:]]+rivet[[:space:]]+disabled'
"$release_binary" auth user enable operator@example.test --users-file "$users_file"

write_password "$admin_password_file"
admin_password=$(tr -d '\r\n' < "$admin_password_file")
"$release_binary" auth user password admin@example.test \
    --users-file "$users_file" --password-file "$admin_password_file"
if rg -F -q "$admin_password" "$users_file"; then
    printf '%s\n' "local auth smoke refused: plaintext password reached users file" >&2
    exit 1
fi

auth_port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
"$release_binary" --data-dir "$data_dir" server \
    --bind "127.0.0.1:$auth_port" --auth-users-file "$users_file" \
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

login_body="$auth_dir/login.json"
curl -fsS -o "$login_body" -X POST \
    -H 'content-type: application/json' \
    "http://127.0.0.1:$auth_port/api/v1/auth/login" \
    --data "{\"username\":\"admin@example.test\",\"password\":\"$admin_password\"}"
session_token=$(jq -er '.session_token' "$login_body")
test -n "$session_token"
me_body="$auth_dir/me.json"
curl -fsS -o "$me_body" \
    -H "authorization: Bearer $session_token" \
    "http://127.0.0.1:$auth_port/api/v1/auth/me"
jq -e '.role == "admin" and (.local_mode == false)' "$me_body" >/dev/null

wrong_body="$auth_dir/wrong.json"
wrong_status=$(curl -sS -o "$wrong_body" -w '%{http_code}' -X POST \
    -H 'content-type: application/json' \
    "http://127.0.0.1:$auth_port/api/v1/auth/login" \
    --data "{\"username\":\"admin@example.test\",\"password\":\"wrong-password\"}")
test "$wrong_status" = "401"
jq -e '.error == "invalid username or password"' "$wrong_body" >/dev/null
if rg -F -q "$admin_password" "$server_log"; then
    printf '%s\n' "local auth smoke refused: plaintext password reached server logs" >&2
    exit 1
fi

printf '%s\n' "local authentication smoke passed"
