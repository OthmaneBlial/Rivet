#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
release_binary=${RIVET_COMPAT_BINARY:-$repo_root/target/release/rivet}

if [ ! -x "$release_binary" ]; then
    printf '%s\n' "compatibility capture smoke refused: release binary is missing: $release_binary" >&2
    exit 1
fi

compat_root=$(mktemp -d "${TMPDIR:-/tmp}/rivet-compat.XXXXXX")
server_pid=
mock_pid=
cleanup() {
    if [ -n "${server_pid:-}" ]; then
        kill "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
    fi
    if [ -n "${mock_pid:-}" ]; then
        kill "$mock_pid" 2>/dev/null || true
        wait "$mock_pid" 2>/dev/null || true
    fi
    rm -rf -- "$compat_root"
}
trap cleanup EXIT HUP INT TERM

umask 077
data_dir="$compat_root/.rivet"
project_name=rivet-compat

"$release_binary" --data-dir "$data_dir" project create "$project_name" --repository "$repo_root"
"$release_binary" --data-dir "$data_dir" run "$project_name" >/dev/null

server_port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
"$release_binary" --data-dir "$data_dir" server --bind "127.0.0.1:$server_port" >"$compat_root/rivet-server.log" 2>&1 &
server_pid=$!

ready=0
attempt=0
while [ "$attempt" -lt 40 ]; do
    if curl -fsS "http://127.0.0.1:$server_port/api/v1/ready" >/dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 0.25
    attempt=$((attempt + 1))
done
test "$ready" -eq 1

"$release_binary" compat capture-rivet "$project_name" \
    --server "http://127.0.0.1:$server_port" \
    --build 1 \
    --output "$compat_root/rivet.snapshot.json"

mock_port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
python3 - "$mock_port" >"$compat_root/jenkins-mock.log" 2>&1 <<'PY' &
import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = urlparse(self.path).path
        if path.endswith('/api/json'):
            payload = {
                'result': 'SUCCESS',
                'building': False,
                'actions': [],
                'artifacts': [],
            }
        elif path.endswith('/wfapi/describe'):
            payload = {
                'status': 'SUCCESS',
                'stages': [
                    {
                        'name': 'Validate',
                        'status': 'SUCCESS',
                        'steps': [
                            {'name': 'format', 'status': 'SUCCESS', 'exit_code': 0},
                            {'name': 'test', 'status': 'SUCCESS', 'exit_code': 0},
                        ],
                    }
                ],
            }
        else:
            self.send_error(404)
            return
        body = json.dumps(payload).encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass

HTTPServer(('127.0.0.1', int(sys.argv[1])), Handler).serve_forever()
PY
mock_pid=$!

mock_ready=0
attempt=0
while [ "$attempt" -lt 40 ]; do
    if curl -fsS "http://127.0.0.1:$mock_port/job/folder/job/service/7/api/json" >/dev/null 2>&1; then
        mock_ready=1
        break
    fi
    sleep 0.25
    attempt=$((attempt + 1))
done
test "$mock_ready" -eq 1

"$release_binary" compat capture-jenkins "folder/service" \
    --server "http://127.0.0.1:$mock_port" \
    --build 7 \
    --output "$compat_root/jenkins.snapshot.json"

"$release_binary" compat assemble \
    --scenario sequential-build-live-capture \
    --jenkins "$compat_root/jenkins.snapshot.json" \
    --rivet "$compat_root/rivet.snapshot.json" \
    --output "$compat_root/live.fixture.json"
"$release_binary" compat compare "$compat_root/live.fixture.json" >"$compat_root/report.json"
jq -e '.matches == true and (.differences | length) == 0' "$compat_root/report.json" >/dev/null
test "$(stat -f '%Lp' "$compat_root/live.fixture.json")" = 600

printf '%s\n' "local compatibility capture smoke passed"
