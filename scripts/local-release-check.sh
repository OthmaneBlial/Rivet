#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
if [ -n "${RIVET_RELEASE_OUTPUT:-}" ]; then
    release_output=$RIVET_RELEASE_OUTPUT
else
    release_output=$(mktemp -d "${TMPDIR:-/tmp}/rivet-release.XXXXXX")
fi

cd "$repo_root"

echo "[1/11] checking local progress and repository boundaries"
./scripts/local-progress-check.sh

echo "[2/11] checking Rust formatting"
cargo fmt --all -- --check

echo "[3/11] running the local Rust workspace tests"
cargo test --workspace

echo "[4/11] building the release CLI"
cargo build --release -p rivet

echo "[5/11] building the desktop client"
(cd apps/desktop && npm run build)

echo "[6/11] checking the native Tauri host"
cargo check --manifest-path apps/desktop/src-tauri/Cargo.toml

mkdir -p "$release_output"
cp target/release/rivet "$release_output/rivet"

if command -v shasum >/dev/null 2>&1; then
    binary_checksum=$(shasum -a 256 "$release_output/rivet" | awk '{print $1}')
elif command -v sha256sum >/dev/null 2>&1; then
    binary_checksum=$(sha256sum "$release_output/rivet" | awk '{print $1}')
else
    echo "release check refused: no SHA-256 utility is available" >&2
    exit 1
fi

echo "[7/11] exercising the real local CLI workflow"
./scripts/local-e2e-smoke.sh

echo "[8/11] exercising local user authentication"
./scripts/local-auth-smoke.sh

echo "[9/11] exercising live compatibility capture adapters"
./scripts/local-compat-capture-smoke.sh

echo "[10/11] exercising local deployment hardening"
./scripts/local-deployment-smoke.sh

echo "[11/11] writing the local release manifest"
jq -n \
    --arg commit "$(git rev-parse HEAD)" \
    --arg generated_at "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    --arg binary "rivet" \
    --arg sha256 "$binary_checksum" \
    '{schema_version: 1, source_commit: $commit, generated_at: $generated_at,
      github_actions: false, checks: {format: true, workspace_tests: true,
      cli_release_build: true, desktop_web_build: true, tauri_host_check: true,
      local_e2e_smoke: true, local_auth_smoke: true, compat_capture_smoke: true,
      local_deployment_smoke: true},
      artifacts: [{name: $binary, path: $binary, sha256: $sha256}]}' \
    > "$release_output/manifest.json"
test -x "$release_output/rivet"
test "$(jq -r '.artifacts[0].sha256' "$release_output/manifest.json")" = "$binary_checksum"

echo "release check passed"
echo "artifact: $release_output/rivet"
echo "manifest: $release_output/manifest.json"
