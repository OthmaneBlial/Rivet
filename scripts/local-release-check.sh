#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
if [ -n "${RIVET_RELEASE_OUTPUT:-}" ]; then
    release_output=$RIVET_RELEASE_OUTPUT
else
    release_output=$(mktemp -d "${TMPDIR:-/tmp}/rivet-release.XXXXXX")
fi
release_version=${RIVET_RELEASE_VERSION:-0.1.0-alpha}
release_arch=$(uname -m)
bundle_path="$repo_root/apps/desktop/src-tauri/target/release/bundle/macos/Rivet.app"
desktop_archive="$release_output/Rivet-$release_version-macos-$release_arch.app.zip"

cd "$repo_root"

echo "[1/14] checking local progress and repository boundaries"
./scripts/local-progress-check.sh

echo "[2/14] checking Rust formatting"
cargo fmt --all -- --check

echo "[3/14] running the local Rust workspace tests"
cargo test --workspace

echo "[4/14] building the release CLI"
cargo build --release -p rivet

echo "[5/14] building the desktop client"
(cd apps/desktop && npm run build)

echo "[6/14] checking the native Tauri host"
cargo check --manifest-path apps/desktop/src-tauri/Cargo.toml

echo "[7/14] building the local Tauri macOS bundle"
./scripts/local-tauri-bundle-smoke.sh

mkdir -p "$release_output"
cp target/release/rivet "$release_output/rivet"
ditto -c -k --sequesterRsrc --keepParent "$bundle_path" "$desktop_archive"

if command -v shasum >/dev/null 2>&1; then
    binary_checksum=$(shasum -a 256 "$release_output/rivet" | awk '{print $1}')
elif command -v sha256sum >/dev/null 2>&1; then
    binary_checksum=$(sha256sum "$release_output/rivet" | awk '{print $1}')
else
    echo "release check refused: no SHA-256 utility is available" >&2
    exit 1
fi
if command -v shasum >/dev/null 2>&1; then
    desktop_checksum=$(shasum -a 256 "$desktop_archive" | awk '{print $1}')
else
    desktop_checksum=$(sha256sum "$desktop_archive" | awk '{print $1}')
fi

echo "[8/14] exercising the real local CLI workflow"
./scripts/local-e2e-smoke.sh

echo "[9/14] exercising the real server queue workflow"
./scripts/local-queue-smoke.sh

echo "[10/14] exercising local backup and restore"
./scripts/local-backup-restore-smoke.sh

echo "[11/14] exercising local user authentication"
./scripts/local-auth-smoke.sh

echo "[12/14] exercising live compatibility capture adapters"
./scripts/local-compat-capture-smoke.sh

echo "[13/15] exercising the permanent compatibility corpus"
./scripts/local-compat-corpus-smoke.sh

echo "[14/15] exercising local deployment hardening"
./scripts/local-deployment-smoke.sh

echo "[15/15] writing the local release manifest"
jq -n \
    --arg commit "$(git rev-parse HEAD)" \
    --arg generated_at "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    --arg binary "rivet" \
    --arg sha256 "$binary_checksum" \
    --arg desktop "$(basename "$desktop_archive")" \
    --arg desktop_sha256 "$desktop_checksum" \
    '{schema_version: 1, source_commit: $commit, generated_at: $generated_at,
      github_actions: false, checks: {format: true, workspace_tests: true,
      cli_release_build: true, desktop_web_build: true, tauri_host_check: true,
      tauri_bundle_smoke: true, local_e2e_smoke: true, local_queue_smoke: true, local_backup_restore_smoke: true, local_auth_smoke: true, compat_capture_smoke: true, compat_corpus_smoke: true,
      local_deployment_smoke: true},
      artifacts: [{name: $binary, path: $binary, sha256: $sha256},
        {name: $desktop, path: $desktop, sha256: $desktop_sha256}]}' \
    > "$release_output/manifest.json"
test -x "$release_output/rivet"
test -f "$desktop_archive"
test "$(jq -r '.artifacts[0].sha256' "$release_output/manifest.json")" = "$binary_checksum"
test "$(jq -r '.artifacts[1].sha256' "$release_output/manifest.json")" = "$desktop_checksum"

echo "release check passed"
echo "artifact: $release_output/rivet"
echo "artifact: $desktop_archive"
echo "manifest: $release_output/manifest.json"
