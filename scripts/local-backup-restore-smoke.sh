#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
release_binary=${RIVET_BACKUP_BINARY:-$repo_root/target/release/rivet}

if [ ! -x "$release_binary" ]; then
    printf '%s\n' "local backup smoke refused: release binary is missing: $release_binary" >&2
    exit 1
fi

backup_root=$(mktemp -d "${TMPDIR:-/tmp}/rivet-backup.XXXXXX")
cleanup() {
    rm -rf -- "$backup_root"
}
trap cleanup EXIT HUP INT TERM
umask 077

repo_dir="$backup_root/repo"
data_dir="$backup_root/data"
backup_dir="$backup_root/backup"
restored_dir="$backup_root/restored"
mkdir -p "$repo_dir"
printf '%s\n' \
    'version = 1' \
    'name = "backup"' \
    '' \
    '[[artifacts]]' \
    'name = "bundle"' \
    'paths = ["dist/**"]' \
    '' \
    '[[stages]]' \
    'name = "Build"' \
    '' \
    '[[stages.steps]]' \
    'name = "create-artifact"' \
    'program = "sh"' \
    'args = ["-c", "mkdir -p dist && printf backup-artifact > dist/result.txt"]' \
    > "$repo_dir/Rivetfile.toml"

"$release_binary" --data-dir "$data_dir" project create backup --repository "$repo_dir" >/dev/null
"$release_binary" --data-dir "$data_dir" run backup >/dev/null
"$release_binary" --data-dir "$data_dir" backup --output "$backup_dir"
jq -e '
    .format == "rivet-local-backup" and
    .version == 1 and
    .database_bytes > 0 and
    .database_sha256 and
    .artifact_files == 1 and
    .artifact_bytes > 0 and
    .includes_cache == false and
    .includes_credentials == false
' "$backup_dir/manifest.json" >/dev/null
test "$(stat -f '%Lp' "$backup_dir/manifest.json")" = 600

"$release_binary" restore --backup "$backup_dir" --target "$restored_dir" >/dev/null
"$release_binary" --data-dir "$restored_dir" builds backup > "$backup_root/builds.txt"
rg -Fq '#1	passed' "$backup_root/builds.txt"
"$release_binary" --data-dir "$restored_dir" artifacts backup --build 1 > "$backup_root/artifacts.txt"
rg -Fq 'bundle' "$backup_root/artifacts.txt"
rg -Fq 'dist/result.txt' "$backup_root/artifacts.txt"

if "$release_binary" restore --backup "$backup_dir" --target "$restored_dir" >/dev/null 2>&1; then
    printf '%s\n' 'local backup smoke refused: existing restore target was silently replaced' >&2
    exit 1
fi
"$release_binary" restore --backup "$backup_dir" --target "$restored_dir" --replace >/dev/null
previous_target=$(find "$backup_root" -type d -name '.restored.pre-restore-*' -print -quit)
test -n "$previous_target"

printf '%s\n' 'local backup restore smoke passed'
