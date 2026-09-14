#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
release_binary=${RIVET_E2E_BINARY:-$repo_root/target/release/rivet}

if [ ! -x "$release_binary" ]; then
    printf '%s\n' "local e2e smoke refused: release binary is missing: $release_binary" >&2
    exit 1
fi

e2e_data_dir=$(mktemp -d "${TMPDIR:-/tmp}/rivet-e2e.XXXXXX")
cleanup() {
    rm -rf -- "$e2e_data_dir"
}
trap cleanup EXIT HUP INT TERM

data_dir="$e2e_data_dir/.rivet"
project_name=rivet-e2e
clone_dir="$e2e_data_dir/cloned-repository"

"$release_binary" scm clone "$repo_root" "$clone_dir" \
    --branch main --depth 1
test -f "$clone_dir/Rivetfile.toml"
test "$(git -C "$clone_dir" rev-parse --abbrev-ref HEAD)" = "main"

"$release_binary" --data-dir "$data_dir" project create "$project_name" --repository "$repo_root"
"$release_binary" --data-dir "$data_dir" run "$project_name"

builds_output=$("$release_binary" --data-dir "$data_dir" builds "$project_name")
printf '%s\n' "$builds_output"
printf '%s\n' "$builds_output" | rg -q '^#1[[:space:]]+passed[[:space:]]'

"$release_binary" --data-dir "$data_dir" inspect "$project_name" --build 1
"$release_binary" --data-dir "$data_dir" logs "$project_name" --build 1
printf '%s\n' "local end-to-end smoke passed"
