#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
release_binary=${RIVET_COMPAT_CORPUS_BINARY:-$repo_root/target/release/rivet}

if [ ! -x "$release_binary" ]; then
    printf '%s\n' "compatibility corpus smoke refused: release binary is missing: $release_binary" >&2
    exit 1
fi

fixture_count=0
for fixture in "$repo_root"/compat/fixtures/*.json; do
    test -f "$fixture"
    "$release_binary" compat compare "$fixture" >/dev/null
    fixture_count=$((fixture_count + 1))
done

test "$fixture_count" -ge 4
printf 'local compatibility corpus smoke passed: %s fixtures\n' "$fixture_count"
