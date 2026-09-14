#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
app_path="$repo_root/apps/desktop/src-tauri/target/release/bundle/macos/Rivet.app"

if [ "$(uname -s)" != "Darwin" ]; then
    printf '%s\n' "Tauri macOS bundle smoke refused: this check requires macOS" >&2
    exit 1
fi

(cd "$repo_root/apps/desktop" && npm run tauri -- build --no-sign --bundles app)

test -d "$app_path"
test -x "$app_path/Contents/MacOS/rivet-desktop"
test "$(plutil -extract CFBundleIdentifier raw "$app_path/Contents/Info.plist")" = "com.othmaneblial.rivet"
test "$(plutil -extract CFBundleShortVersionString raw "$app_path/Contents/Info.plist")" = "0.1.0"

printf '%s\n' "local Tauri macOS bundle smoke passed"
printf 'bundle: %s\n' "$app_path"
