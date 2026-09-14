#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
app_path="$repo_root/apps/desktop/src-tauri/target/release/bundle/macos/Rivet.app"
launch_root=$(mktemp -d "${TMPDIR:-/tmp}/rivet-tauri-launch.XXXXXX")
app_pid=
cleanup() {
    if [ -n "${app_pid:-}" ] && kill -0 "$app_pid" 2>/dev/null; then
        kill -TERM "$app_pid" 2>/dev/null || true
        sleep 1
        kill -KILL "$app_pid" 2>/dev/null || true
        wait "$app_pid" 2>/dev/null || true
    fi
    rm -rf -- "$launch_root"
}
trap cleanup EXIT HUP INT TERM
umask 077

if [ "$(uname -s)" != "Darwin" ]; then
    printf '%s\n' "Tauri macOS bundle smoke refused: this check requires macOS" >&2
    exit 1
fi

(cd "$repo_root/apps/desktop" && npm run tauri -- build --no-sign --bundles app)

test -d "$app_path"
test -x "$app_path/Contents/MacOS/rivet-desktop"
test "$(plutil -extract CFBundleIdentifier raw "$app_path/Contents/Info.plist")" = "com.othmaneblial.rivet"
test "$(plutil -extract CFBundleShortVersionString raw "$app_path/Contents/Info.plist")" = "0.1.0"

"$app_path/Contents/MacOS/rivet-desktop" >"$launch_root/rivet.log" 2>&1 &
app_pid=$!
engine_ready=0
attempt=0
while [ "$attempt" -lt 40 ]; do
    if lsof -nP -a -p "$app_pid" -iTCP -sTCP:LISTEN 2>/dev/null | rg -q '127\.0\.0\.1:'; then
        engine_ready=1
        break
    fi
    if ! kill -0 "$app_pid" 2>/dev/null; then
        break
    fi
    sleep 0.25
    attempt=$((attempt + 1))
done
if [ "$engine_ready" -ne 1 ]; then
    sed -n '1,80p' "$launch_root/rivet.log" >&2 || true
    printf '%s\n' "Tauri macOS bundle smoke refused: packaged engine did not open a loopback listener" >&2
    exit 1
fi

# A loopback listener alone can be provided by a headless process. Confirm
# that the packaged Tauri host also created the operator-facing window.
if ! window_name=$(osascript 2>"$launch_root/window-error.log" <<'APPLESCRIPT'
tell application "System Events"
    tell process "rivet-desktop"
        if (count of windows) = 0 then error "no Tauri window"
        return name of window 1
    end tell
end tell
APPLESCRIPT
); then
    printf '%s\n' "Tauri macOS bundle smoke refused: macOS could not inspect the application window." >&2
    printf '%s\n' "Grant Accessibility access to the shell/terminal running this check in System Settings > Privacy & Security > Accessibility, then retry." >&2
    sed -n '1,20p' "$launch_root/window-error.log" >&2 || true
    exit 1
fi
case "$window_name" in
    *Rivet*) ;;
    *)
        printf 'Tauri macOS bundle smoke refused: unexpected window title: %s\n' "$window_name" >&2
        exit 1
        ;;
esac

printf '%s\n' "local Tauri macOS bundle and launch smoke passed"
printf 'bundle: %s\n' "$app_path"
printf 'window: %s\n' "$window_name"
