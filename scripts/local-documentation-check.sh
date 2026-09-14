#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

test -s README.md
test -s ROADMAP.md
test -s LICENSE
test -s CONTRIBUTING.md
test -s SECURITY.md
test -s compat/jenkins-compatibility.json
jq empty compat/jenkins-compatibility.json
test -s assets/demo/rivet-desktop-demo.gif
test -s assets/demo/rivet-desktop-demo.mp4
test -s apps/desktop/src-tauri/icons/icon.svg

for path in ROADMAP.md CONTRIBUTING.md SECURITY.md LICENSE \
    compat/jenkins-compatibility.json \
    assets/demo/rivet-desktop-demo.gif \
    assets/demo/rivet-desktop-demo.mp4 \
    apps/desktop/src-tauri/icons/icon.svg; do
    case "$path" in
        assets/demo/rivet-desktop-demo.mp4) grep -Fq "rivet-desktop-demo.mp4" README.md ;;
        apps/desktop/src-tauri/icons/icon.svg) grep -Fq "src=\"apps/desktop/src-tauri/icons/icon.svg\"" README.md ;;
        assets/*|compat/*) grep -Fq "$path" README.md ;;
        *) grep -Fq "($path)" README.md ;;
    esac
done

if rg -n 'base/' README.md ROADMAP.md compat >/dev/null 2>&1; then
    echo "documentation check refused: internal path reference leaked into public project files" >&2
    exit 1
fi

./scripts/local-progress-check.sh
printf '%s\n' "local documentation check passed"
