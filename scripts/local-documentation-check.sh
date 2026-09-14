#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

test -s README.md
test -s ROADMAP.md
test -s LICENSE
test -s CONTRIBUTING.md
test -s SECURITY.md
test -s SUPPORT.md
test -s CODE_OF_CONDUCT.md
test -s CHANGELOG.md
test -s CITATION.cff
test -s Makefile
test -s .github/pull_request_template.md
test -s .github/CODEOWNERS
test -s .github/ISSUE_TEMPLATE/bug_report.md
test -s .github/ISSUE_TEMPLATE/feature_request.md
test -s .github/ISSUE_TEMPLATE/config.yml
test -s compat/jenkins-compatibility.json
jq empty compat/jenkins-compatibility.json
test -s assets/demo/rivet-desktop-demo.gif
test -s assets/demo/rivet-desktop-demo.mp4
test -s apps/desktop/src-tauri/icons/icon.svg

for path in ROADMAP.md CONTRIBUTING.md SECURITY.md LICENSE CHANGELOG.md CITATION.cff \
    SUPPORT.md CODE_OF_CONDUCT.md \
    compat/jenkins-compatibility.json \
    assets/demo/rivet-desktop-demo.gif \
    assets/demo/rivet-desktop-demo.mp4 \
    apps/desktop/src-tauri/icons/icon.svg; do
    case "$path" in
        assets/demo/rivet-desktop-demo.mp4) grep -Fq "rivet-desktop-demo.mp4" README.md ;;
        apps/desktop/src-tauri/icons/icon.svg) grep -Fq "src=\"apps/desktop/src-tauri/icons/icon.svg\"" README.md ;;
        assets/*|compat/*) grep -Fq "$path" README.md ;;
        *) grep -Eq "\\($path\\)|href=\"$path\"" README.md ;;
    esac
done

grep -Fq "ROADMAP.md" .github/ISSUE_TEMPLATE/bug_report.md
grep -Fq "SECURITY.md" SUPPORT.md
grep -Fq "github.com/OthmaneBlial/Rivet/discussions" SUPPORT.md
grep -Fq "Security Advisory" .github/ISSUE_TEMPLATE/config.yml

if rg -n 'base/' README.md ROADMAP.md compat >/dev/null 2>&1; then
    echo "documentation check refused: internal path reference leaked into public project files" >&2
    exit 1
fi

./scripts/local-progress-check.sh
printf '%s\n' "local documentation check passed"
