#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

calculated_points=$(jq -r '[.workstreams[] | (.weight * .completion)] | add | ((.*100)|round)/100 | tostring' progress.json)
reported_points=$(jq -r '.weighted_evidence_points | tostring' progress.json)
reported_percent=$(jq -r '.verified_percent | tostring' progress.json)
readme_percent=$(sed -n 's/^\*\*\([0-9][0-9]*\)% verified.*/\1/p' README.md | sed -n '1p')
readme_points=$(sed -n 's/^Weighted evidence score: \*\*\([0-9][0-9]*\.[0-9][0-9]*\) \/ 100.*/\1/p' README.md | sed -n '1p')

[ "$calculated_points" = "$reported_points" ]
[ "$readme_points" = "$reported_points" ]
[ "$readme_percent" = "$reported_percent" ]
[ ! -d .github/workflows ]
git check-ignore -q base/jenkins
if rg -n 'base/' README.md ROADMAP.md >/dev/null 2>&1; then
    echo "progress check refused: internal base reference leaked into public documentation" >&2
    exit 1
fi

printf '%s\n' "progress check passed: $readme_percent% verified / $readme_points weighted points"
