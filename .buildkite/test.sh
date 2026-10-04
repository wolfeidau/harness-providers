#!/usr/bin/env bash
set -euo pipefail

mkdir -p .buildkite/results
summary=.buildkite/results/tests.md
printf '## Go tests\n\n' > "$summary"

echo "--- Test"
status=0
style=success
# Preserve test failures while still publishing their summary.
if go test -v -count=1 -json -coverprofile=.buildkite/results/coverage.txt -covermode=atomic ./... \
  | tee .buildkite/results/tests.json \
  | go tool tparse -all -notests -format markdown >> "$summary"; then
  style=success
else
  status=$?
  style=error
fi

cat "$summary"
buildkite-agent annotate --context go-tests --style "$style" < "$summary"
exit "$status"
