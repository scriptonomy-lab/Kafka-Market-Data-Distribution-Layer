#!/usr/bin/env bash
# =============================================================================
# test_report.sh — `make test-report`: run lint, unit and integration tests
# and keep their full output as evidence in results/<stamp>-tests/.
#
# The integration tests need the stack's Kafka and TimescaleDB (`make up` or
# `make up-infra`). Every step runs even if an earlier one fails, so the
# report always shows all three results; the exit code is non-zero if any
# step failed. No secret is printed: the tests never read the API keys.
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")/.."

PY=.venv/bin/python
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT="results/${STAMP}-tests"
mkdir -p "$OUT"

{
  echo "test report ${STAMP}"
  echo "git commit: $(git rev-parse --short HEAD 2>/dev/null) $(git diff --quiet 2>/dev/null || echo '(working tree has changes)')"
  echo "host python: $($PY --version 2>&1)"
  echo "docker: $(docker --version 2>&1)"
  echo "images:"
  docker compose images --format json 2>/dev/null \
    | $PY -c "import json,sys; [print(f'  {i[\"Repository\"]}:{i[\"Tag\"]} ({i[\"ContainerName\"]})') for i in json.load(sys.stdin)]"
} > "$OUT/environment.txt"

status=0
$PY -m ruff check . > "$OUT/lint.txt" 2>&1 && $PY -m ruff format --check . >> "$OUT/lint.txt" 2>&1 || status=1
$PY -m pytest -v -m "not integration" --junitxml="$OUT/unit.xml" > "$OUT/unit.txt" 2>&1 || status=1
$PY -m pytest -v -m integration --junitxml="$OUT/integration.xml" > "$OUT/integration.txt" 2>&1 || status=1

{
  echo "# Test report ${STAMP}"
  echo
  echo "| Suite | Result |"
  echo "| --- | --- |"
  echo "| lint (ruff check + format) | $(tail -1 "$OUT/lint.txt") |"
  echo "| unit | $(tail -1 "$OUT/unit.txt" | sed 's/=//g' | xargs) |"
  echo "| integration (Kafka + TimescaleDB) | $(tail -1 "$OUT/integration.txt" | sed 's/=//g' | xargs) |"
  echo
  echo "Full output: lint.txt, unit.txt, integration.txt (JUnit XML alongside); environment.txt."
} > "$OUT/summary.md"
cat "$OUT/summary.md"
echo "written to $OUT"
exit $status
