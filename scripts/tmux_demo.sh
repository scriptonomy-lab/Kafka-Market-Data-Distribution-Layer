#!/usr/bin/env bash
# =============================================================================
# tmux_demo.sh — one terminal window showing the running CONTAINERS, laid out
# for the "everything running together" screenshots. It starts nothing: run
# `make up` (or `make up-dev`) first; this only attaches to what is running.
#
#   +------------------------+------------------------+
#   | producer (logs)        | pricing (live table)   |
#   +------------------------+------------------------+
#   | risk (live table)      | storage x N (logs)     |
#   +------------------------+------------------------+
#   | monitor (lag per topic, members, assignments)   |
#   +-------------------------------------------------+
#
# The status bar shows a live wall clock, so every screenshot is timestamped.
#
# The table panes use `docker attach --sig-proxy=false`: closing the pane or
# the session (Ctrl-b d to detach; `tmux kill-session -t tmdemo` to close)
# only ends the VIEW. The containers keep running; stop them with `make down`.
# =============================================================================
set -euo pipefail

SESSION=tmdemo
cd "$(dirname "$0")/.."            # run from the repo root

if tmux has-session -t "$SESSION" 2>/dev/null; then
  echo "Session $SESSION already exists: tmux attach -t $SESSION"
  exit 0
fi
for c in tm-producer tm-pricing tm-risk tm-monitor; do
  if [[ "$(docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null)" != "true" ]]; then
    echo "$c is not running: start the stack first (make up, or make up-dev for the simulator)"
    exit 1
  fi
done

ATTACH="docker attach --sig-proxy=false"
# Logs panes: compose prefixes each line with the container name, which is
# exactly what tells Storage replicas apart.
PRODUCER_CMD="docker compose logs -f --tail 15 producer"
STORAGE_CMD="docker compose logs -f --tail 15 storage"

tmux new-session -d -s "$SESSION" -n pipeline -x 230 -y 72
tmux set-option -w -t "$SESSION":0 remain-on-exit on
tmux set-option -t "$SESSION" status-interval 1
tmux set-option -t "$SESSION" status-right "#[bold]%Y-%m-%d %H:%M:%S %Z"
tmux set-option -t "$SESSION" status-right-length 40

tmux respawn-pane -k -t "$SESSION":0.0 "$PRODUCER_CMD"
tmux split-window -t "$SESSION":0.0 -h "$ATTACH tm-pricing"
tmux split-window -t "$SESSION":0.0 -v "$ATTACH tm-risk"
tmux split-window -t "$SESSION":0.2 -v "$STORAGE_CMD"
tmux split-window -t "$SESSION":0 -v -f -l 40% "$ATTACH tm-monitor"

if [[ "${NO_ATTACH:-0}" == "1" ]]; then
  echo "Started tmux session $SESSION in the background: tmux attach -t $SESSION"
else
  tmux attach -t "$SESSION"
fi
