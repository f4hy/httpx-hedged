#!/usr/bin/env bash
# Installs deps, starts the demo FastAPI backend, runs the hedging demo
# client against it, and prints the stats report, then tears the server
# down. Run from anywhere:
#
#   ./examples/run_example.sh

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
REPO_ROOT="$(dirname -- "$SCRIPT_DIR")"
HOST=127.0.0.1
PORT=8000

cd "$REPO_ROOT"

echo "==> Installing dependencies (uv sync --group examples)"
uv sync --group examples

SERVER_PID=""
cleanup() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "==> Stopping demo server (pid $SERVER_PID)"
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

echo "==> Starting demo server on http://$HOST:$PORT"
uv run uvicorn examples.app:app --host "$HOST" --port "$PORT" --log-level warning &
SERVER_PID=$!

echo "==> Waiting for the server to come up"
for _ in $(seq 1 50); do
    if curl -s -o /dev/null "http://$HOST:$PORT/fast"; then
        break
    fi
    sleep 0.2
done

echo "==> Running example_usage.py"
uv run python examples/example_usage.py
