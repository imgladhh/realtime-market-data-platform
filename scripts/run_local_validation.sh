#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python_bin="${repo_dir}/venv/bin/python"

if [[ ! -x "${python_bin}" ]]; then
  echo "Missing project interpreter: ${python_bin}" >&2
  exit 1
fi

pids=()
cleanup() {
  if ((${#pids[@]})); then
    kill "${pids[@]}" 2>/dev/null || true
    wait "${pids[@]}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

cd "${repo_dir}"
validation_started_ms="$(date +%s%3N)"
validation_namespace="validation-${validation_started_ms}"
echo "Using isolated Redis namespace: ${validation_namespace}"

MARKET_DATA_NAMESPACE="${validation_namespace}" \
  "${python_bin}" -m src.engine.engine >/tmp/market-data-engine.log 2>&1 &
pids+=("$!")
"${python_bin}" -m src.storage.tick_writer >/tmp/market-data-writer.log 2>&1 &
pids+=("$!")
MARKET_DATA_NAMESPACE="${validation_namespace}" \
  "${python_bin}" -m src.gateway.gateway >/tmp/market-data-gateway.log 2>&1 &
pids+=("$!")

for _ in {1..30}; do
  if curl --fail --silent http://localhost:8000/health >/dev/null; then
    break
  fi
  sleep 0.5
done
if ! curl --fail --silent http://localhost:8000/health >/dev/null; then
  echo "Gateway did not become ready" >&2
  tail -30 /tmp/market-data-gateway.log || true
  exit 1
fi

"${python_bin}" -m src.feed.simulator >/tmp/market-data-feed.log 2>&1 &
pids+=("$!")
sleep 5

if ! "${python_bin}" -m src.benchmark.load_bench; then
  for component in engine writer gateway feed; do
    echo "--- ${component} log ---"
    tail -30 "/tmp/market-data-${component}.log" || true
  done
  exit 1
fi

echo "--- history response ---"
curl --fail --silent --show-error \
  "http://localhost:8000/history/AAPL?from_ts=${validation_started_ms}&to_ts=253402300799999&limit=3"
echo
