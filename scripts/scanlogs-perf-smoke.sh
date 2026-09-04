#!/usr/bin/env bash

set -Eeuo pipefail

: "${SCANLOGS_CASE_MANIFEST:?SCANLOGS_CASE_MANIFEST is required}"
: "${SCANLOGS_PERF_OUTPUT_DIR:?SCANLOGS_PERF_OUTPUT_DIR is required}"
: "${SCANLOGS_CFX_RPC:?SCANLOGS_CFX_RPC is required}"
: "${SCANLOGS_ETH_RPC:?SCANLOGS_ETH_RPC is required}"
: "${SCANLOGS_PERF_COLD_RUNS:=5}"
: "${SCANLOGS_RPC_TIMEOUT_SECONDS:=15}"

mkdir -p "$SCANLOGS_PERF_OUTPUT_DIR/responses"
samples="$SCANLOGS_PERF_OUTPUT_DIR/samples.jsonl"
: >"$samples"

for space in cfx eth; do
  if [[ "$space" == cfx ]]; then
    endpoint="$SCANLOGS_CFX_RPC"
  else
    endpoint="$SCANLOGS_ETH_RPC"
  fi
  prefix="$space"
  for source in db fn mixed; do
    definition="$(jq -ec --arg space "$space" --arg source "$source" '
      .cases[] | select(.enabled != false and .tier == "gate" and
      .space == $space and .source == $source and
      ((.filter.address == null) and (.filter.topic0 == null)))
    ' "$SCANLOGS_CASE_MANIFEST" | head -n 1)"
    filter="$(jq -c '.filter' <<<"$definition")"
    name="$(jq -r '.name' <<<"$definition")"

    for direction in forward reverse; do
      reverse=false
      [[ "$direction" == reverse ]] && reverse=true
      request="$(jq -nc --argjson filter "$filter" --argjson reverse "$reverse" \
        '{filter:$filter,limit:"0x64",reverse:$reverse}')"
      payload="$(jq -nc --arg method "${prefix}_scanLogs" --argjson request "$request" \
        '{jsonrpc:"2.0",id:1,method:$method,params:[$request]}')"

      for ((run=1; run<=SCANLOGS_PERF_COLD_RUNS; run++)); do
        output="$SCANLOGS_PERF_OUTPUT_DIR/responses/${name}-${direction}-${run}.json"
        timing="$(curl --silent --show-error --connect-timeout 5 \
          --max-time "$SCANLOGS_RPC_TIMEOUT_SECONDS" \
          --header 'Content-Type: application/json' --data "$payload" \
          --output "$output" --write-out '%{time_total} %{size_download}' "$endpoint")"
        read -r seconds bytes <<<"$timing"
        jq -e 'has("result") and (.error == null) and (.result.logs | type == "array") and (.result.logs | length <= 100)' \
          "$output" >/dev/null
        count="$(jq '.result.logs | length' "$output")"
        jq -nc --arg profile P1 --arg space "$space" --arg source "$source" \
          --arg direction "$direction" --arg case "$name" --argjson run "$run" \
          --argjson seconds "$seconds" --argjson bytes "$bytes" --argjson logs "$count" \
          '{profile:$profile,space:$space,source:$source,direction:$direction,case:$case,
            run:$run,latencyMs:($seconds*1000),bytes:$bytes,logs:$logs,status:"passed"}' >>"$samples"
      done
    done
  done
done

jq -s '
  def percentile($p): sort | .[((length * $p | ceil) - 1)];
  group_by([.space,.source,.direction]) |
  map({
    space: .[0].space, source: .[0].source, direction: .[0].direction,
    requests: length, errors: map(select(.status != "passed")) | length,
    p50Ms: (map(.latencyMs) | percentile(0.50)),
    p95Ms: (map(.latencyMs) | percentile(0.95)),
    p99Ms: (map(.latencyMs) | percentile(0.99)),
    maxBytes: (map(.bytes) | max), maxLogs: (map(.logs) | max)
  })
' "$samples" >"$SCANLOGS_PERF_OUTPUT_DIR/summary.json"

jq -r '
  ["space","source","direction","requests","errors","p50_ms","p95_ms","p99_ms","max_bytes","max_logs"],
  (.[] | [.space,.source,.direction,.requests,.errors,.p50Ms,.p95Ms,.p99Ms,.maxBytes,.maxLogs]) |
  @csv
' "$SCANLOGS_PERF_OUTPUT_DIR/summary.json" >"$SCANLOGS_PERF_OUTPUT_DIR/summary.csv"

jq -e 'all(.[]; .errors == 0) and all(.[] | select(.source == "db"); .p95Ms <= 500) and
  all(.[] | select(.source != "db"); .p95Ms <= 2500)' \
  "$SCANLOGS_PERF_OUTPUT_DIR/summary.json" >/dev/null
