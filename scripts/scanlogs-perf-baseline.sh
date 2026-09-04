#!/usr/bin/env bash

set -Eeuo pipefail

: "${SCANLOGS_CASE_MANIFEST:?SCANLOGS_CASE_MANIFEST is required}"
: "${SCANLOGS_PERF_OUTPUT_DIR:?SCANLOGS_PERF_OUTPUT_DIR is required}"
: "${SCANLOGS_ETH_RPC:?SCANLOGS_ETH_RPC is required}"
: "${SCANLOGS_ETH_PERF_FROM_NONE:?SCANLOGS_ETH_PERF_FROM_NONE is required}"
: "${SCANLOGS_ETH_PERF_FROM_ADDRESS:?SCANLOGS_ETH_PERF_FROM_ADDRESS is required}"
: "${SCANLOGS_ETH_PERF_FROM_TOPIC0:?SCANLOGS_ETH_PERF_FROM_TOPIC0 is required}"
: "${SCANLOGS_ETH_PERF_TO:?SCANLOGS_ETH_PERF_TO is required}"
: "${SCANLOGS_PERF_P2_PAGES:=500}"
: "${SCANLOGS_RPC_TIMEOUT_SECONDS:=15}"

mkdir -p "$SCANLOGS_PERF_OUTPUT_DIR"
samples="$SCANLOGS_PERF_OUTPUT_DIR/samples.jsonl"
: >"$samples"

base="$(jq -ec '.cases[] | select(.enabled != false and .tier == "gate" and
  .space == "eth" and .source == "db" and .filter.address != null and .filter.topic0 != null)' \
  "$SCANLOGS_CASE_MANIFEST" | head -n 1)"
address="$(jq -r '.filter.address' <<<"$base")"
topic0="$(jq -r '.filter.topic0' <<<"$base")"
to_hex="$(printf '0x%x' "$SCANLOGS_ETH_PERF_TO")"

for filter_name in none address topic0 address-topic0; do
  case "$filter_name" in
    none) from="$SCANLOGS_ETH_PERF_FROM_NONE" ;;
    address) from="$SCANLOGS_ETH_PERF_FROM_ADDRESS" ;;
    topic0) from="$SCANLOGS_ETH_PERF_FROM_TOPIC0" ;;
    address-topic0)
      from="$SCANLOGS_ETH_PERF_FROM_ADDRESS"
      (( SCANLOGS_ETH_PERF_FROM_TOPIC0 > from )) && from="$SCANLOGS_ETH_PERF_FROM_TOPIC0"
      ;;
  esac
  from_hex="$(printf '0x%x' "$from")"
  filter="$(jq -nc --arg from "$from_hex" --arg to "$to_hex" \
    '{blockRange:{fromBlock:$from,toBlock:$to}}')"
  case "$filter_name" in
    address) filter="$(jq -c --arg address "$address" '. + {address:$address}' <<<"$filter")" ;;
    topic0) filter="$(jq -c --arg topic0 "$topic0" '. + {topic0:$topic0}' <<<"$filter")" ;;
    address-topic0)
      filter="$(jq -c --arg address "$address" --arg topic0 "$topic0" \
        '. + {address:$address,topic0:$topic0}' <<<"$filter")"
      ;;
  esac

  for direction in forward reverse; do
    reverse=false
    [[ "$direction" == reverse ]] && reverse=true
    cursor=null
    cursor_file="$SCANLOGS_PERF_OUTPUT_DIR/${filter_name}-${direction}-cursors.jsonl"
    : >"$cursor_file"

    for ((page=1; page<=SCANLOGS_PERF_P2_PAGES; page++)); do
      request="$(jq -nc --argjson filter "$filter" --argjson reverse "$reverse" \
        --argjson cursor "$cursor" \
        '{filter:$filter,limit:"0x64",reverse:$reverse} +
        (if $cursor == null then {} else {cursor:$cursor} end)')"
      payload="$(jq -nc --argjson request "$request" \
        '{jsonrpc:"2.0",id:1,method:"eth_scanLogs",params:[$request]}')"
      response="$SCANLOGS_PERF_OUTPUT_DIR/.current-response.json"
      timing="$(curl --silent --show-error --connect-timeout 5 \
        --max-time "$SCANLOGS_RPC_TIMEOUT_SECONDS" \
        --header 'Content-Type: application/json' --data "$payload" \
        --output "$response" --write-out '%{time_total} %{size_download}' "$SCANLOGS_ETH_RPC")"
      read -r seconds bytes <<<"$timing"
      jq -e 'has("result") and (.error == null) and (.result.logs | type == "array") and
        (.result.logs | length > 0 and length <= 100) and (.result.nextCursor != null)' \
        "$response" >/dev/null
      next_cursor="$(jq -c '.result.nextCursor' "$response")"
      [[ "$next_cursor" != "$cursor" ]] || {
        printf 'cursor did not advance: filter=%s direction=%s page=%s\n' "$filter_name" "$direction" "$page" >&2
        exit 1
      }
      count="$(jq '.result.logs | length' "$response")"
      printf '%s\n' "$next_cursor" >>"$cursor_file"
      jq -nc --arg profile P2 --arg filter "$filter_name" --arg direction "$direction" \
        --argjson page "$page" --argjson seconds "$seconds" --argjson bytes "$bytes" \
        --argjson logs "$count" \
        '{profile:$profile,space:"eth",source:"db",filter:$filter,direction:$direction,
          page:$page,latencyMs:($seconds*1000),bytes:$bytes,logs:$logs,status:"passed"}' >>"$samples"
      cursor="$next_cursor"
    done
  done
done
rm -f "$SCANLOGS_PERF_OUTPUT_DIR/.current-response.json"

jq -s '
  def percentile($p): sort | .[((length * $p | ceil) - 1)];
  group_by([.filter,.direction]) |
  map({filter:.[0].filter,direction:.[0].direction,pages:length,
    errors:(map(select(.status != "passed"))|length),
    p50Ms:(map(.latencyMs)|percentile(0.50)),
    p95Ms:(map(.latencyMs)|percentile(0.95)),
    p99Ms:(map(.latencyMs)|percentile(0.99)),
    maxBytes:(map(.bytes)|max),minLogs:(map(.logs)|min),maxLogs:(map(.logs)|max)})
' "$samples" >"$SCANLOGS_PERF_OUTPUT_DIR/summary.json"

jq -e --argjson pages "$SCANLOGS_PERF_P2_PAGES" \
  'length == 8 and all(.[]; .pages == $pages and .errors == 0 and .p95Ms <= 500)' \
  "$SCANLOGS_PERF_OUTPUT_DIR/summary.json" >/dev/null
