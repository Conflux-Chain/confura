#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
BIN=${CONFURA_BIN:-"$ROOT_DIR/bin/confura"}
UPSTREAM=${CONFURA_CFX_BRIDGE_ETHNODE:-"https://evmtestnet-internal.confluxrpc.com"}
CFX_UPSTREAM=${CONFURA_CFX_BRIDGE_CFXNODE:-"http://test-internal.confluxrpc.com"}
ENDPOINT=${CONFURA_CFX_BRIDGE_ENDPOINT:-":32537"}
RUN_DIR=${CONFURA_RUN_DIR:-"$ROOT_DIR/run"}
LOG_FILE=${CONFURA_LOG_FILE:-"$ROOT_DIR/cfx-bridge.log"}
PID_FILE="$RUN_DIR/cfx-bridge.pid"

mkdir -p "$RUN_DIR"

running_pid() {
  if [[ -f "$PID_FILE" ]]; then
    local pid
    pid=$(cat "$PID_FILE")
    if kill -0 "$pid" 2>/dev/null; then
      echo "$pid"
      return 0
    fi
    rm -f "$PID_FILE"
  fi
  return 1
}

case "${1:-start}" in
  start)
    if pid=$(running_pid); then
      echo "cfx bridge is already running: pid=$pid"
      exit 0
    fi
    cd "$ROOT_DIR"
    echo "starting cfx bridge: cfx_upstream=$CFX_UPSTREAM eth_upstream=$UPSTREAM endpoint=$ENDPOINT"
    INFURA_RPC_CFXBRIDGE_CFXNODE="$CFX_UPSTREAM" \
    INFURA_RPC_CFXBRIDGE_ETHNODE="$UPSTREAM" \
    INFURA_RPC_CFXBRIDGE_ENDPOINT="$ENDPOINT" \
      nohup "$BIN" rpc --cfxBridge >>"$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    echo "started: pid=$(cat "$PID_FILE"), log=$LOG_FILE"
    ;;
  stop)
    if pid=$(running_pid); then
      kill "$pid"
      for _ in {1..20}; do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.5
      done
      if kill -0 "$pid" 2>/dev/null; then kill -TERM "$pid" 2>/dev/null || true; fi
      rm -f "$PID_FILE"
      echo "stopped: pid=$pid"
    else
      echo "cfx bridge is not running"
    fi
    ;;
  status)
    if pid=$(running_pid); then
      echo "cfx bridge is running: pid=$pid endpoint=$ENDPOINT upstream=$UPSTREAM"
    else
      echo "cfx bridge is not running"
      exit 1
    fi
    ;;
  *)
    echo "usage: $0 {start|stop|status}" >&2
    exit 2
    ;;
esac
