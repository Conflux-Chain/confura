#!/usr/bin/env bash

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
readonly DDL_SCRIPT="${SCRIPT_DIR}/scanlogs-index-ddl.sh"

COMMAND=""
ENV_FILE=""
RUN_ID_OVERRIDE=""
ARTIFACT_OVERRIDE=""

usage() {
  cat <<'USAGE'
Usage:
  scanlogs-local-test.sh [--env-file FILE] [--run-id ID]
                         [--artifact-dir DIR] COMMAND

Commands:
  init            Create the evidence directory and capture local versions.
  preflight       Check tools, MySQL connectivity, and RPC reachability.
  unit            Build and run scanLogs-focused, race, and repository tests.
  fault           Run deterministic scanLogs retry/boundary/Route-B tests.
  watermarks      Capture Core/eSpace earliest and latest DB mappings.
  ddl-plan        Read-only DDL inventory for both databases.
  ddl-add         Add indexes. Requires SCANLOGS_ALLOW_DDL=yes.
  ddl-verify      Read-only index and EXPLAIN verification.
  ddl-drop        Drop replaced indexes. Requires SCANLOGS_ALLOW_DROP=yes.
  ddl-cycle       plan -> add twice -> verify -> drop -> verify. Requires both
                  SCANLOGS_ALLOW_DDL=yes and SCANLOGS_ALLOW_DROP=yes.
  rpc-smoke       Check four scanLogs methods and basic node RPC methods.
  rpc-negative    Check unknown fields and framework default business errors.
  e2e             Run manifest-driven scanLogs pagination vs getLogs oracle.
  e2e-boundary    Run parameterized cursor/range/limit/pivot boundary cases.
  api-blackbox    Run parameterized JSON-RPC request-shape negative cases.
  api-security    Run API black-box, ACL, unavailable-Store, and routing tests.
  consistency     Validate CON mapping and run its existing evidence tests.
  mysql-integration
                  Run disposable-database MySQL integration cases.
  perf-smoke      Run the P1 latency matrix against fixed natural-data cases.
  perf-baseline   Run P2 eSpace 500-page serial baselines for four filters.
  perf-large      Run P5 eSpace limit-1000 load with resource observation.
  regression      Run Go regression and manifest-driven getLogs comparison.
  all-readonly    Run all non-DDL-mutating phases, including E2E.
  report          Build conclusion.md from phase status files.
  help            Show this help.

The script never starts or stops Confura, sync, MySQL, or full nodes. Prepare
those services with scanlogs-local-test-runbook.md first. ddl-add and ddl-drop
are intentionally guarded and must only target disposable test databases.
USAGE
}

log() {
  printf '%s [%s] %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$1" "$2"
}

die() {
  log ERROR "$1" >&2
  exit "${2:-1}"
}

while (($# > 0)); do
  case "$1" in
    --env-file)
      ENV_FILE="${2:-}"
      shift 2
      ;;
    --run-id)
      RUN_ID_OVERRIDE="${2:-}"
      shift 2
      ;;
    --artifact-dir)
      ARTIFACT_OVERRIDE="${2:-}"
      shift 2
      ;;
    -h|--help)
      COMMAND="help"
      shift
      ;;
    *)
      [[ -z "$COMMAND" ]] || die "only one command may be supplied" 2
      COMMAND="$1"
      shift
      ;;
  esac
done

[[ -n "$COMMAND" ]] || COMMAND="help"

if [[ -n "$ENV_FILE" ]]; then
  [[ -r "$ENV_FILE" ]] || die "environment file is not readable: $ENV_FILE" 2
  # The env file is a trusted local shell file. For local-only testing it may
  # contain MySQL credentials; keep it out of Git and restrict its permissions.
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

if [[ -n "$RUN_ID_OVERRIDE" ]]; then
  SCANLOGS_RUN_ID="$RUN_ID_OVERRIDE"
fi
if [[ -n "$ARTIFACT_OVERRIDE" ]]; then
  SCANLOGS_ARTIFACT_DIR="$ARTIFACT_OVERRIDE"
fi

: "${SCANLOGS_RUN_ID:=$(date -u +%Y%m%dT%H%M%SZ)}"
: "${SCANLOGS_ARTIFACT_DIR:=${REPO_ROOT}/artifacts/scanlogs/${SCANLOGS_RUN_ID}}"
: "${SCANLOGS_RPC_TIMEOUT_SECONDS:=15}"
: "${SCANLOGS_PAGE_LIMIT:=7}"
: "${SCANLOGS_MAX_LIMIT:=1000}"
: "${SCANLOGS_MAX_PAGES:=10000}"
: "${SCANLOGS_ORACLE_WINDOW:=1000}"
: "${SCANLOGS_ORACLE_TIMEOUT_SECONDS:=60}"
: "${SCANLOGS_E2E_DIRECTIONS:=forward,reverse}"
: "${SCANLOGS_E2E_VARIANTS:=plain,pivot}"
: "${SCANLOGS_CFX_ADDRESS_PARTITIONS:=100}"
: "${SCANLOGS_CFX_TOPIC_PARTITIONS:=10}"
: "${SCANLOGS_ETH_ADDRESS_PARTITIONS:=100}"
: "${SCANLOGS_ETH_TOPIC_PARTITIONS:=10}"
: "${SCANLOGS_RUN_RACE:=1}"
: "${SCANLOGS_RUN_ALL_TESTS:=1}"
: "${SCANLOGS_RUN_LEGACY_GETLOGS:=0}"
: "${SCANLOGS_VALIDATE_SOURCE:=1}"
: "${SCANLOGS_MYSQL_BIN:=mysql}"
: "${SCANLOGS_SPECIAL_CASE_MANIFEST:=${REPO_ROOT}/scripts/scanlogs-local-special-cases.json}"
: "${SCANLOGS_CONSISTENCY_MANIFEST:=${REPO_ROOT}/scripts/scanlogs-consistency-cases.json}"

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

require_positive_integer() {
  [[ "$2" =~ ^[1-9][0-9]*$ ]] || die "$1 must be a positive integer, got: $2" 2
}

require_non_negative_integer() {
  [[ "$2" =~ ^[0-9]+$ ]] || die "$1 must be a non-negative integer, got: $2" 2
}

require_yes() {
  [[ "${2:-}" == "yes" ]] || die "$1=yes is required for this command" 2
}

init_run() {
  mkdir -p "${SCANLOGS_ARTIFACT_DIR}"/{env,unit,fault,ddl,data,integration,api,api-blackbox,boundary,e2e,regression,perf,staging}

  go version >"${SCANLOGS_ARTIFACT_DIR}/env/go-version.txt" 2>&1 || true
  git -C "$REPO_ROOT" rev-parse HEAD >"${SCANLOGS_ARTIFACT_DIR}/env/git-revision.txt" 2>&1 || true
  git -C "$REPO_ROOT" status --short >"${SCANLOGS_ARTIFACT_DIR}/env/git-status.txt" 2>&1 || true
  uname -a >"${SCANLOGS_ARTIFACT_DIR}/env/uname.txt" 2>&1 || true

  {
    printf 'run_id=%s\n' "$SCANLOGS_RUN_ID"
    printf 'repo_root=%s\n' "$REPO_ROOT"
    printf 'started_utc=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    printf 'cfx_rpc_configured=%s\n' "$([[ -n "${SCANLOGS_CFX_RPC:-}" ]] && printf yes || printf no)"
    printf 'eth_rpc_configured=%s\n' "$([[ -n "${SCANLOGS_ETH_RPC:-}" ]] && printf yes || printf no)"
    printf 'cfx_fn_configured=%s\n' "$([[ -n "${SCANLOGS_CFX_FN_RPC:-}" ]] && printf yes || printf no)"
    printf 'eth_fn_configured=%s\n' "$([[ -n "${SCANLOGS_ETH_FN_RPC:-}" ]] && printf yes || printf no)"
    printf 'cfx_database=%s\n' "${SCANLOGS_CFX_DATABASE:-}"
    printf 'eth_database=%s\n' "${SCANLOGS_ETH_DATABASE:-}"
    printf 'case_manifest=%s\n' "${SCANLOGS_CASE_MANIFEST:-}"
  } >"${SCANLOGS_ARTIFACT_DIR}/env/run.properties"

  log INFO "run_id=$SCANLOGS_RUN_ID artifacts=$SCANLOGS_ARTIFACT_DIR"
}

run_logged() {
  local phase=$1
  shift
  local log_file="${SCANLOGS_ARTIFACT_DIR}/${phase}.log"
  local status_file="${SCANLOGS_ARTIFACT_DIR}/${phase}.status"
  local exit_code

  mkdir -p "$(dirname "$log_file")"
  log INFO "starting phase=$phase"
  set +e
  (set -Eeuo pipefail; "$@") 2>&1 | tee "$log_file"
  exit_code=${PIPESTATUS[0]}
  set -e
  printf '%s\n' "$exit_code" >"$status_file"
  if ((exit_code != 0)); then
    log ERROR "failed phase=$phase exit=$exit_code"
    return "$exit_code"
  fi
  log INFO "passed phase=$phase"
}

rpc_call_to_file() {
  local endpoint=$1
  local method=$2
  local params=$3
  local output=$4
  local payload

  payload="$(jq -nc --arg method "$method" --argjson params "$params" \
    '{jsonrpc:"2.0",id:1,method:$method,params:$params}')"
  printf '%s\n' "$payload" >"${output%.json}.request.json"
  if ! curl --silent --show-error \
    --connect-timeout 5 \
    --max-time "$SCANLOGS_RPC_TIMEOUT_SECONDS" \
    --header 'Content-Type: application/json' \
    --data "$payload" \
    --output "$output" \
    "$endpoint"; then
    log ERROR "RPC transport failed: method=$method" >&2
    return 1
  fi
  if [[ ! -s "$output" ]]; then
    log ERROR "RPC returned an empty response body: method=$method" >&2
    return 1
  fi
  if ! jq -e . "$output" >/dev/null; then
    log ERROR "RPC returned invalid JSON: method=$method output=$output" >&2
    return 1
  fi
}

rpc_expect_success() {
  local endpoint=$1
  local method=$2
  local params=$3
  local output=$4

  rpc_call_to_file "$endpoint" "$method" "$params" "$output"
  jq -e 'has("result") and (.error == null)' "$output" >/dev/null || {
    jq . "$output" >&2
    return 1
  }
  jq . "$output"
}

rpc_expect_error() {
  local endpoint=$1
  local method=$2
  local params=$3
  local expected_code=$4
  local message_pattern=$5
  local output=$6

  rpc_call_to_file "$endpoint" "$method" "$params" "$output"
  jq -e 'has("error") and (.error != null)' "$output" >/dev/null || {
    jq . "$output" >&2
    return 1
  }
  if [[ -n "$expected_code" ]]; then
    jq -e --argjson code "$expected_code" '.error.code == $code' "$output" >/dev/null
  fi
  if [[ -n "$message_pattern" ]]; then
    jq -e --arg pattern "$message_pattern" '.error.message | test($pattern; "i")' "$output" >/dev/null
  fi
  jq . "$output"
}

rpc_expect_success_retry() {
  local endpoint=$1
  local method=$2
  local params=$3
  local output=$4
  local attempt attempt_output

  for attempt in 1 2 3; do
    attempt_output="${output%.json}.attempt-${attempt}.json"
    if rpc_call_to_file "$endpoint" "$method" "$params" "$attempt_output"; then
      if jq -e 'has("result") and (.error == null)' "$attempt_output" >/dev/null; then
        cp "$attempt_output" "$output"
        jq . "$output"
        return 0
      fi
      if ! jq -e '.error.message | strings | test("internal error"; "i")' \
        "$attempt_output" >/dev/null; then
        jq . "$attempt_output" >&2
        return 1
      fi
    fi
    ((attempt < 3)) || break
    log WARN "transient RPC failure; retrying method=$method attempt=$((attempt + 1))/3" >&2
    sleep 1
  done
  [[ -r "$attempt_output" ]] && jq . "$attempt_output" >&2
  return 1
}

mysql_exec() {
  local database=$1
  local sql=$2
  local auth_args=()

  if [[ -n "${SCANLOGS_MYSQL_DEFAULTS_FILE:-}" ]]; then
    auth_args+=("--defaults-extra-file=${SCANLOGS_MYSQL_DEFAULTS_FILE}")
  elif [[ -n "${SCANLOGS_MYSQL_LOGIN_PATH:-}" ]]; then
    auth_args+=("--login-path=${SCANLOGS_MYSQL_LOGIN_PATH}")
  else
    auth_args+=("--host=${SCANLOGS_MYSQL_HOST}" "--port=${SCANLOGS_MYSQL_PORT}" \
      "--user=${SCANLOGS_MYSQL_USER}")
    MYSQL_PWD="${SCANLOGS_MYSQL_PASSWORD:-}" \
      "$SCANLOGS_MYSQL_BIN" "${auth_args[@]}" --database="$database" --batch --raw --execute="$sql"
    return
  fi

  "$SCANLOGS_MYSQL_BIN" "${auth_args[@]}" --database="$database" --batch --raw --execute="$sql"
}

mysql_scalar() {
  local database=$1
  local sql=$2
  local auth_args=()

  if [[ -n "${SCANLOGS_MYSQL_DEFAULTS_FILE:-}" ]]; then
    auth_args+=("--defaults-extra-file=${SCANLOGS_MYSQL_DEFAULTS_FILE}")
  elif [[ -n "${SCANLOGS_MYSQL_LOGIN_PATH:-}" ]]; then
    auth_args+=("--login-path=${SCANLOGS_MYSQL_LOGIN_PATH}")
  else
    auth_args+=("--host=${SCANLOGS_MYSQL_HOST}" "--port=${SCANLOGS_MYSQL_PORT}" \
      "--user=${SCANLOGS_MYSQL_USER}")
    MYSQL_PWD="${SCANLOGS_MYSQL_PASSWORD:-}" \
      "$SCANLOGS_MYSQL_BIN" "${auth_args[@]}" --database="$database" --batch --raw \
        --skip-column-names --execute="$sql"
    return
  fi
  "$SCANLOGS_MYSQL_BIN" "${auth_args[@]}" --database="$database" --batch --raw \
    --skip-column-names --execute="$sql"
}

require_mysql_config() {
  [[ -z "${SCANLOGS_MYSQL_DEFAULTS_FILE:-}" || -z "${SCANLOGS_MYSQL_LOGIN_PATH:-}" ]] || \
    die "SCANLOGS_MYSQL_DEFAULTS_FILE and SCANLOGS_MYSQL_LOGIN_PATH are mutually exclusive" 2
  if [[ -n "${SCANLOGS_MYSQL_DEFAULTS_FILE:-}" ]]; then
    [[ -r "$SCANLOGS_MYSQL_DEFAULTS_FILE" ]] || \
      die "MySQL defaults file is not readable: $SCANLOGS_MYSQL_DEFAULTS_FILE" 2
  fi
  if [[ -z "${SCANLOGS_MYSQL_DEFAULTS_FILE:-}${SCANLOGS_MYSQL_LOGIN_PATH:-}" ]]; then
    [[ -n "${SCANLOGS_MYSQL_HOST:-}" ]] || die "SCANLOGS_MYSQL_HOST is required" 2
    [[ "${SCANLOGS_MYSQL_PORT:-}" =~ ^[0-9]+$ ]] || \
      die "SCANLOGS_MYSQL_PORT must be an integer" 2
    [[ -n "${SCANLOGS_MYSQL_USER:-}" ]] || die "SCANLOGS_MYSQL_USER is required" 2
  fi
}

mysql_password_command() {
  MYSQL_PWD="${SCANLOGS_MYSQL_PASSWORD:-}" "$@"
}

preflight() {
  require_command go
  require_command git
  require_command curl
  require_command jq
  require_positive_integer SCANLOGS_RPC_TIMEOUT_SECONDS "$SCANLOGS_RPC_TIMEOUT_SECONDS"
  require_positive_integer SCANLOGS_PAGE_LIMIT "$SCANLOGS_PAGE_LIMIT"
  require_positive_integer SCANLOGS_MAX_PAGES "$SCANLOGS_MAX_PAGES"
  [[ -x "$DDL_SCRIPT" ]] || die "DDL script is not executable: $DDL_SCRIPT"

  [[ -n "${SCANLOGS_CFX_RPC:-}" ]] || die "SCANLOGS_CFX_RPC is required"
  [[ -n "${SCANLOGS_ETH_RPC:-}" ]] || die "SCANLOGS_ETH_RPC is required"
  [[ -n "${SCANLOGS_CFX_FN_RPC:-}" ]] || die "SCANLOGS_CFX_FN_RPC is required"
  [[ -n "${SCANLOGS_ETH_FN_RPC:-}" ]] || die "SCANLOGS_ETH_FN_RPC is required"
  [[ -n "${SCANLOGS_CFX_DATABASE:-}" ]] || die "SCANLOGS_CFX_DATABASE is required"
  [[ -n "${SCANLOGS_ETH_DATABASE:-}" ]] || die "SCANLOGS_ETH_DATABASE is required"

  require_command "$SCANLOGS_MYSQL_BIN"
  require_mysql_config
  mysql_exec "$SCANLOGS_CFX_DATABASE" 'SELECT VERSION() AS mysql_version, DATABASE() AS selected_database;'
  mysql_exec "$SCANLOGS_ETH_DATABASE" 'SELECT VERSION() AS mysql_version, DATABASE() AS selected_database;'

  mkdir -p "${SCANLOGS_ARTIFACT_DIR}/env/rpc"
  rpc_expect_success "$SCANLOGS_CFX_RPC" cfx_getStatus '[]' \
    "${SCANLOGS_ARTIFACT_DIR}/env/rpc/cfx-proxy-status.json" >/dev/null
  rpc_expect_success "$SCANLOGS_ETH_RPC" eth_blockNumber '[]' \
    "${SCANLOGS_ARTIFACT_DIR}/env/rpc/eth-proxy-height.json" >/dev/null
  rpc_expect_success "$SCANLOGS_CFX_FN_RPC" cfx_getStatus '[]' \
    "${SCANLOGS_ARTIFACT_DIR}/env/rpc/cfx-fn-status.json" >/dev/null
  rpc_expect_success "$SCANLOGS_ETH_FN_RPC" eth_blockNumber '[]' \
    "${SCANLOGS_ARTIFACT_DIR}/env/rpc/eth-fn-height.json" >/dev/null
}

unit_tests() {
  run_logged unit/build go build ./...
  run_logged unit/scanlogs-targeted go test -count=1 \
    ./store/mysql ./rpc/handler ./util/acl
  if [[ "$SCANLOGS_RUN_RACE" == "1" ]]; then
    run_logged unit/scanlogs-race go test -race -count=1 \
      ./store/mysql ./rpc/handler ./util/acl
  fi
  if [[ "$SCANLOGS_RUN_ALL_TESTS" == "1" ]]; then
    run_logged unit/all go test -count=1 ./...
  fi
}

fault_tests() {
  local pattern='ScanLogs|RouteB|FNReader|PivotGuard|FNOversized|CanonicalCommit'
  run_logged fault/handler go test -count=1 -v ./rpc/handler -run "$pattern"
  run_logged fault/store go test -count=1 -v ./store/mysql -run 'ScanLog|Migration'
}

capture_watermarks() {
  require_command "$SCANLOGS_MYSQL_BIN"
  require_mysql_config
  local sql='SELECT epoch, bn_min, bn_max, pivot_hash FROM epoch_block_map ORDER BY epoch ASC LIMIT 1; SELECT epoch, bn_min, bn_max, pivot_hash FROM epoch_block_map ORDER BY epoch DESC LIMIT 1;'

  [[ -n "${SCANLOGS_CFX_DATABASE:-}" ]] || die "SCANLOGS_CFX_DATABASE is required"
  [[ -n "${SCANLOGS_ETH_DATABASE:-}" ]] || die "SCANLOGS_ETH_DATABASE is required"
  run_logged data/cfx-watermarks mysql_exec "$SCANLOGS_CFX_DATABASE" "$sql"
  run_logged data/eth-watermarks mysql_exec "$SCANLOGS_ETH_DATABASE" "$sql"
}

run_ddl_one() {
  local space=$1
  local database=$2
  local address_partitions=$3
  local topic_partitions=$4
  local mode=$5
  local phase_label=$6
  local auth_args=()

  [[ -n "$database" ]] || die "$space database is not configured"
  require_non_negative_integer "$space address partitions" "$address_partitions"
  require_non_negative_integer "$space topic partitions" "$topic_partitions"
  require_mysql_config

  if [[ -n "${SCANLOGS_MYSQL_DEFAULTS_FILE:-}" ]]; then
    auth_args+=(--defaults-extra-file "$SCANLOGS_MYSQL_DEFAULTS_FILE")
  elif [[ -n "${SCANLOGS_MYSQL_LOGIN_PATH:-}" ]]; then
    auth_args+=(--login-path "$SCANLOGS_MYSQL_LOGIN_PATH")
  else
    auth_args+=(--host "$SCANLOGS_MYSQL_HOST" --port "$SCANLOGS_MYSQL_PORT" \
      --user "$SCANLOGS_MYSQL_USER")
  fi
  local ddl_command=("$DDL_SCRIPT" --database "$database" \
    --address-partitions "$address_partitions" --topic-partitions "$topic_partitions" \
    "${auth_args[@]}" --mysql-bin "$SCANLOGS_MYSQL_BIN" --mode "$mode")
  if [[ "$mode" == "add" || "$mode" == "drop" ]]; then
    ddl_command+=(--execute)
  fi
  if [[ -z "${SCANLOGS_MYSQL_DEFAULTS_FILE:-}${SCANLOGS_MYSQL_LOGIN_PATH:-}" ]]; then
    run_logged "ddl/${space}-${phase_label}" mysql_password_command "${ddl_command[@]}"
  else
    run_logged "ddl/${space}-${phase_label}" "${ddl_command[@]}"
  fi
}

run_ddl_mode() {
  local mode=$1
  local phase_label=${2:-$mode}
  if [[ "$mode" == "add" ]]; then
    require_yes SCANLOGS_ALLOW_DDL "${SCANLOGS_ALLOW_DDL:-}"
  elif [[ "$mode" == "drop" ]]; then
    require_yes SCANLOGS_ALLOW_DROP "${SCANLOGS_ALLOW_DROP:-}"
  fi
  require_command "$SCANLOGS_MYSQL_BIN"

  run_ddl_one cfx "${SCANLOGS_CFX_DATABASE:-}" \
    "$SCANLOGS_CFX_ADDRESS_PARTITIONS" "$SCANLOGS_CFX_TOPIC_PARTITIONS" "$mode" "$phase_label"
  run_ddl_one eth "${SCANLOGS_ETH_DATABASE:-}" \
    "$SCANLOGS_ETH_ADDRESS_PARTITIONS" "$SCANLOGS_ETH_TOPIC_PARTITIONS" "$mode" "$phase_label"
}

ddl_cycle() {
  require_yes SCANLOGS_ALLOW_DDL "${SCANLOGS_ALLOW_DDL:-}"
  require_yes SCANLOGS_ALLOW_DROP "${SCANLOGS_ALLOW_DROP:-}"
  run_ddl_mode plan plan
  run_ddl_mode add add-1
  run_ddl_mode add add-2-idempotent
  run_ddl_mode verify verify-before-drop
  run_ddl_mode drop drop
  run_ddl_mode verify verify-after-drop
}

rpc_smoke() {
  require_command curl
  require_command jq
  [[ -n "${SCANLOGS_CFX_RPC:-}" ]] || die "SCANLOGS_CFX_RPC is required"
  [[ -n "${SCANLOGS_ETH_RPC:-}" ]] || die "SCANLOGS_ETH_RPC is required"
  local params='[{"filter":{},"limit":"0x1"}]'

  run_logged api/cfx-scan-smoke rpc_expect_success "$SCANLOGS_CFX_RPC" \
    cfx_scanLogs "$params" "${SCANLOGS_ARTIFACT_DIR}/api/cfx-scan-smoke.json"
  run_logged api/cfx-pivot-smoke rpc_expect_success "$SCANLOGS_CFX_RPC" \
    cfx_scanLogsWithPivotAssumption "$params" "${SCANLOGS_ARTIFACT_DIR}/api/cfx-pivot-smoke.json"
  run_logged api/eth-scan-smoke rpc_expect_success "$SCANLOGS_ETH_RPC" \
    eth_scanLogs "$params" "${SCANLOGS_ARTIFACT_DIR}/api/eth-scan-smoke.json"
  run_logged api/eth-pivot-smoke rpc_expect_success "$SCANLOGS_ETH_RPC" \
    eth_scanLogsWithPivotAssumption "$params" "${SCANLOGS_ARTIFACT_DIR}/api/eth-pivot-smoke.json"
}

rpc_negative() {
  require_command curl
  require_command jq
  [[ -n "${SCANLOGS_CFX_RPC:-}" ]] || die "SCANLOGS_CFX_RPC is required"
  [[ -n "${SCANLOGS_ETH_RPC:-}" ]] || die "SCANLOGS_ETH_RPC is required"
  local unknown='[{"filter":{},"unexpected":true}]'
  local invalid_limit limit_hex over_limit
  require_positive_integer SCANLOGS_MAX_LIMIT "$SCANLOGS_MAX_LIMIT"
  invalid_limit=$((SCANLOGS_MAX_LIMIT + 1))
  printf -v limit_hex '0x%x' "$invalid_limit"
  over_limit="$(jq -nc --arg limit "$limit_hex" '[{filter:{},limit:$limit}]')"

  run_logged api/cfx-unknown-field rpc_expect_error "$SCANLOGS_CFX_RPC" \
    cfx_scanLogs "$unknown" '' 'unknown' \
    "${SCANLOGS_ARTIFACT_DIR}/api/cfx-unknown-field.json"
  run_logged api/eth-unknown-field rpc_expect_error "$SCANLOGS_ETH_RPC" \
    eth_scanLogs "$unknown" '' 'unknown' \
    "${SCANLOGS_ARTIFACT_DIR}/api/eth-unknown-field.json"
  run_logged api/cfx-default-error rpc_expect_error "$SCANLOGS_CFX_RPC" \
    cfx_scanLogs "$over_limit" -32000 'limit|maximum|invalid' \
    "${SCANLOGS_ARTIFACT_DIR}/api/cfx-default-error.json"
  run_logged api/eth-default-error rpc_expect_error "$SCANLOGS_ETH_RPC" \
    eth_scanLogs "$over_limit" -32000 'limit|maximum|invalid' \
    "${SCANLOGS_ARTIFACT_DIR}/api/eth-default-error.json"
}

safe_case_name() {
  printf '%s' "$1" | tr -c 'A-Za-z0-9._-' '_'
}

case_rpc_values() {
  local case_json=$1
  local space
  space="$(jq -r '.space' <<<"$case_json")"
  case "$space" in
    cfx)
      printf '%s\t%s\t%s\t%s\n' \
        "$space" "${SCANLOGS_CFX_RPC:-}" "${SCANLOGS_CFX_FN_RPC:-}" cfx
      ;;
    eth)
      printf '%s\t%s\t%s\t%s\n' \
        "$space" "${SCANLOGS_ETH_RPC:-}" "${SCANLOGS_ETH_FN_RPC:-}" eth
      ;;
    *)
      die "unsupported case space: $space"
      ;;
  esac
}

oracle_filter_for_case() {
  local case_json=$1
  local space=$2
  if [[ "$space" == "cfx" ]]; then
    jq -c '
      .filter as $f |
      {fromEpoch:$f.epochRange.fromEpoch,toEpoch:$f.epochRange.toEpoch}
      + (if $f.address then {address:[$f.address]} else {} end)
      + (if $f.topic0 then {topics:[[$f.topic0]]} else {} end)
    ' <<<"$case_json"
  else
    jq -c '
      .filter as $f |
      {fromBlock:$f.blockRange.fromBlock,toBlock:$f.blockRange.toBlock}
      + (if $f.address then {address:$f.address} else {} end)
      + (if $f.topic0 then {topics:[$f.topic0]} else {} end)
    ' <<<"$case_json"
  fi
}

canonical_hash_for_case() {
  local case_json=$1
  local space=$2
  local fn_endpoint=$3
  local output=$4
  local upper method params

  if [[ "$space" == "cfx" ]]; then
    upper="$(jq -er '.filter.epochRange.toEpoch' <<<"$case_json")"
    method=cfx_getBlockByEpochNumber
  else
    upper="$(jq -er '.filter.blockRange.toBlock' <<<"$case_json")"
    method=eth_getBlockByNumber
  fi
  [[ "$upper" =~ ^0x[0-9a-fA-F]+$ ]] || die "case upper bound must be numeric hex: $upper"
  params="$(jq -nc --arg upper "$upper" '[$upper,false]')"
  rpc_expect_success "$fn_endpoint" "$method" "$params" "$output" >/dev/null
  jq -er '.result.hash | select(type == "string" and length > 2)' "$output"
}

hex_to_uint() {
  local value=$1
  [[ "$value" =~ ^0x[0-9a-fA-F]+$ ]] || die "not a hex quantity: $value"
  value="${value#0x}"
  printf '%u\n' "$((16#$value))"
}

fetch_chunked_oracle() {
  local case_json=$1
  local space=$2
  local endpoint=$3
  local method=$4
  local output=$5
  local chunks_dir=$6
  local base_filter from_hex to_hex from_number to_number
  local chunk_from chunk_to chunk_index=0 chunk_filter params response
  local accumulated="${chunks_dir}/accumulated.json"
  # Bash dynamic scoping makes the RPC helper use the longer timeout only for
  # historical getLogs oracle calls. scanLogs pages retain the normal timeout.
  local SCANLOGS_RPC_TIMEOUT_SECONDS="$SCANLOGS_ORACLE_TIMEOUT_SECONDS"

  require_positive_integer SCANLOGS_ORACLE_WINDOW "$SCANLOGS_ORACLE_WINDOW"
  require_positive_integer SCANLOGS_ORACLE_TIMEOUT_SECONDS "$SCANLOGS_ORACLE_TIMEOUT_SECONDS"
  mkdir -p "$chunks_dir"
  base_filter="$(oracle_filter_for_case "$case_json" "$space")"
  if [[ "$space" == "cfx" ]]; then
    from_hex="$(jq -er '.filter.epochRange.fromEpoch' <<<"$case_json")"
    to_hex="$(jq -er '.filter.epochRange.toEpoch' <<<"$case_json")"
  else
    from_hex="$(jq -er '.filter.blockRange.fromBlock' <<<"$case_json")"
    to_hex="$(jq -er '.filter.blockRange.toBlock' <<<"$case_json")"
  fi
  from_number="$(hex_to_uint "$from_hex")"
  to_number="$(hex_to_uint "$to_hex")"
  ((from_number <= to_number)) || die "inverted oracle range [$from_hex,$to_hex]"
  printf '[]\n' >"$accumulated"

  chunk_from=$from_number
  while ((chunk_from <= to_number)); do
    chunk_to=$((chunk_from + SCANLOGS_ORACLE_WINDOW - 1))
    ((chunk_to <= to_number)) || chunk_to=$to_number
    chunk_index=$((chunk_index + 1))
    response="${chunks_dir}/$(printf '%05d' "$chunk_index").json"

    if [[ "$space" == "cfx" ]]; then
      chunk_filter="$(jq -nc --argjson base "$base_filter" \
        --arg from "$(printf '0x%x' "$chunk_from")" \
        --arg to "$(printf '0x%x' "$chunk_to")" \
        '$base + {fromEpoch:$from,toEpoch:$to}')"
    else
      chunk_filter="$(jq -nc --argjson base "$base_filter" \
        --arg from "$(printf '0x%x' "$chunk_from")" \
        --arg to "$(printf '0x%x' "$chunk_to")" \
        '$base + {fromBlock:$from,toBlock:$to}')"
    fi
    params="$(jq -nc --argjson filter "$chunk_filter" '[ $filter ]')"
    rpc_expect_success "$endpoint" "$method" "$params" "$response" >/dev/null
    jq -e '.result | type == "array"' "$response" >/dev/null
    jq -s '.[0] + .[1].result' "$accumulated" "$response" \
      >"${chunks_dir}/accumulated.next.json"
    mv "${chunks_dir}/accumulated.next.json" "$accumulated"
    chunk_from=$((chunk_to + 1))
  done

  jq -n --slurpfile result "$accumulated" \
    --argjson chunks "$chunk_index" '{result:$result[0],oracleChunks:$chunks}' >"$output"
}

validate_log_sequence() {
  local logs_file=$1
  local direction=$2
  local space=$3
  local block_hex index_hex block_number log_index
  local previous_block=-1 previous_index=-1 count=0

  if [[ "$space" == "cfx" ]]; then
    jq -e '
      map([.blockHash, .transactionHash, .logIndex] | join(":")) as $keys |
      ($keys | length) == ($keys | unique | length)
    ' "$logs_file" >/dev/null || die "duplicate Core logs found in $logs_file"
    printf 'canonical_oracle_order=passed unique_core_logs=passed logs=%d\n' \
      "$(jq 'length' "$logs_file")"
    return
  fi

  while IFS=$'\t' read -r block_hex index_hex; do
    [[ -n "$block_hex" && -n "$index_hex" ]] || \
      die "log is missing blockNumber or logIndex in $logs_file"
    block_number="$(hex_to_uint "$block_hex")"
    log_index="$(hex_to_uint "$index_hex")"

    if ((count > 0)); then
      if [[ "$direction" == "forward" ]]; then
        ((block_number > previous_block || \
          (block_number == previous_block && log_index > previous_index))) || \
          die "logs are not strictly increasing at item $((count + 1)) in $logs_file"
      else
        ((block_number < previous_block || \
          (block_number == previous_block && log_index < previous_index))) || \
          die "logs are not strictly decreasing at item $((count + 1)) in $logs_file"
      fi
    fi

    previous_block=$block_number
    previous_index=$log_index
    count=$((count + 1))
  done < <(jq -r '.[] | [.blockNumber, .logIndex] | @tsv' "$logs_file")

  printf 'strict_%s_sequence=passed logs=%d\n' "$direction" "$count"
}

normalize_log_array() {
  local input=$1
  local output=$2
  local space=$3

  jq -S --arg space "$space" '
    def canonical_cfx_address:
      if type == "string" then
        split(":") as $parts |
        if ($parts | length) >= 2 then
          (($parts[0] | ascii_downcase) + ":" + ($parts[-1] | ascii_downcase))
        else
          ascii_downcase
        end
      else . end;
    if $space == "cfx" then
      map(if has("address") then .address |= canonical_cfx_address else . end)
    else
      .
    end
  ' "$input" >"$output"
}

validate_case_source() {
  local case_json=$1
  local space=$2
  local name source database watermark from_hex to_hex from to

  [[ "$SCANLOGS_VALIDATE_SOURCE" == "1" ]] || return 0
  source="$(jq -r '.source // "unspecified"' <<<"$case_json")"
  [[ "$source" != "unspecified" ]] || return 0
  name="$(jq -r '.name' <<<"$case_json")"
  require_command "$SCANLOGS_MYSQL_BIN"
  require_mysql_config

  if [[ "$space" == "cfx" ]]; then
    database="${SCANLOGS_CFX_DATABASE:-}"
    [[ -n "$database" ]] || die "Core database is required to validate source for $name"
    watermark="$(mysql_scalar "$database" 'SELECT epoch FROM epoch_block_map ORDER BY epoch DESC LIMIT 1;')"
    from_hex="$(jq -er '.filter.epochRange.fromEpoch' <<<"$case_json")"
    to_hex="$(jq -er '.filter.epochRange.toEpoch' <<<"$case_json")"
  else
    database="${SCANLOGS_ETH_DATABASE:-}"
    [[ -n "$database" ]] || die "eSpace database is required to validate source for $name"
    watermark="$(mysql_scalar "$database" 'SELECT bn_max FROM epoch_block_map ORDER BY epoch DESC LIMIT 1;')"
    from_hex="$(jq -er '.filter.blockRange.fromBlock' <<<"$case_json")"
    to_hex="$(jq -er '.filter.blockRange.toBlock' <<<"$case_json")"
  fi

  [[ "$watermark" =~ ^[0-9]+$ ]] || die "DB watermark is unavailable for $name"
  from="$(hex_to_uint "$from_hex")"
  to="$(hex_to_uint "$to_hex")"
  ((from <= to)) || die "inverted range for $name"

  case "$source" in
    db)
      ((to <= watermark)) || die "$name is labeled db but to=$to exceeds watermark=$watermark"
      ;;
    fn)
      ((from > watermark)) || die "$name is labeled fn but from=$from is not above watermark=$watermark"
      ;;
    mixed)
      ((from <= watermark && watermark < to)) || \
        die "$name is labeled mixed but range=[$from,$to] does not straddle watermark=$watermark"
      ;;
    *)
      die "invalid source label for $name: $source"
      ;;
  esac
}

e2e_case() {
  local case_json=$1
  local direction=$2
  local variant=$3
  local name space proxy_endpoint fn_endpoint prefix
  local filter oracle_method scan_method
  local limit_hex reverse_json base_request current_request params
  local case_dir oracle_response expected actual normalized_expected normalized_actual
  local hash_before hash_after page page_response page_logs page_size
  local cursor previous_cursor guard first_reverse_guard assumption
  local oracle_logs min_logs exact_multiple
  local completed=0

  name="$(jq -er '.name' <<<"$case_json")"
  IFS=$'\t' read -r space proxy_endpoint fn_endpoint prefix < <(case_rpc_values "$case_json")
  [[ -n "$proxy_endpoint" ]] || die "proxy endpoint is missing for case $name"
  [[ -n "$fn_endpoint" ]] || die "fullnode endpoint is missing for case $name"

  case_dir="${SCANLOGS_ARTIFACT_DIR}/e2e/$(safe_case_name "$name")/${direction}-${variant}"
  mkdir -p "$case_dir/pages"

  filter="$(jq -c '.filter' <<<"$case_json")"
  validate_case_source "$case_json" "$space"
  oracle_method="${prefix}_getLogs"
  if [[ "$variant" == "pivot" ]]; then
    scan_method="${prefix}_scanLogsWithPivotAssumption"
  else
    scan_method="${prefix}_scanLogs"
  fi

  printf -v limit_hex '0x%x' "$SCANLOGS_PAGE_LIMIT"
  reverse_json=false
  [[ "$direction" == "forward" ]] || reverse_json=true
  base_request="$(jq -nc \
    --argjson filter "$filter" \
    --arg limit "$limit_hex" \
    --argjson reverse "$reverse_json" \
    '{filter:$filter,limit:$limit,reverse:$reverse}')"

  hash_before="$(canonical_hash_for_case "$case_json" "$space" "$fn_endpoint" \
    "$case_dir/canonical-before.json")"
  fetch_chunked_oracle "$case_json" "$space" "$fn_endpoint" "$oracle_method" \
    "$case_dir/oracle-response.json" "$case_dir/oracle-chunks"
  jq -e '.result | type == "array"' "$case_dir/oracle-response.json" >/dev/null

  oracle_logs="$(jq '.result | length' "$case_dir/oracle-response.json")"
  min_logs="$(jq -r '.qualification.minLogs // 0' <<<"$case_json")"
  require_non_negative_integer "qualification.minLogs for $name" "$min_logs"
  ((oracle_logs >= min_logs)) || \
    die "$name has $oracle_logs oracle logs, below required minimum $min_logs"
  exact_multiple="$(jq -r '.qualification.exactMultipleOfPageLimit // false' <<<"$case_json")"
  if [[ "$exact_multiple" == "true" ]]; then
    ((oracle_logs > 0 && oracle_logs % SCANLOGS_PAGE_LIMIT == 0)) || \
      die "$name requires an exact page multiple but has $oracle_logs logs at limit $SCANLOGS_PAGE_LIMIT"
  fi

  oracle_response="$case_dir/oracle-response.json"
  expected="$case_dir/expected.json"
  if [[ "$direction" == "reverse" ]]; then
    jq '.result | reverse' "$oracle_response" >"$expected"
  else
    jq '.result' "$oracle_response" >"$expected"
  fi

  actual="$case_dir/actual.json"
  printf '[]\n' >"$actual"
  current_request="$base_request"
  previous_cursor='null'
  assumption='null'
  first_reverse_guard='null'
  page=0

  while ((page < SCANLOGS_MAX_PAGES)); do
    page=$((page + 1))
    page_response="$case_dir/pages/$(printf '%05d' "$page").json"
    if [[ "$variant" == "pivot" && "$assumption" != "null" ]]; then
      params="$(jq -nc --argjson req "$current_request" --argjson guard "$assumption" '[$req,$guard]')"
    else
      params="$(jq -nc --argjson req "$current_request" '[$req]')"
    fi

    rpc_expect_success "$proxy_endpoint" "$scan_method" "$params" "$page_response" >/dev/null
    jq -e '.result.logs | type == "array"' "$page_response" >/dev/null
    page_logs="$case_dir/pages/$(printf '%05d' "$page").logs.json"
    jq '.result.logs' "$page_response" >"$page_logs"
    page_size="$(jq 'length' "$page_logs")"
    [[ "$page_size" =~ ^[0-9]+$ ]] || die "invalid page size for $name page $page"
    ((page_size <= SCANLOGS_PAGE_LIMIT)) || die "page exceeds limit for $name page $page"

    jq -s '.[0] + .[1]' "$actual" "$page_logs" >"$case_dir/actual.next.json"
    mv "$case_dir/actual.next.json" "$actual"

    cursor="$(jq -c 'if .result | has("nextCursor") then .result.nextCursor else null end' "$page_response")"
    guard="$(jq -c 'if .result | has("pivotGuard") then .result.pivotGuard else null end' "$page_response")"

    if [[ "$variant" == "plain" ]]; then
      [[ "$guard" == "null" ]] || die "plain scan returned pivotGuard for $name page $page"
    elif ((page_size > 0)); then
      [[ "$guard" != "null" ]] || die "pivot scan omitted guard for non-empty $name page $page"
      if [[ "$direction" == "reverse" ]]; then
        if [[ "$first_reverse_guard" == "null" ]]; then
          first_reverse_guard="$guard"
        else
          [[ "$guard" == "$first_reverse_guard" ]] || \
            die "reverse pivot guard drifted for $name page $page"
        fi
      fi
      assumption="$guard"
    else
      if [[ "$assumption" == "null" ]]; then
        [[ "$guard" == "null" ]] || \
          die "first empty pivot page returned an unexpected guard for $name page $page"
      else
        [[ "$guard" == "$assumption" ]] || \
          die "empty pivot continuation did not preserve its assumption for $name page $page"
      fi
    fi

    if ((page_size == 0)); then
      [[ "$cursor" == "null" ]] || die "empty page returned cursor for $name page $page"
      completed=1
      break
    fi

    [[ "$cursor" != "null" ]] || die "non-empty page omitted cursor for $name page $page"
    [[ "$cursor" != "$previous_cursor" ]] || die "cursor did not advance for $name page $page"
    if [[ "$space" == "eth" ]]; then
      jq -e '(.result.logs[-1] | {blockNumber,logIndex}) == .result.nextCursor' \
        "$page_response" >/dev/null || \
        die "cursor does not equal the page tail position for $name page $page"
    else
      jq -e '.result.logs[-1].logIndex == .result.nextCursor.logIndex' \
        "$page_response" >/dev/null || \
        die "Core cursor logIndex does not equal the page tail logIndex for $name page $page"
    fi
    previous_cursor="$cursor"

    if ((page_size < SCANLOGS_PAGE_LIMIT)); then
      completed=1
      break
    fi
    if [[ "$variant" == "pivot" ]]; then
      [[ "$assumption" != "null" ]] || die "continuation has no pivot assumption for $name page $page"
    fi
    current_request="$(jq -nc --argjson base "$base_request" --argjson cursor "$cursor" \
      '$base + {cursor:$cursor}')"
  done

  ((completed == 1)) || die "max pages reached for $name"
  hash_after="$(canonical_hash_for_case "$case_json" "$space" "$fn_endpoint" \
    "$case_dir/canonical-after.json")"
  [[ "$hash_before" == "$hash_after" ]] || \
    die "canonical upper hash changed for $name; case is invalidated and must be rerun"

  normalized_expected="$case_dir/expected.normalized.json"
  normalized_actual="$case_dir/actual.normalized.json"
  normalize_log_array "$expected" "$normalized_expected" "$space"
  normalize_log_array "$actual" "$normalized_actual" "$space"
  if ! cmp -s "$normalized_expected" "$normalized_actual"; then
    diff -u "$normalized_expected" "$normalized_actual" >"$case_dir/diff.txt" || true
    die "scanLogs result differs from getLogs oracle for $name $direction $variant"
  fi
  validate_log_sequence "$actual" "$direction" "$space" \
    >"$case_dir/sequence-validation.txt"

  jq -nc \
    --arg name "$name" --arg space "$space" --arg direction "$direction" \
    --arg variant "$variant" --arg source "$(jq -r '.source // "unspecified"' <<<"$case_json")" \
    --arg tier "$(jq -r '.tier // "unspecified"' <<<"$case_json")" \
    --arg hash "$hash_after" --argjson pages "$page" \
    --argjson logs "$(jq 'length' "$actual")" \
    '{name:$name,tier:$tier,space:$space,source:$source,direction:$direction,variant:$variant,pages:$pages,logs:$logs,canonicalHash:$hash,status:"passed"}' \
    >"$case_dir/result.json"
  jq . "$case_dir/result.json"
}

e2e_tests() {
  require_command curl
  require_command jq
  [[ -n "${SCANLOGS_CASE_MANIFEST:-}" ]] || die "SCANLOGS_CASE_MANIFEST is required"
  [[ -r "$SCANLOGS_CASE_MANIFEST" ]] || die "case manifest is not readable: $SCANLOGS_CASE_MANIFEST"
  require_positive_integer SCANLOGS_PAGE_LIMIT "$SCANLOGS_PAGE_LIMIT"
  require_positive_integer SCANLOGS_MAX_PAGES "$SCANLOGS_MAX_PAGES"
  jq -e '.cases | type == "array"' "$SCANLOGS_CASE_MANIFEST" >/dev/null
  cp "$SCANLOGS_CASE_MANIFEST" "${SCANLOGS_ARTIFACT_DIR}/data/manifest.json"

  local directions variants case_json name safe_name direction variant count=0
  IFS=',' read -r -a directions <<<"$SCANLOGS_E2E_DIRECTIONS"
  IFS=',' read -r -a variants <<<"$SCANLOGS_E2E_VARIANTS"

  while IFS= read -r case_json; do
    count=$((count + 1))
    name="$(jq -er '.name' <<<"$case_json")"
    safe_name="$(safe_case_name "$name")"
    for direction in "${directions[@]}"; do
      [[ "$direction" == "forward" || "$direction" == "reverse" ]] || \
        die "invalid E2E direction: $direction"
      for variant in "${variants[@]}"; do
        [[ "$variant" == "plain" || "$variant" == "pivot" ]] || \
          die "invalid E2E variant: $variant"
        run_logged "e2e/${safe_name}/${direction}-${variant}" \
          e2e_case "$case_json" "$direction" "$variant"
      done
    done
  done < <(jq -c '.cases[] | select(.enabled != false)' "$SCANLOGS_CASE_MANIFEST")
  ((count > 0)) || die "case manifest contains no enabled cases"
}

special_anchor_case() {
  local space=$1
  local anchor_name
  anchor_name="$(jq -er --arg space "$space" '.anchors[$space]' "$SCANLOGS_SPECIAL_CASE_MANIFEST")"
  jq -ec --arg name "$anchor_name" \
    '.cases[] | select(.enabled != false and .name == $name)' "$SCANLOGS_CASE_MANIFEST"
}

increment_hex() {
  local value=$1
  local delta=$2
  local number
  number="$(hex_to_uint "$value")"
  ((number + delta >= 0)) || die "hex adjustment underflow: $value $delta"
  printf '0x%x\n' "$((number + delta))"
}

assert_special_logs() {
  local response=$1
  local oracle=$2
  local space=$3
  local direction=$4
  local offset=$5
  local count=$6
  local case_dir=$7

  if [[ "$direction" == "reverse" ]]; then
    jq '.result | reverse' "$oracle" >"$case_dir/oracle-direction.json"
  else
    jq '.result' "$oracle" >"$case_dir/oracle-direction.json"
  fi
  jq --argjson offset "$offset" --argjson count "$count" \
    '.[$offset:($offset + $count)]' "$case_dir/oracle-direction.json" >"$case_dir/expected.json"
  jq '.result.logs' "$response" >"$case_dir/actual.json"
  normalize_log_array "$case_dir/expected.json" "$case_dir/expected.normalized.json" "$space"
  normalize_log_array "$case_dir/actual.json" "$case_dir/actual.normalized.json" "$space"
  cmp -s "$case_dir/expected.normalized.json" "$case_dir/actual.normalized.json" || {
    diff -u "$case_dir/expected.normalized.json" "$case_dir/actual.normalized.json" \
      >"$case_dir/diff.txt" || true
    die "special E2E logs differ from oracle: $case_dir"
  }
}

boundary_case() {
  local definition=$1
  local space=$2
  local direction=$3
  local id operation anchor endpoint fn_endpoint prefix filter reverse_json case_dir
  local base_request seed_response seed_cursor seed_guard scan_method pivot_method oracle_response
  local request response params cursor block adjusted empty_filter value limit expected_count oracle_count
  local head_response head future guard height_key hash_key

  id="$(jq -er '.id' <<<"$definition")"
  operation="$(jq -er '.operation' <<<"$definition")"
  anchor="$(special_anchor_case "$space")"
  IFS=$'\t' read -r _ endpoint fn_endpoint prefix < <(case_rpc_values "$anchor")
  filter="$(jq -c '.filter' <<<"$anchor")"
  reverse_json=false
  [[ "$direction" == "forward" ]] || reverse_json=true
  case_dir="${SCANLOGS_ARTIFACT_DIR}/boundary/$(safe_case_name "$id")/${space}-${direction}"
  mkdir -p "$case_dir"
  scan_method="${prefix}_scanLogs"
  pivot_method="${prefix}_scanLogsWithPivotAssumption"
  base_request="$(jq -nc --argjson filter "$filter" --argjson reverse "$reverse_json" \
    '{filter:$filter,limit:"0x1",reverse:$reverse}')"

  seed_response="$case_dir/seed.json"
  rpc_expect_success_retry "$endpoint" "$pivot_method" "$(jq -nc --argjson req "$base_request" '[$req]')" \
    "$seed_response" >/dev/null
  seed_cursor="$(jq -ec '.result.nextCursor' "$seed_response")"
  seed_guard="$(jq -ec '.result.pivotGuard' "$seed_response")"
  fetch_chunked_oracle "$anchor" "$space" "$fn_endpoint" "${prefix}_getLogs" \
    "$case_dir/oracle.json" "$case_dir/oracle-chunks"
  oracle_response="$case_dir/oracle.json"

  case "$operation" in
    cursor-exclusive)
      request="$(jq -nc --argjson base "$base_request" --argjson cursor "$seed_cursor" \
        '$base + {cursor:$cursor}')"
      params="$(jq -nc --argjson req "$request" --argjson guard "$seed_guard" '[$req,$guard]')"
      response="$case_dir/response.json"
      rpc_expect_success_retry "$endpoint" "$pivot_method" "$params" "$response" >/dev/null
      assert_special_logs "$response" "$oracle_response" "$space" "$direction" 1 1 "$case_dir"
      ;;
    invalid-cursor-low|invalid-cursor-high)
      block="$(jq -er '.blockNumber' <<<"$seed_cursor")"
      if [[ "$operation" == "invalid-cursor-low" ]]; then
        adjusted="$(increment_hex "$block" -1)"
      else
        adjusted="$(increment_hex "$block" 1)"
      fi
      cursor="$(jq -nc --arg block "$adjusted" '{blockNumber:$block,logIndex:"0x0"}')"
      request="$(jq -nc --argjson base "$base_request" --argjson cursor "$cursor" '$base + {cursor:$cursor}')"
      rpc_expect_error "$endpoint" "$scan_method" "$(jq -nc --argjson req "$request" '[$req]')" \
        -32000 'cursor|range' "$case_dir/response.json" >/dev/null
      ;;
    limit)
      value="$(jq -er '.value' <<<"$definition")"
      request="$(jq -nc --argjson filter "$filter" --argjson reverse "$reverse_json" \
        '{filter:$filter,reverse:$reverse}')"
      case "$value" in
        omit) ;;
        max)
          printf -v limit '0x%x' "$SCANLOGS_MAX_LIMIT"
          request="$(jq -nc --argjson base "$request" --arg limit "$limit" '$base + {limit:$limit}')"
          ;;
        *) request="$(jq -nc --argjson base "$request" --arg limit "$value" '$base + {limit:$limit}')" ;;
      esac
      response="$case_dir/response.json"
      rpc_expect_success_retry "$endpoint" "$scan_method" "$(jq -nc --argjson req "$request" '[$req]')" \
        "$response" >/dev/null
      oracle_count="$(jq '.result | length' "$oracle_response")"
      expected_count="$oracle_count"
      [[ "$value" != "0x1" ]] || expected_count=1
      assert_special_logs "$response" "$oracle_response" "$space" "$direction" 0 "$expected_count" "$case_dir"
      ;;
    endpoint)
      request="$(jq -nc --argjson filter "$filter" --argjson reverse "$reverse_json" \
        --arg limit "0x$(printf '%x' "$SCANLOGS_MAX_LIMIT")" \
        '{filter:$filter,limit:$limit,reverse:$reverse}')"
      response="$case_dir/response.json"
      rpc_expect_success_retry "$endpoint" "$scan_method" "$(jq -nc --argjson req "$request" '[$req]')" \
        "$response" >/dev/null
      oracle_count="$(jq '.result | length' "$oracle_response")"
      assert_special_logs "$response" "$oracle_response" "$space" "$direction" 0 "$oracle_count" "$case_dir"
      ;;
    invalid-range)
      if [[ "$space" == "cfx" ]]; then
        value="$(jq -er '.epochRange.toEpoch' <<<"$filter")"
        adjusted="$(increment_hex "$value" 1)"
        request="$(jq -nc --argjson base "$base_request" --arg from "$adjusted" --arg to "$value" \
          '$base | .filter.epochRange={fromEpoch:$from,toEpoch:$to}')"
      else
        value="$(jq -er '.blockRange.toBlock' <<<"$filter")"
        adjusted="$(increment_hex "$value" 1)"
        request="$(jq -nc --argjson base "$base_request" --arg from "$adjusted" --arg to "$value" \
          '$base | .filter.blockRange={fromBlock:$from,toBlock:$to}')"
      fi
      rpc_expect_error "$endpoint" "$scan_method" "$(jq -nc --argjson req "$request" '[$req]')" \
        -32000 'range|exceeds' "$case_dir/response.json" >/dev/null
      ;;
    future-upper)
      head_response="$case_dir/head.json"
      if [[ "$space" == "cfx" ]]; then
        rpc_expect_success_retry "$endpoint" cfx_getStatus '[]' "$head_response" >/dev/null
        head="$(jq -er '.result.latestState' "$head_response")"
        future="$(increment_hex "$head" 65536)"
        request="$(jq -nc --argjson base "$base_request" --arg future "$future" \
          '$base | .filter.epochRange={fromEpoch:$future,toEpoch:$future}')"
      else
        rpc_expect_success_retry "$endpoint" eth_blockNumber '[]' "$head_response" >/dev/null
        head="$(jq -er '.result' "$head_response")"
        future="$(increment_hex "$head" 65536)"
        request="$(jq -nc --argjson base "$base_request" --arg future "$future" \
          '$base | .filter.blockRange={fromBlock:$future,toBlock:$future}')"
      fi
      rpc_expect_error "$endpoint" "$scan_method" "$(jq -nc --argjson req "$request" '[$req]')" \
        -32000 'future|exceeds|latest' "$case_dir/response.json" >/dev/null
      ;;
    empty|empty-with-assumption)
      empty_filter="$(jq -nc --argjson filter "$filter" \
        '$filter + {topic0:"0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"}')"
      request="$(jq -nc --argjson base "$base_request" --argjson filter "$empty_filter" \
        '$base + {filter:$filter}')"
      if [[ "$operation" == "empty-with-assumption" ]]; then
        request="$(jq -nc --argjson base "$request" --argjson cursor "$seed_cursor" '$base + {cursor:$cursor}')"
        params="$(jq -nc --argjson req "$request" --argjson guard "$seed_guard" '[$req,$guard]')"
        response="$case_dir/response.json"
        rpc_expect_success_retry "$endpoint" "$pivot_method" "$params" "$response" >/dev/null
        jq -e --argjson guard "$seed_guard" \
          '.result.logs == [] and (.result | has("nextCursor") | not) and .result.pivotGuard == $guard' \
          "$response" >/dev/null
      else
        response="$case_dir/response.json"
        rpc_expect_success_retry "$endpoint" "$pivot_method" "$(jq -nc --argjson req "$request" '[$req]')" \
          "$response" >/dev/null
        jq -e '.result.logs == [] and (.result | has("nextCursor") | not) and (.result | has("pivotGuard") | not)' \
          "$response" >/dev/null
      fi
      ;;
    missing-assumption)
      request="$(jq -nc --argjson base "$base_request" --argjson cursor "$seed_cursor" '$base + {cursor:$cursor}')"
      rpc_expect_error "$endpoint" "$pivot_method" "$(jq -nc --argjson req "$request" '[$req]')" \
        -32000 'missing pivot assumption' "$case_dir/response.json" >/dev/null
      ;;
    stale-hash|stale-height)
      request="$(jq -nc --argjson base "$base_request" --argjson cursor "$seed_cursor" '$base + {cursor:$cursor}')"
      guard="$seed_guard"
      if [[ "$space" == "cfx" ]]; then
        height_key=epochNumber
        hash_key=pivotBlockHash
      else
        height_key=blockNumber
        hash_key=blockHash
      fi
      if [[ "$operation" == "stale-hash" ]]; then
        guard="$(jq -nc --argjson guard "$guard" --arg key "$hash_key" \
          '$guard + {($key):"0x0000000000000000000000000000000000000000000000000000000000000000"}')"
      else
        value="$(jq -er --arg key "$height_key" '.[$key]' <<<"$guard")"
        adjusted="$(increment_hex "$value" 1)"
        guard="$(jq -nc --argjson guard "$guard" --arg key "$height_key" --arg value "$adjusted" \
          '$guard + {($key):$value}')"
      fi
      rpc_expect_error "$endpoint" "$pivot_method" \
        "$(jq -nc --argjson req "$request" --argjson guard "$guard" '[$req,$guard]')" \
        -32000 'assumption|pivot|canonical|mapping' "$case_dir/response.json" >/dev/null
      ;;
    *) die "unsupported boundary operation: $operation" ;;
  esac

  jq -nc --arg id "$id" --arg operation "$operation" --arg space "$space" \
    --arg direction "$direction" \
    '{id:$id,operation:$operation,space:$space,direction:$direction,status:"passed"}' \
    >"$case_dir/result.json"
}

boundary_tests() {
  require_command curl
  require_command jq
  [[ -r "$SCANLOGS_CASE_MANIFEST" ]] || die "case manifest is not readable: $SCANLOGS_CASE_MANIFEST"
  [[ -r "$SCANLOGS_SPECIAL_CASE_MANIFEST" ]] || \
    die "special case manifest is not readable: $SCANLOGS_SPECIAL_CASE_MANIFEST"
  cp "$SCANLOGS_SPECIAL_CASE_MANIFEST" "${SCANLOGS_ARTIFACT_DIR}/data/special-manifest.json"

  local definition id space direction
  while IFS= read -r definition; do
    id="$(jq -er '.id' <<<"$definition")"
    while IFS= read -r space; do
      while IFS= read -r direction; do
        run_logged "boundary/$(safe_case_name "$id")/${space}-${direction}" \
          boundary_case "$definition" "$space" "$direction"
      done < <(jq -r '.directions[]' <<<"$definition")
    done < <(jq -r '.spaces[]' <<<"$definition")
  done < <(jq -c '.boundaryCases[] | select(.enabled != false)' "$SCANLOGS_SPECIAL_CASE_MANIFEST")
}

api_blackbox_case() {
  local definition=$1
  local space=$2
  local id mutation anchor endpoint prefix filter method request params case_dir guard
  id="$(jq -er '.id' <<<"$definition")"
  mutation="$(jq -er '.mutation' <<<"$definition")"
  anchor="$(special_anchor_case "$space")"
  IFS=$'\t' read -r _ endpoint _ prefix < <(case_rpc_values "$anchor")
  filter="$(jq -c '.filter' <<<"$anchor")"
  method="${prefix}_scanLogs"
  request="$(jq -nc --argjson filter "$filter" '{filter:$filter,limit:"0x1",reverse:false}')"
  params="$(jq -nc --argjson req "$request" '[$req]')"

  case "$mutation" in
    unknown-request)
      request="$(jq -nc --argjson req "$request" '$req + {unexpected:true}')"
      params="$(jq -nc --argjson req "$request" '[$req]')"
      ;;
    unknown-filter)
      request="$(jq -nc --argjson req "$request" '$req | .filter.unexpected=true')"
      params="$(jq -nc --argjson req "$request" '[$req]')"
      ;;
    unknown-range)
      if [[ "$space" == "cfx" ]]; then
        request="$(jq -nc --argjson req "$request" '$req | .filter.epochRange.unexpected=true')"
      else
        request="$(jq -nc --argjson req "$request" '$req | .filter.blockRange.unexpected=true')"
      fi
      params="$(jq -nc --argjson req "$request" '[$req]')"
      ;;
    unknown-cursor)
      request="$(jq -nc --argjson req "$request" \
        '$req + {cursor:{blockNumber:"0x1",logIndex:"0x0",unexpected:true}}')"
      params="$(jq -nc --argjson req "$request" '[$req]')"
      ;;
    unknown-assumption)
      method="${prefix}_scanLogsWithPivotAssumption"
      if [[ "$space" == "cfx" ]]; then
        guard='{ "epochNumber":"0x1", "pivotBlockHash":"0x0000000000000000000000000000000000000000000000000000000000000000", "unexpected":true }'
      else
        guard='{ "blockNumber":"0x1", "blockHash":"0x0000000000000000000000000000000000000000000000000000000000000000", "unexpected":true }'
      fi
      params="$(jq -nc --argjson req "$request" --argjson guard "$guard" '[$req,$guard]')"
      ;;
    decimal-limit)
      params="$(jq -nc --argjson req "$request" '[($req + {limit:1})]')"
      ;;
    negative-limit)
      params="$(jq -nc --argjson req "$request" '[($req + {limit:"-0x1"})]')"
      ;;
    leading-zero-limit)
      params="$(jq -nc --argjson req "$request" '[($req + {limit:"0x01"})]')"
      ;;
    nonhex-limit)
      params="$(jq -nc --argjson req "$request" '[($req + {limit:"0x1g"})]')"
      ;;
    overflow-limit)
      params="$(jq -nc --argjson req "$request" \
        '[($req + {limit:"0x10000000000000000"})]')"
      ;;
    null-filter)
      params="$(jq -nc --argjson req "$request" '[($req + {filter:null})]')"
      ;;
    wrong-reverse-type)
      params="$(jq -nc --argjson req "$request" '[($req + {reverse:"true"})]')"
      ;;
    extra-param)
      params="$(jq -nc --argjson req "$request" '[$req,null]')"
      ;;
    *) die "unsupported API mutation: $mutation" ;;
  esac

  case_dir="${SCANLOGS_ARTIFACT_DIR}/api-blackbox/$(safe_case_name "$id")/$space"
  mkdir -p "$case_dir"
  rpc_expect_error "$endpoint" "$method" "$params" -32602 \
    'invalid|unknown|argument|params|unmarshal|hex|field' "$case_dir/response.json" >/dev/null
  jq -nc --arg id "$id" --arg mutation "$mutation" --arg space "$space" \
    '{id:$id,mutation:$mutation,space:$space,status:"passed"}' >"$case_dir/result.json"
}

api_blackbox_tests() {
  require_command curl
  require_command jq
  [[ -r "$SCANLOGS_CASE_MANIFEST" ]] || die "case manifest is not readable: $SCANLOGS_CASE_MANIFEST"
  [[ -r "$SCANLOGS_SPECIAL_CASE_MANIFEST" ]] || \
    die "special case manifest is not readable: $SCANLOGS_SPECIAL_CASE_MANIFEST"
  cp "$SCANLOGS_SPECIAL_CASE_MANIFEST" "${SCANLOGS_ARTIFACT_DIR}/data/special-manifest.json"

  local definition id space
  while IFS= read -r definition; do
    id="$(jq -er '.id' <<<"$definition")"
    while IFS= read -r space; do
      run_logged "api-blackbox/$(safe_case_name "$id")/$space" \
        api_blackbox_case "$definition" "$space"
    done < <(jq -r '.spaces[]' <<<"$definition")
  done < <(jq -c '.apiCases[] | select(.enabled != false)' "$SCANLOGS_SPECIAL_CASE_MANIFEST")
}

api_security_tests() {
  api_blackbox_tests
  run_logged api/security-acl go test -count=1 -v ./util/acl -run 'ScanLogs'
  run_logged api/security-routing go test -count=1 -v ./rpc \
    -run 'ScanLogsMethodsUseDedicatedNodeGroups|ScanLogsEntryErrorCategories|ValidateScanLogsRequest'
}

consistency_tests() {
  require_command jq
  [[ -r "$SCANLOGS_CONSISTENCY_MANIFEST" ]] || \
    die "consistency manifest is not readable: $SCANLOGS_CONSISTENCY_MANIFEST"
  jq -e '
    (.cases | length) == 17 and
    ([.cases[].id] | unique | length) == 17 and
    all(.cases[]; .status == "covered" or .status == "partial" or
      .status == "missing" or .status == "documented")
  ' "$SCANLOGS_CONSISTENCY_MANIFEST" >/dev/null

  local test_name pattern report="${SCANLOGS_ARTIFACT_DIR}/fault/consistency-coverage.md"
  while IFS= read -r test_name; do
    find "$REPO_ROOT" -type f -name '*_test.go' \
      -exec grep -Eq "^func ${test_name}\\(" {} + || \
      die "mapped consistency test does not exist: $test_name"
  done < <(jq -r '[.cases[].tests[]] | unique[]' "$SCANLOGS_CONSISTENCY_MANIFEST")

  pattern="$(jq -r '[.cases[].tests[]] | unique | join("|")' "$SCANLOGS_CONSISTENCY_MANIFEST")"
  [[ -n "$pattern" ]] || die "consistency manifest has no mapped tests"
  run_logged fault/consistency-existing go test -count=1 -v ./rpc/handler -run "^(${pattern})$"

  {
    printf '# scanLogs consistency coverage\n\n'
    printf '| Case | Status | Evidence tests | Remaining gap |\n'
    printf '|---|---|---|---|\n'
    jq -r '.cases[] | "| `\(.id)` | \(.status) | \(if (.tests|length)>0 then (.tests|map("`" + . + "`")|join("<br>")) else "-" end) | \(if .gap=="" then "-" else .gap end) |"' \
      "$SCANLOGS_CONSISTENCY_MANIFEST"
    printf '\n'
    jq -r '[.cases[]] | group_by(.status)[] | "- \(.[0].status): \(length)"' \
      "$SCANLOGS_CONSISTENCY_MANIFEST"
  } >"$report"
  cp "$SCANLOGS_CONSISTENCY_MANIFEST" "${SCANLOGS_ARTIFACT_DIR}/fault/consistency-cases.json"
  printf '%s\n' "$report"
}

mysql_integration_tests() {
  require_command go
  require_mysql_config
  [[ -z "${SCANLOGS_MYSQL_DEFAULTS_FILE:-}${SCANLOGS_MYSQL_LOGIN_PATH:-}" ]] || \
    die "mysql-integration currently requires direct SCANLOGS_MYSQL_HOST/PORT/USER/PASSWORD settings"
  export SCANLOGS_MYSQL_HOST SCANLOGS_MYSQL_PORT SCANLOGS_MYSQL_USER SCANLOGS_MYSQL_PASSWORD
  SCANLOGS_RUN_MYSQL_INTEGRATION=1 run_logged integration/mysql \
    go test -count=1 -v ./rpc/handler -run '^TestScanLogsDisposableMySQLMappingStates$'
}

performance_smoke() {
  require_command curl
  require_command jq
  [[ -r "$SCANLOGS_CASE_MANIFEST" ]] || die "case manifest is not readable: $SCANLOGS_CASE_MANIFEST"
  export SCANLOGS_CASE_MANIFEST SCANLOGS_CFX_RPC SCANLOGS_ETH_RPC SCANLOGS_RPC_TIMEOUT_SECONDS
  export SCANLOGS_PERF_COLD_RUNS="${SCANLOGS_PERF_COLD_RUNS:-5}"
  export SCANLOGS_PERF_OUTPUT_DIR="${SCANLOGS_ARTIFACT_DIR}/perf/p1"
  run_logged perf/p1 "$SCRIPT_DIR/scanlogs-perf-smoke.sh"
}

performance_baseline() {
  require_command curl
  require_command jq
  require_command "$SCANLOGS_MYSQL_BIN"
  require_mysql_config
  local base perf_address perf_topic0 contract_id topic_id
  local perf_from_none perf_from_address perf_from_topic0 perf_to
  base="$(jq -ec '.cases[] | select(.enabled != false and .tier == "gate" and
    .space == "eth" and .source == "db" and .filter.address != null and
    .filter.topic0 != null)' "$SCANLOGS_CASE_MANIFEST" | head -n 1)"
  [[ -n "$base" ]] || die "no dense eSpace address/topic performance case in manifest"
  perf_address="$(jq -r '.filter.address' <<<"$base")"
  perf_topic0="$(jq -r '.filter.topic0' <<<"$base")"
  contract_id="$(mysql_scalar "$SCANLOGS_ETH_DATABASE" \
    "SELECT id FROM contracts WHERE LOWER(address)=LOWER('${perf_address}') LIMIT 1;")"
  topic_id="$(mysql_scalar "$SCANLOGS_ETH_DATABASE" \
    "SELECT id FROM topics WHERE LOWER(hash)=LOWER('${perf_topic0}') LIMIT 1;")"
  require_non_negative_integer SCANLOGS_ETH_PERF_CONTRACT_ID "$contract_id"
  require_non_negative_integer SCANLOGS_ETH_PERF_TOPIC_ID "$topic_id"
  perf_from_none="$(mysql_scalar "$SCANLOGS_ETH_DATABASE" \
    "SELECT MIN(bn_min) FROM bn_partitions WHERE entity='logs';")"
  perf_from_address="$(mysql_scalar "$SCANLOGS_ETH_DATABASE" \
    "SELECT MIN(bn_min) FROM bn_partitions WHERE entity='clogs_${contract_id}';")"
  perf_from_topic0="$(mysql_scalar "$SCANLOGS_ETH_DATABASE" \
    "SELECT MIN(bn_min) FROM bn_partitions WHERE entity='tlogs_${topic_id}';")"
  perf_to="$(mysql_scalar "$SCANLOGS_ETH_DATABASE" 'SELECT MAX(bn_max) FROM epoch_block_map;')"
  require_non_negative_integer SCANLOGS_ETH_PERF_FROM_NONE "$perf_from_none"
  require_non_negative_integer SCANLOGS_ETH_PERF_FROM_ADDRESS "$perf_from_address"
  require_non_negative_integer SCANLOGS_ETH_PERF_FROM_TOPIC0 "$perf_from_topic0"
  require_non_negative_integer SCANLOGS_ETH_PERF_TO "$perf_to"
  export SCANLOGS_CASE_MANIFEST SCANLOGS_ETH_RPC SCANLOGS_RPC_TIMEOUT_SECONDS
  export SCANLOGS_ETH_PERF_FROM_NONE="$perf_from_none"
  export SCANLOGS_ETH_PERF_FROM_ADDRESS="$perf_from_address"
  export SCANLOGS_ETH_PERF_FROM_TOPIC0="$perf_from_topic0"
  export SCANLOGS_ETH_PERF_TO="$perf_to"
  export SCANLOGS_PERF_P2_PAGES="${SCANLOGS_PERF_P2_PAGES:-500}"
  export SCANLOGS_PERF_OUTPUT_DIR="${SCANLOGS_ARTIFACT_DIR}/perf/p2"
  run_logged perf/p2 "$SCRIPT_DIR/scanlogs-perf-baseline.sh"
}

performance_large() {
  require_command jq
  require_command k6
  require_command docker
  require_command "$SCANLOGS_MYSQL_BIN"
  require_mysql_config
  local base request perf_address perf_topic0 contract_id topic_id perf_from perf_to
  local monitor_pid=0 load_status=0
  base="$(jq -ec '.cases[] | select(.enabled != false and .tier == "gate" and
    .space == "eth" and .source == "db" and .filter.address != null and
    .filter.topic0 != null)' "$SCANLOGS_CASE_MANIFEST" | head -n 1)"
  [[ -n "$base" ]] || die "no dense eSpace address/topic performance case in manifest"
  perf_address="$(jq -r '.filter.address' <<<"$base")"
  perf_topic0="$(jq -r '.filter.topic0' <<<"$base")"
  contract_id="$(mysql_scalar "$SCANLOGS_ETH_DATABASE" \
    "SELECT id FROM contracts WHERE LOWER(address)=LOWER('${perf_address}') LIMIT 1;")"
  topic_id="$(mysql_scalar "$SCANLOGS_ETH_DATABASE" \
    "SELECT id FROM topics WHERE LOWER(hash)=LOWER('${perf_topic0}') LIMIT 1;")"
  perf_from="$(mysql_scalar "$SCANLOGS_ETH_DATABASE" \
    "SELECT GREATEST(
      (SELECT MIN(bn_min) FROM bn_partitions WHERE entity='clogs_${contract_id}'),
      (SELECT MIN(bn_min) FROM bn_partitions WHERE entity='tlogs_${topic_id}'));" )"
  perf_to="$(mysql_scalar "$SCANLOGS_ETH_DATABASE" 'SELECT MAX(bn_max) FROM epoch_block_map;')"
  require_non_negative_integer SCANLOGS_ETH_PERF_FROM "$perf_from"
  require_non_negative_integer SCANLOGS_ETH_PERF_TO "$perf_to"
  request="$(jq -c --arg from "$(printf '0x%x' "$perf_from")" \
    --arg to "$(printf '0x%x' "$perf_to")" \
    '{filter:(.filter | .blockRange={fromBlock:$from,toBlock:$to}),
      limit:"0x3e8",reverse:false}' <<<"$base")"
  export SCANLOGS_PERF_P5_REQUEST="$request" SCANLOGS_ETH_RPC SCANLOGS_RPC_TIMEOUT_SECONDS
  export SCANLOGS_PERF_P5_VUS="${SCANLOGS_PERF_P5_VUS:-4}"
  export SCANLOGS_PERF_P5_DURATION="${SCANLOGS_PERF_P5_DURATION:-5m}"
  export SCANLOGS_RESOURCE_MONITOR_SECONDS="${SCANLOGS_RESOURCE_MONITOR_SECONDS:-300}"
  export SCANLOGS_RESOURCE_MONITOR_INTERVAL="${SCANLOGS_RESOURCE_MONITOR_INTERVAL:-5}"
  export SCANLOGS_RESOURCE_OUTPUT_DIR="${SCANLOGS_ARTIFACT_DIR}/perf/p5/resources"
  export SCANLOGS_MYSQL_BIN SCANLOGS_MYSQL_HOST SCANLOGS_MYSQL_PORT
  export SCANLOGS_MYSQL_USER SCANLOGS_MYSQL_PASSWORD
  export SCANLOGS_MYSQL_DEFAULTS_FILE="${SCANLOGS_MYSQL_DEFAULTS_FILE:-}"
  export SCANLOGS_MYSQL_LOGIN_PATH="${SCANLOGS_MYSQL_LOGIN_PATH:-}"
  mkdir -p "${SCANLOGS_ARTIFACT_DIR}/perf/p5"
  mysql_exec "$SCANLOGS_ETH_DATABASE" "
    SELECT NOW() AS captured_at, COUNT_STAR, SUM_ROWS_EXAMINED, SUM_ROWS_SENT,
      SUM_ERRORS, SUM_WARNINGS, DIGEST_TEXT
    FROM performance_schema.events_statements_summary_by_digest
    WHERE SCHEMA_NAME='${SCANLOGS_ETH_DATABASE}'
    ORDER BY SUM_ROWS_EXAMINED DESC LIMIT 50;" \
    >"${SCANLOGS_ARTIFACT_DIR}/perf/p5/mysql-digests-before.tsv"
  "$SCRIPT_DIR/scanlogs-resource-monitor.sh" &
  monitor_pid=$!
  k6 run --summary-export "${SCANLOGS_ARTIFACT_DIR}/perf/p5/k6-summary.json" \
    "$SCRIPT_DIR/scanlogs-perf-large.js" \
    >"${SCANLOGS_ARTIFACT_DIR}/perf/p5/k6.log" 2>&1 || load_status=$?
  wait "$monitor_pid"
  export SCANLOGS_RESOURCE_OUTPUT_DIR="${SCANLOGS_ARTIFACT_DIR}/perf/p5/cooldown"
  export SCANLOGS_RESOURCE_MONITOR_SECONDS="${SCANLOGS_RESOURCE_COOLDOWN_SECONDS:-60}"
  "$SCRIPT_DIR/scanlogs-resource-monitor.sh"
  mysql_exec "$SCANLOGS_ETH_DATABASE" "
    SELECT NOW() AS captured_at, COUNT_STAR, SUM_ROWS_EXAMINED, SUM_ROWS_SENT,
      SUM_ERRORS, SUM_WARNINGS, DIGEST_TEXT
    FROM performance_schema.events_statements_summary_by_digest
    WHERE SCHEMA_NAME='${SCANLOGS_ETH_DATABASE}'
    ORDER BY SUM_ROWS_EXAMINED DESC LIMIT 50;" \
    >"${SCANLOGS_ARTIFACT_DIR}/perf/p5/mysql-digests-after.tsv"
  (( load_status == 0 )) || die "P5 k6 load failed; see perf/p5/k6.log"
}

getlogs_regression_case() {
  local case_json=$1
  local name space proxy_endpoint fn_endpoint prefix case_dir
  name="$(jq -er '.name' <<<"$case_json")"
  IFS=$'\t' read -r space proxy_endpoint fn_endpoint prefix < <(case_rpc_values "$case_json")
  [[ -n "$proxy_endpoint" && -n "$fn_endpoint" ]] || die "RPC endpoints missing for $name"
  case_dir="${SCANLOGS_ARTIFACT_DIR}/regression/$(safe_case_name "$name")"
  mkdir -p "$case_dir"

  fetch_chunked_oracle "$case_json" "$space" "$fn_endpoint" "${prefix}_getLogs" \
    "$case_dir/fullnode.json" "$case_dir/fullnode-chunks"
  fetch_chunked_oracle "$case_json" "$space" "$proxy_endpoint" "${prefix}_getLogs" \
    "$case_dir/proxy.json" "$case_dir/proxy-chunks"
  jq '.result' "$case_dir/fullnode.json" >"$case_dir/fullnode.result.json"
  jq '.result' "$case_dir/proxy.json" >"$case_dir/proxy.result.json"
  normalize_log_array "$case_dir/fullnode.result.json" \
    "$case_dir/fullnode.normalized.json" "$space"
  normalize_log_array "$case_dir/proxy.result.json" \
    "$case_dir/proxy.normalized.json" "$space"
  if ! cmp -s "$case_dir/fullnode.normalized.json" "$case_dir/proxy.normalized.json"; then
    diff -u "$case_dir/fullnode.normalized.json" "$case_dir/proxy.normalized.json" \
      >"$case_dir/diff.txt" || true
    die "getLogs regression differs for $name"
  fi
  printf 'getLogs regression passed: %s\n' "$name"
}

regression_tests() {
  run_logged regression/go-all go test -count=1 ./...

  if [[ "$SCANLOGS_RUN_LEGACY_GETLOGS" == "1" ]]; then
    [[ -n "${TEST_CFX_FULL_NODE:-}" && -n "${TEST_CFX_INFURA_NODE:-}" ]] || \
      die "TEST_CFX_FULL_NODE and TEST_CFX_INFURA_NODE are required for legacy getLogs tests"
    run_logged regression/legacy-cfx go test -count=1 -v ./test -run '^TestGetLogs$'
  fi

  if [[ -n "${SCANLOGS_CASE_MANIFEST:-}" ]]; then
    local case_json name
    while IFS= read -r case_json; do
      name="$(safe_case_name "$(jq -er '.name' <<<"$case_json")")"
      run_logged "regression/getlogs-${name}" getlogs_regression_case "$case_json"
    done < <(jq -c '.cases[] | select(.enabled != false)' "$SCANLOGS_CASE_MANIFEST")
  fi
}

build_report() {
  local report="${SCANLOGS_ARTIFACT_DIR}/conclusion.md"
  local status_file relative exit_code failed=0
  local manifest="${SCANLOGS_ARTIFACT_DIR}/data/manifest.json"
  local enabled_cases=0 gate_cases=0 expected_results=0 e2e_results=0 e2e_passed=0
  local boundary_results=0 api_blackbox_results=0 consistency_status=missing
  local directions variants

  if [[ -r "$manifest" ]]; then
    enabled_cases="$(jq '[.cases[] | select(.enabled != false)] | length' "$manifest")"
    gate_cases="$(jq '[.cases[] | select(.enabled != false and .tier == "gate")] | length' "$manifest")"
    IFS=',' read -r -a directions <<<"$SCANLOGS_E2E_DIRECTIONS"
    IFS=',' read -r -a variants <<<"$SCANLOGS_E2E_VARIANTS"
    expected_results=$((enabled_cases * ${#directions[@]} * ${#variants[@]}))
  fi
  if [[ -d "${SCANLOGS_ARTIFACT_DIR}/e2e" ]]; then
    e2e_results="$(find "${SCANLOGS_ARTIFACT_DIR}/e2e" -name result.json | wc -l | tr -d '[:space:]')"
    e2e_passed="$(find "${SCANLOGS_ARTIFACT_DIR}/e2e" -name result.json \
      -exec jq -r 'select(.status == "passed") | 1' {} \; | wc -l | tr -d '[:space:]')"
  fi
  boundary_results="$(find "${SCANLOGS_ARTIFACT_DIR}/boundary" -name result.json 2>/dev/null | wc -l | tr -d '[:space:]')"
  api_blackbox_results="$(find "${SCANLOGS_ARTIFACT_DIR}/api-blackbox" -name result.json 2>/dev/null | wc -l | tr -d '[:space:]')"
  if [[ -r "${SCANLOGS_ARTIFACT_DIR}/fault/consistency-coverage.md" ]]; then
    consistency_status=generated
  fi

  {
    printf '# scanLogs local test conclusion\n\n'
    printf -- '- Run ID: `%s`\n' "$SCANLOGS_RUN_ID"
    printf -- '- Generated UTC: `%s`\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    printf -- '- Revision: `%s`\n\n' "$(head -n 1 "${SCANLOGS_ARTIFACT_DIR}/env/git-revision.txt" 2>/dev/null || printf unknown)"
    printf '| Phase | Exit code | Result |\n'
    printf '|---|---:|---|\n'
  } >"$report"

  while IFS= read -r status_file; do
    relative="${status_file#${SCANLOGS_ARTIFACT_DIR}/}"
    relative="${relative%.status}"
    exit_code="$(tr -d '[:space:]' <"$status_file")"
    if [[ "$exit_code" == "0" ]]; then
      printf '| `%s` | %s | PASS |\n' "$relative" "$exit_code" >>"$report"
    else
      printf '| `%s` | %s | FAIL |\n' "$relative" "$exit_code" >>"$report"
      failed=1
    fi
  done < <(find "$SCANLOGS_ARTIFACT_DIR" -type f -name '*.status' | sort)

  {
    printf '\n## Manual completion fields\n\n'
    printf -- '- DDL/EXPLAIN conclusion: TODO\n'
    if ((expected_results > 0 && e2e_results == expected_results && e2e_passed == e2e_results)); then
      printf -- '- E2E case count and oracle conclusion: PASS (%d/%d executions; %d/%d release-gate executions); all results matched the getLogs oracle\n' \
        "$e2e_passed" "$expected_results" "$((gate_cases * ${#directions[@]} * ${#variants[@]}))" \
        "$((gate_cases * ${#directions[@]} * ${#variants[@]}))"
    else
      printf -- '- E2E case count and oracle conclusion: INCOMPLETE (%d passed result artifacts, %d expected)\n' \
        "$e2e_passed" "$expected_results"
    fi
    printf -- '- Retry/boundary/cache conclusion: TODO\n'
    printf -- '- Extended correctness: boundary=%s, api-blackbox=%s, consistency-map=%s\n' \
      "$boundary_results" "$api_blackbox_results" "$consistency_status"
    printf -- '- Performance p50/p95/p99 and resource conclusion: TODO\n'
    printf -- '- Staging read-only conclusion: TODO\n'
    printf -- '- Known limitation (FN ABA): acknowledged\n'
    printf -- '- Final recommendation: TODO (`GO`, `CONDITIONAL GO`, or `NO-GO`)\n'
  } >>"$report"

  printf '%s\n' "$report"
  return "$failed"
}

all_readonly() {
  run_logged env/preflight preflight
  unit_tests
  fault_tests
  capture_watermarks
  run_ddl_mode plan plan-readonly
  rpc_smoke
  rpc_negative
  boundary_tests
  api_blackbox_tests
  consistency_tests
  e2e_tests
  regression_tests
  build_report
}

if [[ "$COMMAND" == "help" ]]; then
  usage
  exit 0
fi

init_run

case "$COMMAND" in
  init)
    ;;
  preflight)
    run_logged env/preflight preflight
    ;;
  unit)
    unit_tests
    ;;
  fault)
    fault_tests
    ;;
  watermarks)
    capture_watermarks
    ;;
  ddl-plan)
    run_ddl_mode plan "${SCANLOGS_DDL_PHASE_LABEL:-plan}"
    ;;
  ddl-add)
    run_ddl_mode add "${SCANLOGS_DDL_PHASE_LABEL:-add}"
    ;;
  ddl-verify)
    run_ddl_mode verify "${SCANLOGS_DDL_PHASE_LABEL:-verify}"
    ;;
  ddl-drop)
    run_ddl_mode drop "${SCANLOGS_DDL_PHASE_LABEL:-drop}"
    ;;
  ddl-cycle)
    ddl_cycle
    ;;
  rpc-smoke)
    rpc_smoke
    ;;
  rpc-negative)
    rpc_negative
    ;;
  e2e)
    e2e_tests
    ;;
  e2e-boundary)
    boundary_tests
    ;;
  api-blackbox)
    api_blackbox_tests
    ;;
  api-security)
    api_security_tests
    ;;
  consistency)
    consistency_tests
    ;;
  mysql-integration)
    mysql_integration_tests
    ;;
  perf-smoke)
    performance_smoke
    ;;
  perf-baseline)
    performance_baseline
    ;;
  perf-large)
    run_logged perf/p5-phase performance_large
    ;;
  regression)
    regression_tests
    ;;
  all-readonly)
    all_readonly
    ;;
  report)
    build_report
    ;;
  *)
    usage >&2
    die "unknown command: $COMMAND" 2
    ;;
esac
