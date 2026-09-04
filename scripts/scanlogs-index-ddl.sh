#!/usr/bin/env bash

set -Eeuo pipefail

readonly SCRIPT_NAME="${0##*/}"
readonly UINT64_MAX="18446744073709551615"

MODE="plan"
DATABASE=""
ADDRESS_PARTITIONS=""
TOPIC_PARTITIONS=""
MYSQL_BIN="mysql"
DEFAULTS_EXTRA_FILE=""
LOGIN_PATH=""
MYSQL_HOST=""
MYSQL_PORT=""
MYSQL_USER=""
EXECUTE=false
PAUSE_SECONDS=0
MYSQL_QUERY_ABORT_RETRIES=3

usage() {
  cat <<'USAGE'
Usage:
  scanlogs-index-ddl.sh --database NAME \
    --address-partitions N --topic-partitions N \
    [--host HOST --port PORT --user USER] \
    [--defaults-extra-file FILE | --login-path NAME] \
    [--mysql-bin PATH] [--pause-seconds N] \
    --mode plan|add|verify|drop [--execute]

Modes:
  plan    Read-only inventory. Print required ADD/DROP statements.
  add     Add missing scanLogs indexes. Requires --execute.
  verify  Read-only index and EXPLAIN verification.
  drop    Re-verify, drop replaced indexes, then verify again. Requires --execute.

For direct credentials, pass host/port/user and provide the password through
MYSQL_PWD. A defaults file or login path can still be used instead.
USAGE
}

log() {
  printf '%s [%s] %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$1" "$2"
}

die() {
  log ERROR "$1" >&2
  exit "${2:-1}"
}

on_error() {
  local exit_code=$?
  log ERROR "command failed at line ${BASH_LINENO[0]} (exit ${exit_code})" >&2
  exit "$exit_code"
}

trap on_error ERR

while (($# > 0)); do
  case "$1" in
    --database)
      DATABASE="${2:-}"
      shift 2
      ;;
    --address-partitions)
      ADDRESS_PARTITIONS="${2:-}"
      shift 2
      ;;
    --topic-partitions)
      TOPIC_PARTITIONS="${2:-}"
      shift 2
      ;;
    --defaults-extra-file)
      DEFAULTS_EXTRA_FILE="${2:-}"
      shift 2
      ;;
    --login-path)
      LOGIN_PATH="${2:-}"
      shift 2
      ;;
    --host)
      MYSQL_HOST="${2:-}"
      shift 2
      ;;
    --port)
      MYSQL_PORT="${2:-}"
      shift 2
      ;;
    --user)
      MYSQL_USER="${2:-}"
      shift 2
      ;;
    --mysql-bin)
      MYSQL_BIN="${2:-}"
      shift 2
      ;;
    --pause-seconds)
      PAUSE_SECONDS="${2:-}"
      shift 2
      ;;
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    --execute)
      EXECUTE=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage >&2
      die "unknown argument: $1" 2
      ;;
  esac
done

[[ "$DATABASE" =~ ^[A-Za-z0-9_]+$ ]] || die "--database must match [A-Za-z0-9_]+" 2
[[ "$ADDRESS_PARTITIONS" =~ ^[0-9]+$ ]] || die "--address-partitions must be a non-negative integer" 2
[[ "$TOPIC_PARTITIONS" =~ ^[0-9]+$ ]] || die "--topic-partitions must be a non-negative integer" 2
[[ "$PAUSE_SECONDS" =~ ^[0-9]+$ ]] || die "--pause-seconds must be a non-negative integer" 2
[[ "$MODE" =~ ^(plan|add|verify|drop)$ ]] || die "--mode must be plan, add, verify, or drop" 2
[[ -z "$DEFAULTS_EXTRA_FILE" || -z "$LOGIN_PATH" ]] || die "--defaults-extra-file and --login-path are mutually exclusive" 2
if [[ -z "$DEFAULTS_EXTRA_FILE$LOGIN_PATH" ]]; then
  [[ -n "$MYSQL_HOST" ]] || die "--host is required for direct credentials" 2
  [[ "$MYSQL_PORT" =~ ^[0-9]+$ ]] || die "--port must be an integer" 2
  [[ -n "$MYSQL_USER" ]] || die "--user is required for direct credentials" 2
fi

if [[ -n "$DEFAULTS_EXTRA_FILE" ]]; then
  [[ -r "$DEFAULTS_EXTRA_FILE" ]] || die "defaults extra file is not readable: $DEFAULTS_EXTRA_FILE" 2
fi
if [[ "$MODE" == "add" || "$MODE" == "drop" ]]; then
  $EXECUTE || die "--mode $MODE requires --execute" 2
fi
if [[ "$MODE" == "plan" || "$MODE" == "verify" ]]; then
  $EXECUTE && die "--execute is not valid with read-only mode $MODE" 2
fi

command -v "$MYSQL_BIN" >/dev/null 2>&1 || die "mysql client not found: $MYSQL_BIN" 2

MYSQL_COMMAND=("$MYSQL_BIN")
if [[ -n "$DEFAULTS_EXTRA_FILE" ]]; then
  MYSQL_COMMAND+=("--defaults-extra-file=$DEFAULTS_EXTRA_FILE")
elif [[ -n "$LOGIN_PATH" ]]; then
  MYSQL_COMMAND+=("--login-path=$LOGIN_PATH")
else
  MYSQL_COMMAND+=("--host=$MYSQL_HOST" "--port=$MYSQL_PORT" "--user=$MYSQL_USER")
fi
MYSQL_COMMAND+=("--database=$DATABASE" --batch --raw --skip-column-names)

TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/scanlogs-index-ddl.XXXXXX")"
trap 'rm -rf "$TEMP_DIR"' EXIT
TABLES_SNAPSHOT="$TEMP_DIR/tables.tsv"
INDEXES_SNAPSHOT="$TEMP_DIR/indexes.tsv"

mysql_query() {
  local sql=$1
  local attempt=1
  local exit_code
  local output_file="${TEMP_DIR}/mysql-query.out"

  while ((attempt <= MYSQL_QUERY_ABORT_RETRIES)); do
    : >"$output_file"
    if "${MYSQL_COMMAND[@]}" --execute="$sql" </dev/null >"$output_file"; then
      cat "$output_file"
      return 0
    else
      exit_code=$?
    fi

    if ((exit_code != 134 || attempt == MYSQL_QUERY_ABORT_RETRIES)); then
      return "$exit_code"
    fi
    log WARN "mysql client aborted (exit 134); retrying query attempt $((attempt + 1))/${MYSQL_QUERY_ABORT_RETRIES}" >&2
    attempt=$((attempt + 1))
  done
}

refresh_schema_snapshot() {
  mysql_query "SELECT table_name FROM information_schema.tables WHERE table_schema = '$DATABASE' AND table_type = 'BASE TABLE' ORDER BY table_name" >"$TABLES_SNAPSHOT"
  mysql_query "SELECT table_name, index_name, CONCAT(MAX(non_unique), '|', MAX(index_type), '|', GROUP_CONCAT(column_name ORDER BY seq_in_index SEPARATOR ','), '|', SUM(CASE WHEN sub_part IS NULL THEN 0 ELSE 1 END), '|', GROUP_CONCAT(COALESCE(collation, '') ORDER BY seq_in_index SEPARATOR ',')) FROM information_schema.statistics WHERE table_schema = '$DATABASE' GROUP BY table_name, index_name ORDER BY table_name, index_name" >"$INDEXES_SNAPSHOT"
}

quote_identifier() {
  [[ "$1" =~ ^[A-Za-z0-9_]+$ ]] || die "unsafe SQL identifier: $1"
  printf '`%s`' "$1"
}

join_by_comma() {
  local IFS=', '
  printf '%s' "$*"
}

quote_columns() {
  local columns=$1
  local column
  local quoted=()
  IFS=',' read -r -a raw_columns <<<"$columns"
  for column in "${raw_columns[@]}"; do
    quoted+=("$(quote_identifier "$column")")
  done
  join_by_comma "${quoted[@]}"
}

desired_specs() {
  case "$1" in
    logs|tlogs)
      printf '%s\n' 'idx_bn_li|bn,log_index'
      ;;
    addr)
      printf '%s\n' \
        'idx_cid_bn_li|cid,bn,log_index' \
        'idx_cid_tid_bn_li|cid,tid,bn,log_index'
      ;;
    topic)
      printf '%s\n' 'idx_tid_bn_li|tid,bn,log_index'
      ;;
    clogs)
      printf '%s\n' \
        'idx_bn_li|bn,log_index' \
        'idx_tid_bn_li|tid,bn,log_index'
      ;;
    *)
      die "unsupported table family: $1"
      ;;
  esac
}

replaced_specs() {
  case "$1" in
    logs|tlogs)
      printf '%s\n' 'idx_bn|bn'
      ;;
    addr)
      printf '%s\n' 'idx_cid_bn|cid,bn' 'idx_cid_tid_bn|cid,tid,bn'
      ;;
    topic)
      printf '%s\n' 'idx_tid_bn|tid,bn'
      ;;
    clogs)
      printf '%s\n' 'idx_bn|bn' 'idx_tid_bn|tid,bn'
      ;;
    *)
      die "unsupported table family: $1"
      ;;
  esac
}

explain_specs() {
  case "$1" in
    logs)
      printf '%s\n' 'universal|idx_bn_li|'
      ;;
    addr)
      printf '%s\n' \
        'address|idx_cid_bn_li|cid' \
        'address_topic0|idx_cid_tid_bn_li|cid,tid'
      ;;
    topic)
      printf '%s\n' 'topic0|idx_tid_bn_li|tid'
      ;;
    clogs)
      printf '%s\n' \
        'dedicated_address|idx_bn_li|' \
        'dedicated_address_topic0|idx_tid_bn_li|tid'
      ;;
    tlogs)
      printf '%s\n' 'dedicated_topic0|idx_bn_li|'
      ;;
    *)
      die "unsupported table family: $1"
      ;;
  esac
}

prepare_spec_files() {
  local family
  for family in logs addr topic clogs tlogs; do
    desired_specs "$family" >"$TEMP_DIR/desired-$family.tsv"
    replaced_specs "$family" >"$TEMP_DIR/replaced-$family.tsv"
    explain_specs "$family" >"$TEMP_DIR/explain-$family.tsv"
  done
}

table_exists() {
  local table=$1
  awk -v table="$table" '$1 == table { found = 1 } END { exit !found }' "$TABLES_SNAPSHOT"
}

discover_targets() {
  local output_file=$1
  local unsorted_file="$TEMP_DIR/targets.unsorted"
  local metadata_file="$TEMP_DIR/bn-partitions.tsv"
  local hash_tables_file="$TEMP_DIR/hash-tables.tsv"
  local bn_tables_file="$TEMP_DIR/bn-tables.tsv"
  local entity
  local partition
  local table
  local i

  : >"$unsorted_file"
  table_exists bn_partitions || die "required metadata table does not exist: $DATABASE.bn_partitions"

  mysql_query "SELECT entity, pi FROM $(quote_identifier bn_partitions) WHERE entity = 'logs' OR entity REGEXP '^(clogs|tlogs)_[0-9]+$' ORDER BY entity, pi" >"$metadata_file"
  while IFS=$'\t' read -r entity partition; do
    [[ -n "$entity" ]] || continue
    [[ "$partition" =~ ^[0-9]+$ ]] || die "invalid bn_partitions.pi for entity $entity: $partition"
    case "$entity" in
      logs)
        printf 'logs\tlogs_%s\n' "$partition" >>"$unsorted_file"
        ;;
      clogs_[0-9]*)
        [[ "$entity" =~ ^clogs_[0-9]+$ ]] || die "invalid dedicated contract entity: $entity"
        printf 'clogs\t%s_%s\n' "$entity" "$partition" >>"$unsorted_file"
        ;;
      tlogs_[0-9]*)
        [[ "$entity" =~ ^tlogs_[0-9]+$ ]] || die "invalid dedicated topic entity: $entity"
        printf 'tlogs\t%s_%s\n' "$entity" "$partition" >>"$unsorted_file"
        ;;
    esac
  done <"$metadata_file"

  for ((i = 0; i < ADDRESS_PARTITIONS; i++)); do
    printf 'addr\taddr_logs_%s\n' "$i" >>"$unsorted_file"
  done
  for ((i = 0; i < TOPIC_PARTITIONS; i++)); do
    printf 'topic\ttopic_logs_%s\n' "$i" >>"$unsorted_file"
  done

  LC_ALL=C sort -u "$unsorted_file" >"$output_file"

  while IFS=$'\t' read -r _ table; do
    [[ -n "$table" ]] || continue
    table_exists "$table" || die "expected physical table does not exist: $DATABASE.$table"
  done <"$output_file"

  awk '$1 ~ /^(addr_logs|topic_logs)_[0-9]+$/ { print $1 }' "$TABLES_SNAPSHOT" >"$hash_tables_file"
  while IFS= read -r table; do
    [[ -n "$table" ]] || continue
    if ! awk -F '\t' -v table="$table" '$2 == table { found = 1 } END { exit !found }' "$output_file"; then
      die "unexpected hash-partitioned log table outside configured range: $DATABASE.$table"
    fi
  done <"$hash_tables_file"

  awk '$1 ~ /^logs_[0-9]+$/ || $1 ~ /^clogs_[0-9]+_[0-9]+$/ || $1 ~ /^tlogs_[0-9]+_[0-9]+$/ { print $1 }' "$TABLES_SNAPSHOT" >"$bn_tables_file"
  while IFS= read -r table; do
    [[ -n "$table" ]] || continue
    if ! awk -F '\t' -v table="$table" '$2 == table { found = 1 } END { exit !found }' "$output_file"; then
      log WARN "orphan bn-partitioned table is not in bn_partitions and will not be modified: $table"
    fi
  done <"$bn_tables_file"
}

INDEX_SIGNATURE_RESULT=""

index_signature() {
  local table=$1
  local index_name=$2
  local result_file="$TEMP_DIR/index-signature.result"

  INDEX_SIGNATURE_RESULT=""
  awk -F '\t' -v table="$table" -v idx="$index_name" \
    '$1 == table && $2 == idx { print $3; exit }' "$INDEXES_SNAPSHOT" >"$result_file"
  IFS= read -r INDEX_SIGNATURE_RESULT <"$result_file" || true
}

expected_signature() {
  local columns=$1
  local column
  local collations=()
  IFS=',' read -r -a raw_columns <<<"$columns"
  for column in "${raw_columns[@]}"; do
    collations+=(A)
  done
  printf '1|BTREE|%s|0|%s' "$columns" "$(IFS=,; echo "${collations[*]}")"
}

assert_index_definition() {
  local table=$1
  local index=$2
  local columns=$3
  local allow_missing=${4:-false}
  local actual
  local expected

  index_signature "$table" "$index"
  actual="$INDEX_SIGNATURE_RESULT"
  if [[ -z "$actual" ]]; then
    $allow_missing && return 1
    die "missing required index $DATABASE.$table.$index"
  fi

  expected="$(expected_signature "$columns")"
  [[ "$actual" == "$expected" ]] || die "index definition mismatch for $DATABASE.$table.$index: expected $expected, got $actual"
}

validate_desired_indexes() {
  local targets=$1
  local family
  local table
  local spec
  local index
  local columns
  while IFS=$'\t' read -r family table; do
    while IFS='|' read -r index columns; do
      assert_index_definition "$table" "$index" "$columns"
    done <"$TEMP_DIR/desired-$family.tsv"
  done <"$targets"
}

validate_replaced_indexes() {
  local targets=$1
  local family
  local table
  local index
  local columns
  local actual
  local expected
  while IFS=$'\t' read -r family table; do
    while IFS='|' read -r index columns; do
      index_signature "$table" "$index"
      actual="$INDEX_SIGNATURE_RESULT"
      [[ -n "$actual" ]] || continue
      expected="$(expected_signature "$columns")"
      [[ "$actual" == "$expected" ]] || die "replaced index definition mismatch for $DATABASE.$table.$index: expected $expected, got $actual"
    done <"$TEMP_DIR/replaced-$family.tsv"
  done <"$targets"
}

execute_or_print() {
  local sql=$1
  if [[ "$MODE" == "plan" ]]; then
    printf '%s\n' "$sql"
    return
  fi
  log INFO "$sql"
  mysql_query "$sql" >/dev/null
  refresh_schema_snapshot
  if ((PAUSE_SECONDS > 0)); then
    sleep "$PAUSE_SECONDS"
  fi
}

add_missing_indexes() {
  local targets=$1
  local family
  local table
  local index
  local columns
  local actual
  local expected
  local clauses
  local clause_array
  local sql

  while IFS=$'\t' read -r family table; do
    clause_array=()
    while IFS='|' read -r index columns; do
      index_signature "$table" "$index"
      actual="$INDEX_SIGNATURE_RESULT"
      if [[ -n "$actual" ]]; then
        expected="$(expected_signature "$columns")"
        [[ "$actual" == "$expected" ]] || die "index definition mismatch for $DATABASE.$table.$index: expected $expected, got $actual"
        log INFO "ADD skipped; index already correct: $table.$index"
        continue
      fi
      clause_array+=("ADD INDEX $(quote_identifier "$index") ($(quote_columns "$columns"))")
    done <"$TEMP_DIR/desired-$family.tsv"

    ((${#clause_array[@]} > 0)) || continue
    clauses="$(join_by_comma "${clause_array[@]}")"
    sql="ALTER TABLE $(quote_identifier "$table") $clauses, ALGORITHM=INPLACE, LOCK=NONE;"
    execute_or_print "$sql"
  done <"$targets"
}

print_replaced_index_drops() {
  local targets=$1
  local family
  local table
  local index
  local columns
  local actual
  local expected
  local clauses
  local clause_array
  local sql

  while IFS=$'\t' read -r family table; do
    clause_array=()
    while IFS='|' read -r index columns; do
      index_signature "$table" "$index"
      actual="$INDEX_SIGNATURE_RESULT"
      [[ -n "$actual" ]] || continue
      expected="$(expected_signature "$columns")"
      [[ "$actual" == "$expected" ]] || die "replaced index definition mismatch for $DATABASE.$table.$index: expected $expected, got $actual"
      clause_array+=("DROP INDEX $(quote_identifier "$index")")
    done <"$TEMP_DIR/replaced-$family.tsv"

    ((${#clause_array[@]} > 0)) || continue
    clauses="$(join_by_comma "${clause_array[@]}")"
    sql="ALTER TABLE $(quote_identifier "$table") $clauses, ALGORITHM=INPLACE, LOCK=NONE;"
    printf '%s\n' "$sql"
  done <"$targets"
}

drop_replaced_indexes() {
  local targets=$1
  local family
  local table
  local index
  local columns
  local actual
  local clauses
  local clause_array
  local sql

  while IFS=$'\t' read -r family table; do
    clause_array=()
    while IFS='|' read -r index columns; do
      index_signature "$table" "$index"
      actual="$INDEX_SIGNATURE_RESULT"
      [[ -n "$actual" ]] || continue
      clause_array+=("DROP INDEX $(quote_identifier "$index")")
    done <"$TEMP_DIR/replaced-$family.tsv"

    ((${#clause_array[@]} > 0)) || continue
    clauses="$(join_by_comma "${clause_array[@]}")"
    sql="ALTER TABLE $(quote_identifier "$table") $clauses, ALGORITHM=INPLACE, LOCK=NONE;"
    execute_or_print "$sql"
  done <"$targets"
}

validate_replaced_indexes_absent() {
  local targets=$1
  local family
  local table
  local index
  local columns
  local actual
  while IFS=$'\t' read -r family table; do
    while IFS='|' read -r index columns; do
      index_signature "$table" "$index"
      actual="$INDEX_SIGNATURE_RESULT"
      [[ -z "$actual" ]] || die "replaced index still exists: $DATABASE.$table.$index"
    done <"$TEMP_DIR/replaced-$family.tsv"
  done <"$targets"
}

sample_number() {
  local table=$1
  local column=$2
  local value
  value="$(mysql_query "SELECT $(quote_identifier "$column") FROM $(quote_identifier "$table") WHERE $(quote_identifier "$column") IS NOT NULL LIMIT 1")"
  value="${value:-0}"
  [[ "$value" =~ ^[0-9]+$ ]] || die "non-numeric sample from $table.$column: $value"
  printf '%s' "$value"
}

run_one_explain() {
  local table=$1
  local label=$2
  local expected_index=$3
  local equality_columns=$4
  local direction=$5
  local table_hint=$6
  local bn
  local log_index
  local cursor_sample
  local column
  local value
  local equality_sql=""
  local cursor_sql
  local order_sql
  local query
  local result
  local id select_type explained_table partitions access_type possible_keys key key_len ref rows filtered extra

  if [[ -n "$equality_columns" ]]; then
    IFS=',' read -r -a raw_columns <<<"$equality_columns"
    for column in "${raw_columns[@]}"; do
      value="$(sample_number "$table" "$column")"
      equality_sql+="$(quote_identifier "$column") = $value AND "
    done
  fi

  if [[ "$direction" == "forward" ]]; then
    order_sql="$(quote_identifier bn) ASC, $(quote_identifier log_index) ASC"
  else
    order_sql="$(quote_identifier bn) DESC, $(quote_identifier log_index) DESC"
  fi

  cursor_sample="$(mysql_query "SELECT $(quote_identifier bn), $(quote_identifier log_index) FROM $(quote_identifier "$table") WHERE ${equality_sql}1 = 1 ORDER BY $order_sql LIMIT 1")"
  IFS=$'\t' read -r bn log_index <<<"$cursor_sample"
  [[ "$bn" =~ ^[0-9]+$ && "$log_index" =~ ^[0-9]+$ ]] || die "invalid cursor sample for $table/$label/$direction: $cursor_sample"

  if [[ "$direction" == "forward" ]]; then
    cursor_sql="($(quote_identifier bn) > $bn OR ($(quote_identifier bn) = $bn AND $(quote_identifier log_index) > $log_index))"
  else
    cursor_sql="($(quote_identifier bn) < $bn OR ($(quote_identifier bn) = $bn AND $(quote_identifier log_index) < $log_index))"
  fi

  table_hint="${table_hint:+$table_hint }FORCE INDEX ($(quote_identifier "$expected_index"))"

  query="EXPLAIN SELECT * FROM $(quote_identifier "$table")${table_hint:+ $table_hint} WHERE ${equality_sql}$(quote_identifier bn) BETWEEN 0 AND $UINT64_MAX AND $cursor_sql ORDER BY $order_sql LIMIT 100"
  result="$(mysql_query "$query")"
  [[ -n "$result" && "$result" != *$'\n'* ]] || die "unexpected EXPLAIN result for $table/$label/$direction: $result"

  IFS=$'\t' read -r id select_type explained_table partitions access_type possible_keys key key_len ref rows filtered extra <<<"$result"
  [[ "$extra" != *"Using filesort"* ]] || die "EXPLAIN uses filesort for $table/$label/$direction; key=$key expected_key=$expected_index access=$access_type rows=$rows extra=$extra; query: $query"
  [[ "$key" == "$expected_index" ]] || die "EXPLAIN chose key $key instead of $expected_index for $table/$label/$direction; access=$access_type rows=$rows extra=$extra; query: $query"
  log INFO "EXPLAIN passed: table=$table route=$label direction=$direction key=$key extra=$extra"
}

verify_explains() {
  local targets=$1
  local ignore_replaced=${2:-false}
  local family
  local table
  local label
  local expected_index
  local equality_columns
  local direction
  local has_rows
  local old_index
  local old_columns
  local old_signature
  local table_hint
  local ignore_indexes

  while IFS=$'\t' read -r family table; do
    has_rows="$(mysql_query "SELECT EXISTS(SELECT 1 FROM $(quote_identifier "$table") LIMIT 1)")"
    [[ "$has_rows" == "0" || "$has_rows" == "1" ]] || die "unexpected row-existence result for $table: $has_rows"
    if [[ "$has_rows" == "0" ]]; then
      log INFO "EXPLAIN skipped for empty table after index definition validation: $table"
      continue
    fi

    table_hint=""
    if $ignore_replaced; then
      ignore_indexes=()
      while IFS='|' read -r old_index old_columns; do
        index_signature "$table" "$old_index"
        old_signature="$INDEX_SIGNATURE_RESULT"
        [[ -n "$old_signature" ]] || continue
        ignore_indexes+=("$(quote_identifier "$old_index")")
      done <"$TEMP_DIR/replaced-$family.tsv"
      if ((${#ignore_indexes[@]} > 0)); then
        table_hint="IGNORE INDEX ($(join_by_comma "${ignore_indexes[@]}"))"
        log INFO "EXPLAIN simulates removal of replaced indexes on $table: $table_hint"
      fi
    fi

    while IFS='|' read -r label expected_index equality_columns; do
      for direction in forward reverse; do
        run_one_explain "$table" "$label" "$expected_index" "$equality_columns" "$direction" "$table_hint"
      done
    done <"$TEMP_DIR/explain-$family.tsv"
  done <"$targets"
}

inventory_summary() {
  local targets=$1
  local total
  total="$(wc -l <"$targets" | tr -d ' ')"
  log INFO "inventory contains $total active log tables"
  for family in logs addr topic clogs tlogs; do
    count="$(awk -F '\t' -v family="$family" '$1 == family { count++ } END { print count + 0 }' "$targets")"
    log INFO "inventory family=$family tables=$count"
  done
}

preflight() {
  local selected_database
  local version
  selected_database="$(mysql_query 'SELECT DATABASE()')"
  [[ "$selected_database" == "$DATABASE" ]] || die "connected database mismatch: expected $DATABASE, got $selected_database"
  version="$(mysql_query 'SELECT VERSION()')"
  log INFO "connected database=$DATABASE mysql_version=$version mode=$MODE"
}

TARGETS_A="$TEMP_DIR/targets-a.tsv"
TARGETS_B="$TEMP_DIR/targets-b.tsv"

preflight
prepare_spec_files
refresh_schema_snapshot
discover_targets "$TARGETS_A"
inventory_summary "$TARGETS_A"

case "$MODE" in
  plan)
    log INFO "planned ADD statements"
    add_missing_indexes "$TARGETS_A"
    log INFO "planned DROP statements (only after a successful add and verify phase)"
    print_replaced_index_drops "$TARGETS_A"
    ;;
  add)
    for pass in 1 2 3; do
      log INFO "ADD pass $pass"
      add_missing_indexes "$TARGETS_A"
      validate_desired_indexes "$TARGETS_A"
      discover_targets "$TARGETS_B"
      if cmp -s "$TARGETS_A" "$TARGETS_B"; then
        log INFO "ADD completed with a stable table inventory"
        break
      fi
      if ((pass == 3)); then
        die "table inventory did not stabilize after 3 ADD passes"
      fi
      log WARN "table inventory changed during ADD; processing newly discovered tables"
      cp "$TARGETS_B" "$TARGETS_A"
    done
    ;;
  verify)
    validate_desired_indexes "$TARGETS_A"
    validate_replaced_indexes "$TARGETS_A"
    verify_explains "$TARGETS_A" true
    discover_targets "$TARGETS_B"
    cmp -s "$TARGETS_A" "$TARGETS_B" || die "table inventory changed during verification; rerun add and verify"
    log INFO "verification completed successfully"
    ;;
  drop)
    validate_desired_indexes "$TARGETS_A"
    validate_replaced_indexes "$TARGETS_A"
    verify_explains "$TARGETS_A" true
    discover_targets "$TARGETS_B"
    cmp -s "$TARGETS_A" "$TARGETS_B" || die "table inventory changed before DROP; rerun add and verify"

    drop_replaced_indexes "$TARGETS_A"

    discover_targets "$TARGETS_B"
    validate_desired_indexes "$TARGETS_B"
    validate_replaced_indexes_absent "$TARGETS_B"
    verify_explains "$TARGETS_B" false
    log INFO "DROP and final verification completed successfully"
    ;;
esac

log INFO "$SCRIPT_NAME finished successfully"
