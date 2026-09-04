#!/usr/bin/env bash

set -Eeuo pipefail

: "${SCANLOGS_RESOURCE_OUTPUT_DIR:?SCANLOGS_RESOURCE_OUTPUT_DIR is required}"
: "${SCANLOGS_RESOURCE_MONITOR_SECONDS:=300}"
: "${SCANLOGS_RESOURCE_MONITOR_INTERVAL:=5}"
: "${SCANLOGS_MYSQL_BIN:=mysql}"

mkdir -p "$SCANLOGS_RESOURCE_OUTPUT_DIR"
docker_samples="$SCANLOGS_RESOURCE_OUTPUT_DIR/docker-stats.jsonl"
mysql_samples="$SCANLOGS_RESOURCE_OUTPUT_DIR/mysql-status.tsv"
: >"$docker_samples"
printf 'timestamp\tThreads_connected\tThreads_running\tConnections\tAborted_connects\tCreated_tmp_disk_tables\tSlow_queries\n' >"$mysql_samples"

mysql_status() {
  local auth_args=()
  if [[ -n "${SCANLOGS_MYSQL_DEFAULTS_FILE:-}" ]]; then
    auth_args+=("--defaults-extra-file=${SCANLOGS_MYSQL_DEFAULTS_FILE}")
  elif [[ -n "${SCANLOGS_MYSQL_LOGIN_PATH:-}" ]]; then
    auth_args+=("--login-path=${SCANLOGS_MYSQL_LOGIN_PATH}")
  else
    auth_args+=("--host=${SCANLOGS_MYSQL_HOST}" "--port=${SCANLOGS_MYSQL_PORT}" \
      "--user=${SCANLOGS_MYSQL_USER}")
  fi
  MYSQL_PWD="${SCANLOGS_MYSQL_PASSWORD:-}" "$SCANLOGS_MYSQL_BIN" "${auth_args[@]}" \
    --batch --raw --skip-column-names --execute="
      SELECT
        MAX(IF(VARIABLE_NAME='Threads_connected',VARIABLE_VALUE,NULL)),
        MAX(IF(VARIABLE_NAME='Threads_running',VARIABLE_VALUE,NULL)),
        MAX(IF(VARIABLE_NAME='Connections',VARIABLE_VALUE,NULL)),
        MAX(IF(VARIABLE_NAME='Aborted_connects',VARIABLE_VALUE,NULL)),
        MAX(IF(VARIABLE_NAME='Created_tmp_disk_tables',VARIABLE_VALUE,NULL)),
        MAX(IF(VARIABLE_NAME='Slow_queries',VARIABLE_VALUE,NULL))
      FROM performance_schema.global_status
      WHERE VARIABLE_NAME IN ('Threads_connected','Threads_running','Connections',
        'Aborted_connects','Created_tmp_disk_tables','Slow_queries');"
}

end=$((SECONDS + SCANLOGS_RESOURCE_MONITOR_SECONDS))
while (( SECONDS <= end )); do
  timestamp="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  docker stats --no-stream --format '{{json .}}' \
    confura-ethrpc confura-ethvirtual-filter confura-database 2>/dev/null |
    jq -c --arg timestamp "$timestamp" '. + {timestamp:$timestamp}' >>"$docker_samples"
  printf '%s\t%s\n' "$timestamp" "$(mysql_status)" >>"$mysql_samples"
  (( SECONDS + SCANLOGS_RESOURCE_MONITOR_INTERVAL > end )) && break
  sleep "$SCANLOGS_RESOURCE_MONITOR_INTERVAL"
done
