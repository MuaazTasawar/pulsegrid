#!/bin/sh
# shellcheck disable=SC2012,SC2015 # dump names are generated here; "A && B || die" is intended
# PulseGrid database backups.
#
# Runs as the `backup` service of docker-compose.prod.yml (image
# postgres:16-alpine, so busybox sh and a pg_dump that matches the server).
#
# What it does, in a loop:
#   1. dumps the database when the newest dump is older than the interval
#      (so restarting the container, or deploying, never causes a needless dump);
#   2. checks that the new dump is readable, and only then keeps it;
#   3. deletes dumps older than BACKUP_KEEP_DAYS, but never the newest three;
#   4. if OFFSITE_REMOTE is set, copies new dumps to that rclone remote and
#      deletes remote dumps older than OFFSITE_KEEP_DAYS;
#   5. publishes what happened as Prometheus metrics (see alerts.yml). The
#      metrics server is optional: if it cannot start, backups carry on.
# After a failure it retries after BACKUP_RETRY_SECONDS instead of waiting a
# whole interval.
#
# One dump right now, then exit (for example before a risky deploy):
#   $COMPOSE run --rm --no-deps -e BACKUP_RUN_ONCE=1 backup
#
# Settings (all optional except the connection, which Compose provides):
#   PGHOST PGUSER PGPASSWORD PGDATABASE   how to reach Postgres
#   BACKUP_INTERVAL_HOURS   hours between dumps               (24)
#   BACKUP_KEEP_DAYS        local retention in days           (14)
#   BACKUP_RETRY_SECONDS    wait after a failure              (900)
#   OFFSITE_REMOTE          rclone destination, e.g. offsite:pulsegrid (off)
#   OFFSITE_KEEP_DAYS       remote retention in days          (60)
#   RCLONE_CONFIG           rclone config file (/rclone/rclone.conf)
#   BACKUP_DIR (/backups)  METRICS_DIR (/tmp/metrics)  METRICS_PORT (9187)

set -eu

BACKUP_DIR="${BACKUP_DIR:-/backups}"
METRICS_DIR="${METRICS_DIR:-/tmp/metrics}"
METRICS_PORT="${METRICS_PORT:-9187}"
INTERVAL_HOURS="${BACKUP_INTERVAL_HOURS:-24}"
KEEP_DAYS="${BACKUP_KEEP_DAYS:-14}"
RETRY_SECONDS="${BACKUP_RETRY_SECONDS:-900}"
OFFSITE="${OFFSITE_REMOTE:-}"
OFFSITE_KEEP_DAYS="${OFFSITE_KEEP_DAYS:-60}"
RUN_ONCE="${BACKUP_RUN_ONCE:-0}"
KEEP_MIN=3
export RCLONE_CONFIG="${RCLONE_CONFIG:-/rclone/rclone.conf}"

log() { echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') backup: $*"; }
die() { log "ERROR: $*"; exit 1; }
is_number() { case "$1" in '' | *[!0-9]*) return 1 ;; *) return 0 ;; esac; }

is_number "$INTERVAL_HOURS" && [ "$INTERVAL_HOURS" -ge 1 ] ||
  die "BACKUP_INTERVAL_HOURS must be a whole number, 1 or more (got '$INTERVAL_HOURS')"
is_number "$KEEP_DAYS" || die "BACKUP_KEEP_DAYS must be a whole number (got '$KEEP_DAYS')"
is_number "$OFFSITE_KEEP_DAYS" || die "OFFSITE_KEEP_DAYS must be a whole number (got '$OFFSITE_KEEP_DAYS')"
is_number "$RETRY_SECONDS" && [ "$RETRY_SECONDS" -ge 1 ] ||
  die "BACKUP_RETRY_SECONDS must be a whole number, 1 or more (got '$RETRY_SECONDS')"

INTERVAL_SECONDS=$((INTERVAL_HOURS * 3600))
LAST_OK=1
LAST_SUCCESS_TS=0
LAST_SIZE=0
OFFSITE_TS=0

newest_dump() { ls -1t "$BACKUP_DIR"/pulsegrid-*.dump 2>/dev/null | head -n 1 || true; }
count_dumps() { ls -1 "$BACKUP_DIR"/pulsegrid-*.dump 2>/dev/null | wc -l | tr -d ' '; }
file_mtime() { stat -c %Y "$1"; }
file_size() { stat -c %s "$1"; }

write_metrics() {
  mkdir -p "$METRICS_DIR"
  offsite_enabled=0
  [ -n "$OFFSITE" ] && offsite_enabled=1
  {
    echo "# HELP pulsegrid_backup_interval_seconds Configured time between dumps."
    echo "# TYPE pulsegrid_backup_interval_seconds gauge"
    echo "pulsegrid_backup_interval_seconds $INTERVAL_SECONDS"
    echo "# HELP pulsegrid_backup_last_success_timestamp_seconds Unix time of the newest verified dump."
    echo "# TYPE pulsegrid_backup_last_success_timestamp_seconds gauge"
    echo "pulsegrid_backup_last_success_timestamp_seconds $LAST_SUCCESS_TS"
    echo "# HELP pulsegrid_backup_last_run_success 1 if the latest dump attempt succeeded, 0 if it failed."
    echo "# TYPE pulsegrid_backup_last_run_success gauge"
    echo "pulsegrid_backup_last_run_success $LAST_OK"
    echo "# HELP pulsegrid_backup_last_size_bytes Size of the newest dump."
    echo "# TYPE pulsegrid_backup_last_size_bytes gauge"
    echo "pulsegrid_backup_last_size_bytes $LAST_SIZE"
    echo "# HELP pulsegrid_backup_files Dumps currently kept on this server."
    echo "# TYPE pulsegrid_backup_files gauge"
    echo "pulsegrid_backup_files $(count_dumps)"
    echo "# HELP pulsegrid_backup_offsite_enabled 1 if copies are sent off this server."
    echo "# TYPE pulsegrid_backup_offsite_enabled gauge"
    echo "pulsegrid_backup_offsite_enabled $offsite_enabled"
    echo "# HELP pulsegrid_backup_offsite_last_success_timestamp_seconds Unix time of the last successful off-server copy."
    echo "# TYPE pulsegrid_backup_offsite_last_success_timestamp_seconds gauge"
    echo "pulsegrid_backup_offsite_last_success_timestamp_seconds $OFFSITE_TS"
  } >"$METRICS_DIR/.metrics.tmp"
  mv "$METRICS_DIR/.metrics.tmp" "$METRICS_DIR/metrics.txt"
}

dump_due() {
  n=$(newest_dump)
  [ -n "$n" ] || return 0
  age=$(($(date +%s) - $(file_mtime "$n")))
  [ "$age" -ge "$INTERVAL_SECONDS" ]
}

seconds_until_due() {
  n=$(newest_dump)
  [ -n "$n" ] || { echo 60; return; }
  left=$((INTERVAL_SECONDS - ($(date +%s) - $(file_mtime "$n"))))
  [ "$left" -ge 60 ] || left=60
  echo "$left"
}

backup_once() {
  ts=$(date -u +%Y%m%d-%H%M%S)
  final="$BACKUP_DIR/pulsegrid-$ts.dump"
  partial="$BACKUP_DIR/.pulsegrid-$ts.dump.partial"
  log "dumping database '${PGDATABASE:-}' on '${PGHOST:-}'"
  if ! pg_dump --format=custom --no-owner --file="$partial"; then
    rm -f "$partial"
    log "dump failed"
    return 1
  fi
  # A dump is only kept if it can be read back; a damaged file is worse than none.
  if [ ! -s "$partial" ] || ! pg_restore --list "$partial" >/dev/null 2>&1; then
    rm -f "$partial"
    log "dump was empty or unreadable, discarded"
    return 1
  fi
  mv "$partial" "$final"
  LAST_SUCCESS_TS=$(file_mtime "$final")
  LAST_SIZE=$(file_size "$final")
  log "wrote $final ($LAST_SIZE bytes)"
  prune_local
  return 0
}

# Delete dumps older than KEEP_DAYS, but always keep the newest KEEP_MIN: if
# backups stopped for weeks, age alone must not delete the last good ones.
prune_local() {
  i=0
  for f in $(ls -1t "$BACKUP_DIR"/pulsegrid-*.dump 2>/dev/null || true); do
    i=$((i + 1))
    [ "$i" -gt "$KEEP_MIN" ] || continue
    age_days=$((($(date +%s) - $(file_mtime "$f")) / 86400))
    if [ "$age_days" -gt "$KEEP_DAYS" ]; then
      log "pruning $f (${age_days} days old)"
      rm -f "$f"
    fi
  done
}

ensure_rclone() {
  command -v rclone >/dev/null 2>&1 && return 0
  log "offsite: installing rclone"
  apk add --no-cache rclone >/dev/null 2>&1
}

# Copy (never sync: a wiped local directory must not wipe the remote copies)
# and delete remote dumps only by age. Half-written dumps carry a different
# name (.partial) and a finished one appears atomically, so no age filter is
# needed to avoid copying an incomplete file.
offsite_once() {
  [ -n "$OFFSITE" ] || return 0
  ensure_rclone || { log "offsite: rclone is not available"; return 1; }
  log "offsite: copying to $OFFSITE"
  rclone copy "$BACKUP_DIR" "$OFFSITE" --include 'pulsegrid-*.dump' ||
    { log "offsite: copy failed"; return 1; }
  rclone delete "$OFFSITE" --include 'pulsegrid-*.dump' --min-age "${OFFSITE_KEEP_DAYS}d" ||
    { log "offsite: cleanup failed"; return 1; }
  OFFSITE_TS=$(date +%s)
  return 0
}

serve_metrics() {
  busybox httpd -p "$METRICS_PORT" -h "$METRICS_DIR" 2>/dev/null ||
    httpd -p "$METRICS_PORT" -h "$METRICS_DIR" 2>/dev/null
}

# Alpine's busybox has no httpd applet; it comes in the busybox-extras package.
start_metrics_server() {
  serve_metrics && return 0
  log "metrics: no httpd applet here, installing busybox-extras"
  apk add --no-cache busybox-extras >/dev/null 2>&1 && serve_metrics
}

mkdir -p "$BACKUP_DIR"
newest=$(newest_dump)
if [ -n "$newest" ]; then
  LAST_SUCCESS_TS=$(file_mtime "$newest")
  LAST_SIZE=$(file_size "$newest")
fi
write_metrics

if [ "$RUN_ONCE" = "1" ]; then
  status=0
  backup_once || { LAST_OK=0; status=1; }
  offsite_once || status=1
  write_metrics
  exit "$status"
fi

# Backups must never depend on their own monitoring: if the metrics server
# cannot start, say so and carry on (Prometheus then reports the backup
# service as down, which is the right signal).
if ! start_metrics_server; then
  log "WARNING: the metrics server could not start; backups continue without metrics"
fi
log "metrics on :$METRICS_PORT/metrics.txt; interval ${INTERVAL_HOURS}h, keep ${KEEP_DAYS} days, offsite: ${OFFSITE:-off}"

while true; do
  ok=1
  if dump_due; then
    if backup_once; then LAST_OK=1; else LAST_OK=0; ok=0; fi
  fi
  offsite_once || ok=0
  write_metrics
  if [ "$ok" = "1" ]; then pause=$(seconds_until_due); else pause=$RETRY_SECONDS; fi
  log "next check in ${pause}s"
  sleep "$pause"
done
