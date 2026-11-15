#!/bin/sh
# shellcheck disable=SC2012,SC2015 # dump names are generated here; "A && B || die" is intended
# Restore a PulseGrid dump into a database and print the row count of every
# table, so a restore can be checked against the live data.
#
#   restore.sh /backups/pulsegrid-<time>.dump [target-database]
#
# The target defaults to the scratch database pulsegrid_restore_test, which is
# dropped and recreated. Restoring into the live database is refused unless
# RESTORE_CONFIRM=yes is set, and it replaces everything in it: stop the
# coordinator and the worker first (see deploy/PRODUCTION.md).

set -eu

DUMP="${1:-}"
TARGET="${2:-pulsegrid_restore_test}"
LIVE="${PGDATABASE:-pulsegrid}"

[ -n "$DUMP" ] && [ -f "$DUMP" ] || {
  echo "usage: restore.sh <dump-file> [target-database]" >&2
  exit 2
}
if [ "$TARGET" = "$LIVE" ] && [ "${RESTORE_CONFIRM:-}" != "yes" ]; then
  echo "refusing to overwrite the live database '$LIVE' without RESTORE_CONFIRM=yes" >&2
  exit 3
fi

pg_restore --list "$DUMP" >/dev/null || { echo "'$DUMP' is not a readable dump" >&2; exit 4; }

echo "recreating database '$TARGET'"
psql -d postgres -v ON_ERROR_STOP=1 \
  -c "DROP DATABASE IF EXISTS \"$TARGET\" WITH (FORCE)" \
  -c "CREATE DATABASE \"$TARGET\""

echo "restoring $DUMP"
pg_restore --no-owner --no-privileges --exit-on-error --dbname="$TARGET" "$DUMP"

echo "rows per table in '$TARGET':"
for t in $(psql -d "$TARGET" -Atc "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY 1"); do
  n=$(psql -d "$TARGET" -Atc "SELECT count(*) FROM \"$t\"")
  printf '  %-34s %s\n' "$t" "$n"
done
echo "restore finished"
