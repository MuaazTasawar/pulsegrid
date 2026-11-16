#!/usr/bin/env bash
# Update the production stack safely. Run it on the server, from anywhere.
#
#   deploy/deploy.sh                       redeploy the version in .env.docker
#   deploy/deploy.sh --version sha-24a8828 deploy exactly that published build
#   deploy/deploy.sh --rollback            go back to the version before the last deploy
#   deploy/deploy.sh --git-pull            run `git pull --ff-only` first
#   deploy/deploy.sh --no-backup           skip the pre-deploy database dump
#   deploy/deploy.sh --dry-run             print what would happen, change nothing
#
# What it does, in this order, stopping at the first problem:
#   1. checks the settings (names only, never values) and validates the compose files;
#   2. takes a database backup (skipped on a first deploy, when nothing runs yet);
#   3. pulls the new image BEFORE touching anything running, so a typo in the
#      tag or a registry problem changes nothing and causes no downtime;
#   4. records the version in .env.docker and restarts with `up -d --no-build`;
#   5. waits until the coordinator and the worker answer /ready;
#   6. checks the public side (/health answers 200, /metrics is hidden); it
#      retries for up to DEPLOY_PUBLIC_TIMEOUT seconds (90) because Caddy
#      needs a moment to obtain certificates after a start;
#   7. if step 5 fails, puts the previous version back and checks it again.
#
# Exit codes: 0 deployed and checked; 1 failed (nothing changed, or rolled back
# successfully); 2 deployed but a public check failed; 3 the rollback failed too.
#
# A database migration that ran is not undone by rolling back the image: read
# deploy/PRODUCTION.md before rolling back across one.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ENV_FILE="${DEPLOY_ENV_FILE:-.env.docker}"
STATE_FILE="${DEPLOY_STATE_FILE:-.deploy-state}"
HEALTH_TIMEOUT="${DEPLOY_HEALTH_TIMEOUT:-120}"
HEALTH_INTERVAL="${DEPLOY_HEALTH_INTERVAL:-3}"
[ "$HEALTH_INTERVAL" -ge 1 ] 2>/dev/null || HEALTH_INTERVAL=1
PUBLIC_TIMEOUT="${DEPLOY_PUBLIC_TIMEOUT:-90}"
PUBLIC_INTERVAL="${DEPLOY_PUBLIC_INTERVAL:-3}"
[ "$PUBLIC_INTERVAL" -ge 1 ] 2>/dev/null || PUBLIC_INTERVAL=1
COMPOSE=(docker compose --env-file "$ENV_FILE" -f docker-compose.yml -f docker-compose.prod.yml)

TARGET="" DO_BACKUP=1 DRY_RUN=0 GIT_PULL=0 ROLLBACK=0

log() { printf '%s deploy: %s\n' "$(date -u +%H:%M:%S)" "$*"; }
die() { log "ERROR: $*"; exit 1; }
usage() { sed -n '2,/^$/p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; }
run() { if [ "$DRY_RUN" = 1 ]; then log "[dry-run] $*"; else "$@"; fi; }

while [ $# -gt 0 ]; do
  case "$1" in
    --version) [ $# -ge 2 ] || die "--version needs a value"; TARGET="$2"; shift 2 ;;
    --no-backup) DO_BACKUP=0; shift ;;
    --git-pull) GIT_PULL=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    --rollback) ROLLBACK=1; shift ;;
    -h | --help) usage; exit 0 ;;
    *) die "unknown option '$1' (try --help)" ;;
  esac
done

# Value of NAME in the env file (last uncommented assignment); empty if unset.
get_var() {
  local line
  line=$(grep -E "^[[:space:]]*$1=" "$ENV_FILE" | tail -n 1 || true)
  line=${line#*=}
  line=${line%$'\r'}
  line=${line#\"}; line=${line%\"}; line=${line#\'}; line=${line%\'}
  printf '%s' "$line"
}

# Set NAME=VALUE in the env file, keeping every other line, the file's
# permissions and its owner (it is rewritten in place, not replaced).
set_var() {
  local name=$1 value=$2 tmp
  if [ "$DRY_RUN" = 1 ]; then log "[dry-run] set $name=$value in $ENV_FILE"; return 0; fi
  tmp=$(mktemp)
  if grep -qE "^[[:space:]]*$name=" "$ENV_FILE"; then
    sed -E "s|^[[:space:]]*$name=.*|$name=$value|" "$ENV_FILE" >"$tmp"
  else
    {
      cat "$ENV_FILE"
      if [ -n "$(tail -c 1 "$ENV_FILE")" ]; then printf '\n'; fi
      printf '%s=%s\n' "$name" "$value"
    } >"$tmp"
  fi
  cat "$tmp" >"$ENV_FILE"
  rm -f "$tmp"
}

state_get() {
  [ -f "$STATE_FILE" ] || return 0
  grep -E "^$1=" "$STATE_FILE" | tail -n 1 | cut -d= -f2- || true
}
valid_version() { [[ $1 =~ ^(latest|sha-[0-9a-f]{7,40})$ ]]; }

stack_running() { "${COMPOSE[@]}" ps --status running --services 2>/dev/null | grep -qx postgres; }

# The services answer /ready on the Compose network; Caddy's image has wget.
healthy() {
  "${COMPOSE[@]}" exec -T caddy wget -q -O /dev/null http://api-coordinator:8090/ready &&
    "${COMPOSE[@]}" exec -T caddy wget -q -O /dev/null http://fanout-worker:8081/ready
}
wait_healthy() {
  local waited=0
  while ! healthy >/dev/null 2>&1; do
    [ "$waited" -lt "$HEALTH_TIMEOUT" ] || return 1
    sleep "$HEALTH_INTERVAL"
    waited=$((waited + HEALTH_INTERVAL))
  done
}

# From the outside: /health must answer and /metrics must stay hidden. Right
# after a start (or on a new domain) Caddy is still obtaining its certificates
# and refuses HTTPS connections, so /health is retried for a while first.
public_checks() {
  command -v curl >/dev/null 2>&1 || { log "curl not found, skipping the public checks"; return 0; }
  local domain code waited=0 bad=0 insecure=()
  domain=$(get_var API_DOMAIN)
  [ -n "$domain" ] || domain=${API_DOMAIN:-}
  case "$domain" in localhost | *.localhost) insecure=(-k) ;; esac
  while :; do
    code=$(curl -s ${insecure[@]+"${insecure[@]}"} -o /dev/null -w '%{http_code}' --max-time 10 "https://$domain/health" 2>/dev/null || true)
    [ "$code" = 200 ] && break
    [ "$waited" -lt "$PUBLIC_TIMEOUT" ] || break
    [ "$waited" -gt 0 ] || log "waiting for HTTPS on $domain (Caddy may still be getting its certificates; up to ${PUBLIC_TIMEOUT}s)"
    sleep "$PUBLIC_INTERVAL"
    waited=$((waited + PUBLIC_INTERVAL))
  done
  if [ "$code" = 200 ]; then log "public check ok: /health 200"; else log "WARNING: https://$domain/health answered '${code:-nothing}' for ${waited}s (expected 200); check DNS, ports 80/443 and the Caddy log"; bad=1; fi
  code=$(curl -s ${insecure[@]+"${insecure[@]}"} -o /dev/null -w '%{http_code}' --max-time 10 "https://$domain/metrics" 2>/dev/null || true)
  if [ "$code" = 404 ]; then log "public check ok: /metrics hidden (404)"; else log "WARNING: https://$domain/metrics answered '${code:-nothing}' but must be hidden (404)"; bad=1; fi
  return "$bad"
}

deploy_failed() {
  log "FAILED: the stack did not become healthy on '$TARGET'"
  "${COMPOSE[@]}" logs --tail 20 api-coordinator fanout-worker 2>&1 || true
  if [ "$TARGET" = "$CURRENT" ]; then
    log "there is no earlier version to go back to; read the logs above"
    exit 1
  fi
  log "rolling back to '$CURRENT'"
  set_var PULSEGRID_VERSION "$CURRENT"
  if "${COMPOSE[@]}" up -d --no-build --remove-orphans && wait_healthy; then
    log "rolled back: '$CURRENT' is running again. '$TARGET' was NOT deployed."
    exit 1
  fi
  log "ERROR: the rollback did not become healthy either. Look at: ${COMPOSE[*]} ps  and  logs"
  exit 3
}

# --- 1. checks ---------------------------------------------------------------
[ -f "$ENV_FILE" ] || die "$ENV_FILE not found (create it from .env.docker.example)"
# Compose reads these from the environment as well as from the file, so both count.
for name in API_DOMAIN WS_DOMAIN ACME_EMAIL POSTGRES_PASSWORD; do
  [ -n "$(get_var "$name")" ] || [ -n "${!name:-}" ] || die "$name is not set in $ENV_FILE or the environment"
done
[ -f deploy/secrets/telegram_bot_token ] || die "deploy/secrets/telegram_bot_token is missing (see deploy/PRODUCTION.md, Monitoring)"

CURRENT=$(get_var PULSEGRID_VERSION)
[ -n "$CURRENT" ] || CURRENT=latest
if [ "$ROLLBACK" = 1 ]; then
  TARGET=$(state_get previous)
  [ -n "$TARGET" ] || die "no previous version is recorded in $STATE_FILE; use --version sha-<commit>"
else
  [ -n "$TARGET" ] || TARGET=$CURRENT
fi
valid_version "$TARGET" || die "'$TARGET' is not a valid version (use latest or sha-<7 to 40 hex digits>)"
log "version now: $CURRENT; deploying: $TARGET$([ "$DRY_RUN" = 1 ] && echo ' (dry run)')"

if [ "$GIT_PULL" = 1 ]; then run git pull --ff-only || die "git pull failed; nothing was changed"; fi
run "${COMPOSE[@]}" config -q || die "the compose files do not validate; nothing was changed"

# --- 2. backup ---------------------------------------------------------------
if [ "$DO_BACKUP" = 1 ]; then
  if [ "$DRY_RUN" = 1 ] || stack_running; then
    log "taking a database backup first"
    run "${COMPOSE[@]}" run --rm --no-deps -e BACKUP_RUN_ONCE=1 backup || die "the backup failed; nothing was changed (use --no-backup to skip it)"
  else
    log "the database is not running (first deploy?): nothing to back up"
  fi
else
  log "skipping the backup (--no-backup)"
fi

# --- 3. pull before touching anything -----------------------------------------
log "pulling the images"
run env "PULSEGRID_VERSION=$TARGET" "${COMPOSE[@]}" pull ||
  die "pull failed (does the tag '$TARGET' exist? are you logged in to ghcr.io?); nothing was changed"

# --- 4. switch ---------------------------------------------------------------
set_var PULSEGRID_VERSION "$TARGET"
log "starting '$TARGET'"
if [ "$DRY_RUN" = 1 ]; then
  run "${COMPOSE[@]}" up -d --no-build --remove-orphans
  log "[dry-run] would wait for /ready, run the public checks and record the version"
  exit 0
fi
"${COMPOSE[@]}" up -d --no-build --remove-orphans || deploy_failed

# --- 5. wait for health ------------------------------------------------------
log "waiting for the coordinator and the worker (up to ${HEALTH_TIMEOUT}s)"
wait_healthy || deploy_failed
log "healthy"

# --- 6. public checks and record ---------------------------------------------
if [ "$TARGET" != "$CURRENT" ]; then PREVIOUS=$CURRENT; else PREVIOUS=$(state_get previous); [ -n "$PREVIOUS" ] || PREVIOUS=$CURRENT; fi
printf 'previous=%s\ncurrent=%s\n' "$PREVIOUS" "$TARGET" >"$STATE_FILE"
status=0
public_checks || status=2
log "deployed '$TARGET' (the version before it was '$PREVIOUS'; roll back with: deploy/deploy.sh --rollback)"
exit "$status"
