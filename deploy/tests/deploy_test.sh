#!/usr/bin/env bash
# Tests for deploy/deploy.sh. They run it against a fake `docker`, `curl` and
# `git` (no Docker needed), so every path is exercised: success, first deploy,
# failed backup, unpullable tag, unhealthy version with automatic rollback, a
# rollback that fails too, --rollback, dry run, bad input, secrets never printed.
#   bash deploy/tests/deploy_test.sh
# shellcheck disable=SC2016,SC2034,SC2086,SC2164
# (the checks are single-quoted on purpose and eval'd later; $T is a mktemp path)
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
T=$(mktemp -d)
trap 'rm -rf "$T"' EXIT
mkdir -p "$T/bin" "$T/repo/deploy/secrets"
cp "$HERE/../deploy.sh" "$T/repo/deploy/deploy.sh"
touch "$T/repo/docker-compose.yml" "$T/repo/docker-compose.prod.yml"
# deploy.sh wants non-empty secret files; the content is distinctive so that T17
# can check it is never printed
for s in telegram_bot_token jwt_secret operator_api_keys; do echo "file-content-of-$s" > "$T/repo/deploy/secrets/$s"; done

cat > "$T/bin/docker" <<'SHIM'
#!/usr/bin/env bash
# Fake docker for deploy.sh tests: logs every call; behaviour comes from FAKE_* variables.
echo "docker $*" >> "$FAKE_LOG"
[ "${1:-}" = compose ] || exit 0
shift
while [ $# -gt 0 ]; do case "$1" in --env-file | -f) shift 2 ;; *) break ;; esac; done
sub="$1"; shift
ver() { grep -E '^PULSEGRID_VERSION=' "$FAKE_ENV" | tail -1 | cut -d= -f2-; }
case "$sub" in
  config) exit 0 ;;
  ps) [ -n "${FAKE_RUNNING:-}" ] && echo "$FAKE_RUNNING" | tr ',' '\n'; exit 0 ;;
  run) echo "BACKUP-RUN args: $*" >> "$FAKE_LOG"; [ "${FAKE_BACKUP_FAIL:-0}" = 1 ] && exit 1; exit 0 ;;
  pull) echo "PULL with PULSEGRID_VERSION=${PULSEGRID_VERSION:-unset} file-version=$(ver)" >> "$FAKE_LOG"
        [ "${PULSEGRID_VERSION:-}" = "${FAKE_BAD_TAG:-none}" ] && exit 1; exit 0 ;;
  up) echo "UP with env version $(ver)" >> "$FAKE_LOG"; exit 0 ;;
  exec) v=$(ver)
        for bad in ${FAKE_UNHEALTHY:-}; do [ "$v" = "$bad" ] && exit 1; done; exit 0 ;;
  logs) echo "(fake service logs)"; exit 0 ;;
esac
exit 0
SHIM
cat > "$T/bin/curl" <<'SHIM'
#!/usr/bin/env bash
url="${@: -1}"
case "$url" in
  */health)
    # the first FAKE_CURL_HEALTH_FAILS calls fail like a Caddy that has no certificate yet
    n=$(cat "$FAKE_CURL_COUNT" 2>/dev/null || echo 0); echo $((n + 1)) > "$FAKE_CURL_COUNT"
    if [ "$n" -lt "${FAKE_CURL_HEALTH_FAILS:-0}" ]; then echo -n 000; else echo -n "${FAKE_CURL_HEALTH:-200}"; fi ;;
  */metrics) echo -n "${FAKE_CURL_METRICS:-404}" ;;
esac
SHIM
printf '#!/usr/bin/env bash\necho "git $*" >> "$FAKE_LOG"; exit 0\n' > "$T/bin/git"
chmod +x "$T/bin/docker" "$T/bin/curl" "$T/bin/git"

export PATH=$T/bin:$PATH FAKE_LOG=$T/calls.log FAKE_ENV=$T/repo/.env.docker
export DEPLOY_HEALTH_TIMEOUT=2 DEPLOY_HEALTH_INTERVAL=1 DEPLOY_PUBLIC_TIMEOUT=3 DEPLOY_PUBLIC_INTERVAL=1
export FAKE_CURL_COUNT=$T/curl_count
reset() {  # fresh settings file and state for each test
  cd $T/repo
  printf '# comment\nAPI_DOMAIN=api.example.com\nWS_DOMAIN=ws.example.com\nACME_EMAIL=ops@example.com\nPOSTGRES_PASSWORD=s3cret-pw\nJWT_SECRET=very-secret-jwt\n# PULSEGRID_VERSION=latest\nPULSEGRID_VERSION=sha-1111111\nOTHER=keep-me\n' > .env.docker
  chmod 600 .env.docker; rm -f .deploy-state $T/calls.log $T/curl_count; touch $T/calls.log
  unset FAKE_RUNNING FAKE_BACKUP_FAIL FAKE_BAD_TAG FAKE_UNHEALTHY FAKE_CURL_HEALTH FAKE_CURL_METRICS FAKE_CURL_HEALTH_FAILS
}

PASS=0; FAILN=0; ALLOUT=$T/all-output.txt; : > $ALLOUT
ok()   { PASS=$((PASS+1)); echo "PASS  $*"; }
bad()  { FAILN=$((FAILN+1)); echo "FAIL  $*"; }
check() { if eval "$1"; then ok "$2"; else bad "$2   [$1]"; fi; }
D() { OUT=$(bash $T/repo/deploy/deploy.sh "$@" 2>&1); RC=$?; echo "$OUT" >> $ALLOUT; }
ver() { grep -E '^PULSEGRID_VERSION=' $T/repo/.env.docker | tail -1 | cut -d= -f2-; }
calls() { cat $T/calls.log; }
count() { calls | grep -c "$1" || true; }

echo "--- T1 success: backup, pull before switching, switch, health, state"
reset; export FAKE_RUNNING=postgres,backup
D --version sha-2222222
check '[ $RC -eq 0 ]' "exit 0"
check '[ "$(ver)" = sha-2222222 ]' "env file now says sha-2222222"
check 'grep -q "BACKUP-RUN args: .*BACKUP_RUN_ONCE=1" $T/calls.log' "a one-off backup ran first"
check 'calls | grep "^PULL" | grep -q "PULSEGRID_VERSION=sha-2222222 file-version=sha-1111111"' "pulled the NEW tag while the file still said the OLD one (nothing changed before the pull)"
check '[ "$(calls | grep -n "^BACKUP-RUN" | head -1 | cut -d: -f1)" -lt "$(calls | grep -n "^PULL" | head -1 | cut -d: -f1)" ]' "backup happened before the pull"
check 'calls | grep -q "^UP with env version sha-2222222"' "started the new version"
check 'grep -q "^previous=sha-1111111$" $T/repo/.deploy-state && grep -q "^current=sha-2222222$" $T/repo/.deploy-state' "state remembers the previous version"
check 'grep -q "^OTHER=keep-me$" $T/repo/.env.docker && grep -q "^# PULSEGRID_VERSION=latest$" $T/repo/.env.docker && grep -q "^# comment$" $T/repo/.env.docker' "other lines and comments untouched"
check '[ "$(stat -c %a $T/repo/.env.docker)" = 600 ]' "file permissions still 600"

echo "--- T2 first deploy: nothing running, so no backup"
reset; D --version sha-2222222
check '[ $RC -eq 0 ] && [ "$(count "^BACKUP-RUN")" = 0 ]' "exit 0 and no backup attempted"
check 'echo "$OUT" | grep -q "first deploy"' "says why"

echo "--- T3 a failing backup stops everything before any change"
reset; export FAKE_RUNNING=postgres FAKE_BACKUP_FAIL=1
D --version sha-2222222
check '[ $RC -eq 1 ] && [ "$(count "^PULL")" = 0 ] && [ "$(count "^UP")" = 0 ]' "exit 1, no pull, no restart"
check '[ "$(ver)" = sha-1111111 ] && [ ! -f $T/repo/.deploy-state ]' "env and state untouched"
check 'echo "$OUT" | grep -q "backup failed; nothing was changed"' "message says nothing changed"
D --version sha-2222222 --no-backup
check '[ $RC -eq 0 ] && [ "$(ver)" = sha-2222222 ]' "--no-backup lets the deploy through"
unset FAKE_BACKUP_FAIL

echo "--- T4 a tag that cannot be pulled changes nothing and costs no downtime"
reset; export FAKE_RUNNING=postgres FAKE_BAD_TAG=sha-3333333
D --version sha-3333333
check '[ $RC -eq 1 ] && [ "$(count "^UP")" = 0 ]' "exit 1 and the running stack was never restarted"
check '[ "$(ver)" = sha-1111111 ]' "env file still on the old version"

echo "--- T5 unhealthy new version: automatic rollback"
reset; export FAKE_RUNNING=postgres FAKE_UNHEALTHY="sha-4444444"
D --version sha-4444444
check '[ $RC -eq 1 ]' "exit 1"
check '[ "$(ver)" = sha-1111111 ]' "env file restored to the previous version"
check '[ "$(count "^UP")" = 2 ]' "restarted twice (new, then back)"
check 'calls | grep "^UP" | tail -1 | grep -q sha-1111111' "the last start was the old version"
check 'echo "$OUT" | grep -q "rolled back" && echo "$OUT" | grep -q "was NOT deployed"' "message says it rolled back"
check '[ ! -f $T/repo/.deploy-state ]' "state was not updated for a failed deploy"

echo "--- T6 the rollback is unhealthy too"
reset; export FAKE_RUNNING=postgres FAKE_UNHEALTHY="sha-4444444 sha-1111111"
D --version sha-4444444
check '[ $RC -eq 3 ]' "exit 3"
check 'echo "$OUT" | grep -q "rollback did not become healthy"' "tells you to intervene"

echo "--- T7 --rollback after a good deploy"
reset; export FAKE_RUNNING=postgres
D --version sha-2222222; D --rollback
check '[ $RC -eq 0 ] && [ "$(ver)" = sha-1111111 ]' "back on sha-1111111"
check 'grep -q "^previous=sha-2222222$" $T/repo/.deploy-state && grep -q "^current=sha-1111111$" $T/repo/.deploy-state' "state swapped"

echo "--- T8 --rollback with nothing recorded"
reset; D --rollback
check '[ $RC -eq 1 ] && echo "$OUT" | grep -q "no previous version"' "refused with a clear message"

echo "--- T9 invalid version is rejected before anything runs"
reset; D --version 'sha-zz; touch $T/PWNED'
check '[ $RC -eq 1 ] && [ ! -e $T/PWNED ] && [ ! -s $T/calls.log ]' "rejected, nothing executed, no docker call"
D --version latest
check '[ $RC -eq 0 ] && [ "$(ver)" = latest ]' "'latest' is accepted"
D --version
check '[ $RC -eq 1 ] && echo "$OUT" | grep -q "needs a value"' "--version without a value is an error"
D --bogus
check '[ $RC -eq 1 ] && echo "$OUT" | grep -q "unknown option"' "unknown option is an error"

echo "--- T10 a missing setting is named, values are never printed"
reset; sed -i '/^POSTGRES_PASSWORD=/d' $T/repo/.env.docker
D
check '[ $RC -eq 1 ] && echo "$OUT" | grep -q "POSTGRES_PASSWORD is not set"' "names the missing setting"
check '[ ! -s $T/calls.log ]' "stopped before calling docker"

echo "--- T11 dry run changes nothing and calls nothing"
reset; export FAKE_RUNNING=postgres
D --version sha-2222222 --dry-run
check '[ $RC -eq 0 ] && [ "$(ver)" = sha-1111111 ] && [ ! -f $T/repo/.deploy-state ]' "exit 0, env and state untouched"
check '[ ! -s $T/calls.log ]' "no docker call was made"
check 'echo "$OUT" | grep -q "\[dry-run\]"' "it printed the plan"

echo "--- T12 a public check failure is reported but the deploy stands"
reset; export FAKE_RUNNING=postgres FAKE_CURL_METRICS=200
D --version sha-2222222
check '[ $RC -eq 2 ] && [ "$(ver)" = sha-2222222 ]' "exit 2, new version kept"
check 'echo "$OUT" | grep -q "must be hidden (404)"' "says /metrics is exposed"
unset FAKE_CURL_METRICS; D --version sha-2222222
check 'echo "$OUT" | grep -q "public check ok: /health 200" && echo "$OUT" | grep -q "/metrics hidden"' "both checks pass when healthy"

echo "--- T13 redeploying the same version keeps the recorded previous one"
reset; printf 'previous=sha-0000000\ncurrent=sha-1111111\n' > $T/repo/.deploy-state; export FAKE_RUNNING=postgres
D
check '[ $RC -eq 0 ] && grep -q "^previous=sha-0000000$" $T/repo/.deploy-state' "previous stays sha-0000000"

echo "--- T14 unhealthy redeploy of the same version: nothing to roll back to"
reset; export FAKE_RUNNING=postgres FAKE_UNHEALTHY=sha-1111111
D
check '[ $RC -eq 1 ] && [ "$(count "^UP")" = 1 ] && echo "$OUT" | grep -q "no earlier version"' "exit 1, single start, explained"

echo "--- T15 env file with no trailing newline and no version line"
reset; printf 'API_DOMAIN=a.example.com\nWS_DOMAIN=w.example.com\nACME_EMAIL=o@example.com\nPOSTGRES_PASSWORD=pw\nOTHER=keep-me' > $T/repo/.env.docker
D --version latest
check '[ $RC -eq 0 ] && tail -2 $T/repo/.env.docker | head -1 | grep -q "^OTHER=keep-me$" && [ "$(ver)" = latest ]' "appended on its own line, other lines intact"
check 'grep -q "^previous=latest$" $T/repo/.deploy-state' "an unset version counts as latest"

echo "--- T16 --git-pull and a missing secrets file"
reset; D --git-pull --version sha-2222222
check 'calls | grep -q "^git pull --ff-only"' "ran git pull --ff-only"
reset; mv $T/repo/deploy/secrets/telegram_bot_token $T/tok
D --version sha-2222222
check '[ $RC -eq 1 ] && echo "$OUT" | grep -q "telegram_bot_token is missing"' "refuses without the Telegram token file"
mv $T/tok $T/repo/deploy/secrets/telegram_bot_token

echo "--- T17 no secret value ever appears in any output"
check '! grep -Eq "s3cret-pw|very-secret-jwt|file-content-of-" $T/all-output.txt' "passwords and secrets were never printed"

echo "--- T18 required values may come from the environment (Compose allows it)"
reset; sed -i '/^POSTGRES_PASSWORD=/d; /^API_DOMAIN=/d' $T/repo/.env.docker; export FAKE_RUNNING=postgres
D --version sha-2222222
check '[ $RC -eq 1 ] && echo "$OUT" | grep -q "API_DOMAIN is not set"' "missing everywhere: refused"
API_DOMAIN=api.example.com POSTGRES_PASSWORD=from-env D --version sha-2222222
check '[ $RC -eq 0 ] && [ "$(ver)" = sha-2222222 ]' "values from the environment are accepted"
check 'echo "$OUT" | grep -q "public check ok: /health 200"' "the public check used the domain from the environment"

echo "--- T19 HTTPS not ready yet (Caddy still getting certificates): retried, then fine"
reset; export FAKE_RUNNING=postgres FAKE_CURL_HEALTH_FAILS=2
D --version sha-2222222
check '[ $RC -eq 0 ]' "exit 0 instead of a false alarm"
check 'echo "$OUT" | grep -q "waiting for HTTPS" && echo "$OUT" | grep -q "public check ok: /health 200"' "said it was waiting, then confirmed /health"
check '[ "$(cat $FAKE_CURL_COUNT)" -ge 3 ]' "/health was asked at least three times"

echo "--- T20 HTTPS never comes up: warns after the wait, deploy stands"
reset; export FAKE_RUNNING=postgres FAKE_CURL_HEALTH=000
D --version sha-2222222
check '[ $RC -eq 2 ] && [ "$(ver)" = sha-2222222 ]' "exit 2, new version kept"
check 'echo "$OUT" | grep -q "answered .000. for [1-9][0-9]*s"' "the warning says how long it waited"
check 'echo "$OUT" | grep -q "DNS, ports 80/443"' "and where to look"

echo "--- T21 every secret file is checked: missing, empty, or a directory made by Docker"
for s in telegram_bot_token jwt_secret operator_api_keys; do
  f=$T/repo/deploy/secrets/$s
  reset; export FAKE_RUNNING=postgres
  mv $f $T/saved-secret
  D --version sha-2222222
  check '[ $RC -eq 1 ] && echo "$OUT" | grep -q "secrets/$s is missing or empty"' "$s missing: refused"
  check '[ ! -s $T/calls.log ] && [ "$(ver)" = sha-1111111 ]' "$s missing: stopped before any docker call, env untouched"
  : > $f
  D --version sha-2222222
  check '[ $RC -eq 1 ] && echo "$OUT" | grep -q "secrets/$s is missing or empty"' "$s empty: refused"
  rm -f $f; mkdir $f
  D --version sha-2222222
  check '[ $RC -eq 1 ] && echo "$OUT" | grep -q "secrets/$s is a directory" && echo "$OUT" | grep -q "rm -r"' "$s a directory: refused, with the fix"
  check '[ ! -s $T/calls.log ]' "$s a directory: no docker call"
  rmdir $f; mv $T/saved-secret $f
done
reset; export FAKE_RUNNING=postgres
D --version sha-2222222
check '[ $RC -eq 0 ]' "with all three secret files back, the deploy goes through"

echo; echo "RESULT: $PASS passed, $FAILN failed"
[ "$FAILN" -eq 0 ]
