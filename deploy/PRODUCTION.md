# Running PulseGrid in production (single server, Docker Compose)

This sets up one Linux server with automatic HTTPS. It is a single-instance
deployment: one Postgres, one Redis, one NATS, one coordinator, one worker.
See "What this does not give you" at the end.

Every command below uses the same shorthand. Set it once per shell:

```sh
export COMPOSE="docker compose --env-file .env.docker -f docker-compose.yml -f docker-compose.prod.yml"
```

`--env-file` is needed on every command, including `logs` and `ps`: Compose
reads the required values (domains, password) from it, and refuses to run
without them.

## Where the application image comes from

The server never compiles anything. On every push to `main`, after lint, unit
and integration tests all pass, CI builds one image that contains both
binaries and publishes it to GitHub Container Registry:

- `ghcr.io/muaaztasawar/pulsegrid:sha-<short commit>`: one specific commit
  (what to pin and to roll back to);
- `ghcr.io/muaaztasawar/pulsegrid:latest`: the newest `main`.

The image contains the two binaries only; secrets come from `.env.docker` at
run time.

Packages are private by default. The server needs to log in once with a
token that can read packages (a GitHub token with only the `read:packages`
scope), or you make the package public in its settings on GitHub:

```sh
echo "<token>" | docker login ghcr.io -u <github-user> --password-stdin
```

## What you need

- A Linux server with Docker and the Compose plugin, and this repository on it.
- Two DNS records (A and/or AAAA) pointing at the server's public address:
  one for the API and dashboard (`API_DOMAIN`, e.g. `api.example.com`) and one
  for device WebSockets (`WS_DOMAIN`, e.g. `ws.example.com`).
- The server's firewall allowing only SSH, 80 and 443 in. The overlay
  publishes nothing else; the firewall is the second layer.
- A published image: the CI run on `main` must have finished at least once.
- A Telegram account, to receive alerts (see "Monitoring and alerts").

## First start

1. Create the settings file and edit it:

   ```sh
   cp .env.docker.example .env.docker
   chmod 600 .env.docker
   ```

   In `.env.docker`:

   - `JWT_SECRET`: a strong random value, e.g. `openssl rand -hex 32`.
   - `OPERATOR_API_KEYS`: a strong random value (or several, comma separated).
     This key sends alerts and opens the dashboard.
   - `POSTGRES_PASSWORD`: letters and digits only, e.g. `openssl rand -hex 24`.
   - Uncomment and set `API_DOMAIN`, `WS_DOMAIN` and `ACME_EMAIL`.
   - Optionally `PULSEGRID_VERSION=sha-<short commit>` to run an exact build
     (the default is `latest`). Pinning is the better habit for production.

   `POSTGRES_PASSWORD` is only read when the `pgdata` volume is first created.
   Changing it afterwards does not change the database's password, and the
   services can no longer log in. To rotate it later, run
   `ALTER USER pulsegrid PASSWORD '...'` inside Postgres, then update
   `.env.docker` and restart.

   The overlay runs the services with `PULSEGRID_ENV=production`, which makes
   them refuse weak or placeholder secrets at start-up. That refusal is the
   safety net: if a service exits right away, read its log first.

2. Set up alerting: follow "Monitoring and alerts" below (a Telegram bot,
   one number in `deploy/alertmanager/alertmanager.yml`, and one token file).
   Compose will not run any command until that token file exists.

3. Log in to the registry (above), then pull and start:

   ```sh
   $COMPOSE pull
   $COMPOSE up -d --no-build
   ```

   Caddy obtains the certificates for both names when it starts; follow it
   with `$COMPOSE logs -f caddy`. If DNS is wrong or 80/443 are not
   reachable, issuance fails and the log says so.

## Check that it is closed and working

From another machine (replace the names):

```sh
curl -i https://api.example.com/health         # 200
curl -i http://api.example.com/health          # a redirect to https
curl -i https://api.example.com/metrics        # 404 (hidden at the edge)
curl -i https://api.example.com/ready          # 404
curl -i https://ws.example.com/admin/presence/x # 404
curl -i https://ws.example.com/metrics         # 404
nc -vz api.example.com 8090                    # must fail: not published
nc -vz api.example.com 5435                    # must fail
nc -vz api.example.com 6379                    # must fail
nc -vz api.example.com 4222                    # must fail
nc -vz api.example.com 9090                    # must fail (Prometheus: server loopback only)
nc -vz api.example.com 9093                    # must fail (Alertmanager: server loopback only)
```

Open `https://api.example.com/dashboard`, paste an operator key and press
Connect.

## End-to-end proof (devices, alerts, WebSockets over TLS)

From any machine with the repository and Rust installed, using a real
domain (the load generator trusts public certificates only). Keep it small:
production rate limits are on, so a big burst is throttled by design.

```sh
cargo run --release -p loadgen -- --coordinator https://api.example.com \
    --operator-key "<one of OPERATOR_API_KEYS>" \
    --devices 8 --alerts 3 --interval-ms 1000
```

You want every device connected and every delivery counted. This exercises
registration, tickets, the `wss://ws.example.com` connection (the address the
coordinator hands out comes from `WORKER_PUBLIC_URL`, which the overlay sets
from `WS_DOMAIN`), an alert, and the delivery report, all through Caddy.

## Monitoring and alerts

Prometheus scrapes the coordinator and the worker every 15 seconds and keeps
30 days of history. Alertmanager groups the alerts and sends them to
Telegram. Both web pages are reachable only from the server itself; from your
own machine use an SSH tunnel and open `http://localhost:9090` (Prometheus:
Alerts and Targets pages) and `http://localhost:9093` (Alertmanager):

```sh
ssh -L 9090:localhost:9090 -L 9093:localhost:9093 <user>@<server>
```

**Set-up (once):**

1. In Telegram, talk to @BotFather, create a bot and keep its token.
2. Send any message to your new bot, then open
   `https://api.telegram.org/bot<TOKEN>/getUpdates` in a browser and read
   the number after `"chat":{"id":`.
3. Put that number in `chat_id` in `deploy/alertmanager/alertmanager.yml`
   (it is not a secret; commit it).
4. Put the token, and nothing else, in `deploy/secrets/telegram_bot_token`
   (`chmod 600`). The directory is git-ignored; never commit it.

**Prove the whole path works** (rules, Alertmanager, Telegram) by raising a
test alert; the message arrives after about 30 seconds:

```sh
$COMPOSE exec alertmanager amtool alert add TestAlert severity=warning \
    --annotation=summary="Test alert" --alertmanager.url=http://localhost:9093
```

**What alerts, and what to do:**

| Alert | Severity | Meaning |
|---|---|---|
| CoordinatorDown, WorkerDown | critical | A service cannot be scraped for 2 minutes. Check `$COMPOSE ps` and its logs. |
| ShardLeaseLost | critical | A worker no longer owns every shard it should (Redis trouble, or another worker took one). |
| NoShardSubscriptions | critical | A worker listens to no shard, so it receives no alerts. Check NATS. |
| AlertPublishFailures, PartialAlertDispatch | critical | Some alert did not reach some shard: areas missed an alert. Check NATS (JetStream). |
| DevicesDroppingAlerts | warning | Devices are too slow to take alerts (queue full). |
| InvalidAlertMessages | warning | The worker cannot parse alerts: usually a coordinator/worker version mismatch. |
| ReplayFailures | warning | Reconnecting devices may be missing alerts they were offline for. |
| SlowAlertDispatch, SlowBroadcast, SlowHttpResponses | warning | p99 latency is high for 10 minutes. |
| WebSocketRejectSpike | warning | Over 100 refused WebSocket connections in 10 minutes. |

Critical alerts repeat every hour until fixed, warnings every four hours.

**Changing the rules:** edit `deploy/prometheus/alerts.yml`, add or adjust a
case in `alerts_test.yml` next to it, and run the tests before deploying
(no installation needed beyond Docker):

```sh
docker run --rm -v "$PWD/deploy/prometheus:/p" --entrypoint promtool \
    prom/prometheus:v3.16.0 test rules /p/alerts_test.yml
```

Then `$COMPOSE restart prometheus`.

## Messages in the logs that are harmless

- Caddy: `failed to sufficiently increase receive buffer size`. HTTP/3 would
  like a larger UDP buffer. On a Linux host,
  `sysctl -w net.core.rmem_max=7500000 net.core.wmem_max=7500000` removes it;
  HTTP/1.1 and HTTP/2 are unaffected either way.
- Caddy: `certutil is not available`. Caddy tries to install its local
  certificate authority into the container's trust store; irrelevant in
  production, where certificates come from Let's Encrypt.
- Compose: `Found orphan containers`. Left over from another overlay (for
  example the benchmark's workers). Add `--remove-orphans` to `up` once.

## Trying it on your own machine first

Set `API_DOMAIN=localhost`, `WS_DOMAIN=ws.localhost`, any `ACME_EMAIL` and
`POSTGRES_PASSWORD` (use the password your existing `pgdata` volume was
created with: `pulsegrid` for the development volume) in your shell, and
create a dummy `deploy/secrets/telegram_bot_token` (any text; no alert is
sent unless one fires). Then
either log in and `pull` as above, or build the image on your machine with
`$COMPOSE up -d --build` (slow the first time). Wait about 20 seconds before
testing. Caddy signs `localhost` with its own certificate authority, so
browsers warn and curl needs `-k`. Stop the benchmark and any `cargo run`
processes first: ports 80 and 443 must be free. Expected: `/health` answers
200, `/metrics` and `/ready` answer 404 on both names, and plain HTTP
redirects to HTTPS.

## Updating and rolling back

```sh
git pull               # compose and Caddy files; the application is in the image
# set PULSEGRID_VERSION in .env.docker to the new sha-<short commit>
$COMPOSE pull
$COMPOSE up -d --no-build
```

To roll back, set `PULSEGRID_VERSION` back to the previous `sha-...` tag and
run the same two commands. Tags are listed under the repository's Packages on
GitHub. Volumes (`pgdata`, `natsdata`, `redisdata`, `caddy_data`) survive
all of this. Do not run `$COMPOSE down -v`: it deletes the data, including
the certificates.

A database migration that has already run is not undone by an older image;
check the release's migrations before rolling back across one.

## What this does not give you

- **High availability.** Each data store runs once; a server or disk failure
  is downtime and can be data loss.
- **Backups.** Nothing in this overlay copies data off the server. Treat
  `pgdata` as unprotected until backups exist.
- **Monitoring of the infrastructure.** The application's metrics are
  scraped and alerted on, but Postgres, Redis, NATS, Caddy and the host
  (disk, memory, CPU) have no exporters, so a full disk or a dead
  database is noticed only through its effect on the application. There
  is also no "heartbeat" from outside: if the whole server dies,
  nothing is left to send the alert.
- **Authentication on Redis and NATS.** They are safe only because they are
  not published; anything else on the Compose network can reach them.
- **An operator IP allow-list.** The alert and dashboard endpoints are open
  to the internet and protected by the operator key and rate limits. Add a
  `remote_ip` matcher in `deploy/Caddyfile` if operators have fixed
  addresses.
- **Image scanning.** CI audits the Rust dependencies (`cargo audit`) but
  does not scan the base image for known vulnerabilities.
- **Host hardening:** SSH keys only, automatic security updates and the
  firewall are the server's job, not this repository's.
