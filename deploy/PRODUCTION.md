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

2. Log in to the registry (above), then pull and start:

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
created with: `pulsegrid` for the development volume) in your shell. Then
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
- **Metrics collection and alerting.** `/metrics` is hidden from the internet
  and nothing scrapes it yet.
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
