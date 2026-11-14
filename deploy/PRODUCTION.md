# Running PulseGrid in production (single server, Docker Compose)

This sets up one Linux server with automatic HTTPS. It is a single-instance
deployment: one Postgres, one Redis, one NATS, one coordinator, one worker.
See "What this does not give you" at the end.

## What you need

- A Linux server with Docker and the Compose plugin, and this repository on it.
- Two DNS records (A and/or AAAA) pointing at the server's public address:
  one for the API and dashboard (`API_DOMAIN`, e.g. `api.example.com`) and one
  for device WebSockets (`WS_DOMAIN`, e.g. `ws.example.com`).
- The server's firewall allowing only SSH, 80 and 443 in. The overlay
  publishes nothing else; the firewall is the second layer.

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

   `POSTGRES_PASSWORD` is only read when the `pgdata` volume is first created.
   Changing it afterwards does not change the database's password, and the
   services can no longer log in. To rotate it later, run
   `ALTER USER pulsegrid PASSWORD '...'` inside Postgres, then update
   `.env.docker` and restart.

   The overlay runs the services with `PULSEGRID_ENV=production`, which makes
   them refuse weak or placeholder secrets at start-up. That refusal is the
   safety net: if a service exits right away, read its log first.

2. Start everything:

   ```sh
   docker compose --env-file .env.docker \
       -f docker-compose.yml -f docker-compose.prod.yml up -d --build
   ```

   The first image build takes many minutes. Caddy obtains certificates for
   both names on the first request; follow it with
   `docker compose -f docker-compose.yml -f docker-compose.prod.yml logs -f caddy`.

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
Connect. Register a device and send an alert; the device should receive it
over `wss://ws.example.com/...` (the address the coordinator hands out comes
from `WORKER_PUBLIC_URL`, which the overlay sets from `WS_DOMAIN`).

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
registration, tickets, the `wss://ws.example.com` connection, an alert, and
the delivery report, all through Caddy.

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
created with: `pulsegrid` for the development volume) in your shell, start
the same command, and wait about 20 seconds before testing. Caddy signs
`localhost` with its own certificate authority, so browsers warn and curl
needs `-k`. Stop the benchmark and any `cargo run` processes first: ports
80 and 443 must be free. Expected: `/health` answers 200, `/metrics` and
`/ready` answer 404 on both names, and plain HTTP redirects to HTTPS.

## Updating and rolling back

```sh
git pull
docker compose --env-file .env.docker -f docker-compose.yml -f docker-compose.prod.yml up -d --build
```

Volumes (`pgdata`, `natsdata`, `redisdata`, `caddy_data`) survive this. To roll
back, check out the previous commit and run the same command. Do not run
`docker compose down -v`: it deletes the data, including the certificates.

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
- **Host hardening:** SSH keys only, automatic security updates and the
  firewall are the server's job, not this repository's.
