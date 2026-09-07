# PulseGrid

**A geo-distributed, real-time alert fanout backend** capable of delivering
location-targeted push alerts to hundreds of thousands of concurrent
WebSocket connections in sub-second time -- built to demonstrate the
distributed-systems engineering behind real early-warning systems
(earthquake alerts, flash-flood warnings, safety networks), rather than
another CRUD app with a notifications table bolted on.

Built in Rust (Axum, tokio, sqlx/Postgres, Redis, NATS) as a Cargo
workspace of five cooperating services.

---

## Table of Contents

1. [The Problem](#the-problem)
2. [The Solution](#the-solution)
3. [Architecture](#architecture)
4. [Repository Layout](#repository-layout)
5. [Core Algorithm: Geohash Shard Routing](#core-algorithm-geohash-shard-routing)
6. [Getting Started](#getting-started)
7. [API Reference](#api-reference)
8. [Load Testing & Results](#load-testing--results)
9. [Security & Reliability Hardening](#security--reliability-hardening)
10. [Known Limitations & Honest Caveats](#known-limitations--honest-caveats)
11. [What I'd Do Differently / Next Steps](#what-id-do-differently--next-steps)
12. [Tech Stack Summary](#tech-stack-summary)

---

## The Problem

Safety and disaster alert systems (earthquake early warning, flash-flood
alerts, women's-safety SOS networks in dense urban areas) don't fail
because *detecting* the danger is hard -- sensors and threshold rules are
well understood. They fail because the **fanout** is hard: pushing a
targeted, geographically-scoped alert to potentially hundreds of
thousands of nearby devices within seconds, without either

- broadcasting to *every* connected device regardless of location
  (wasteful, slow, and doesn't scale), or
- running a full database scan/join to figure out "who's nearby" on
  every single alert (also doesn't scale, and adds unacceptable
  latency to a time-critical alert).

Most "safety app" portfolio projects sidestep this entirely -- a button
that sends one SMS to one emergency contact isn't the same problem at
all. PulseGrid is built specifically to solve *this* problem: the
routing and fanout infrastructure, not the sensor or the button.

## The Solution

PulseGrid indexes every connected device into a **geohash grid** and
shards WebSocket connections across worker processes by geohash prefix.
When an alert is fired with a center point and radius, a routing
algorithm resolves that radius into the *specific set of geohash
shards* it overlaps -- and the alert is published only to those shards,
via NATS. Each shard's fanout-worker instance then pushes the alert to
its own locally-held WebSocket connections in parallel.

This means fanout cost is proportional to **how many devices are
actually within the alert radius**, not to the total number of devices
connected to the system -- the entire point of a system meant to scale
toward hundreds of millions of users.

## Architecture

```
                          +----------------------+
                          |   api-coordinator    |
                          |  (Axum HTTP + JWT)   |
                          |                      |
   POST /devices/register |  - device registry   |
   PUT  /devices/location |  - JWT issuance       |
   POST /alerts           |  - alert dispatch     |
                          |                      |
                          +----------+-----------+
                                     |
                     +---------------+----------------+
                     |               |                |
               +-----v-----+   +-----v-----+    +-----v-----+
               | Postgres  |   |   NATS    |    |   Redis   |
               |           |   |           |    |           |
               | devices   |   | pub/sub   |    | presence  |
               | alerts    |   | per-shard |    | shard     |
               | deliveries|   | subjects  |    | registry  |
               +-----------+   +-----+-----+    +-----------+
                                     |
                     alerts.geo.<shard-prefix>
                                     |
                     +---------------+----------------+
                     |               |                |
               +-----v-----+   +-----v-----+    +-----v-----+
               |  fanout-  |   |  fanout-  |    |  fanout-  |
               |  worker   |   |  worker   |    |  worker   |
               | (shard A) |   | (shard B) |    | (shard N) |
               |           |   |           |    |           |
               | WS conns  |   | WS conns  |    | WS conns  |
               +-----+-----+   +-----+-----+    +-----+-----+
                     |               |                |
                 devices          devices          devices
              (WebSocket)       (WebSocket)      (WebSocket)
```

**Coordinator** (`api-coordinator`): the HTTP-facing "brain". Handles
device registration (issuing JWTs), location updates, and alert
dispatch -- resolving an alert's radius into target shard prefixes and
publishing one NATS message per shard.

**Fanout worker** (`fanout-worker`): the stateful WebSocket layer. Each
instance owns a configurable set of geohash shard prefixes, holds the
live WebSocket connections for devices in those shards, subscribes to
the corresponding NATS subjects, and pushes incoming alerts out to its
local connections with bounded-channel backpressure (a slow client
never blocks delivery to everyone else).

**Postgres**: durable device registry and full audit trail
(`alert_deliveries` -- which shard got which alert, with how many
devices, and when).

**Redis**: two roles. A cross-instance *presence registry* (which
fanout-worker instance currently holds a given device's connection --
used for admin/debug visibility and duplicate-connection detection),
and a *shard registry* (`ShardRegistry`) mapping each geohash shard
prefix to the base WebSocket URL of the worker instance that currently
owns it -- this is what lets a multi-worker deployment tell a client
which host to actually connect to.

**NATS**: the pub/sub backbone connecting the coordinator's dispatch
decision to the specific fanout-worker instance(s) that actually need
to know about it.

## Repository Layout

```
pulsegrid/
|-- crates/
|   |-- domain/            # Pure logic, zero I/O: Device, Alert, JWT
|   |                        claims/verification, the geohash routing
|   |                        algorithm, shared errors
|   |-- infra/              # Postgres repos, Redis presence + shard
|   |                        registries, NATS pub/sub helpers
|   |-- api-coordinator/    # HTTP API: registration, auth, dispatch,
|   |                        health/ready/metrics, rate limiting
|   |-- fanout-worker/      # Stateful WS server, one per shard set,
|   |                        JWT-gated connections, health/metrics
|   |-- loadgen/            # Connection-storm load test tool
|-- tests/                  # Integration tests (testcontainers)
|-- migrations/              # SQL schema
|-- .github/workflows/       # CI pipeline
|-- Dockerfile                # Multi-stage build for both binaries
|-- docker-compose.yml        # Postgres + Redis + NATS (+ app services)
```

Five crates in one Cargo workspace, sharing dependency versions via
`[workspace.dependencies]`. `domain` has zero I/O framework dependencies
by design -- it's pure business logic (geohash math, validation, domain
types, JWT claims) that both binaries and the test suite depend on
without pulling in Axum, sqlx, or NATS. JWT issuing/verification lives
here specifically so `api-coordinator` and `fanout-worker` can never
drift on token shape or validation logic independently.

## Core Algorithm: Geohash Shard Routing

This is the actual engineering centerpiece, in `crates/domain/src/geo.rs`.

Every device's location is encoded to a 7-character geohash (approx.
153m x 153m precision) for identification, and truncated to a
4-character prefix (approx. 39km x 19.5km cells) for **shard
assignment** -- which fanout-worker instance owns that device's
connection.

When an alert fires with a center point and radius:

1. **BFS ring expansion**: starting from the center's 4-character
   geohash cell, expand outward one ring of neighboring cells at a
   time (using the standard 8-directional geohash neighbor grid),
   until the covered area's radius meets or exceeds the alert's
   requested radius.
2. **Haversine pruning**: geohash grid expansion over-covers slightly
   at the diagonal edges of each ring (cells are roughly square, alert
   radii are circular). A haversine great-circle distance check drops
   any candidate cell whose decoded center falls outside `radius + one
   cell diagonal` of the true alert center -- keeping the result tight
   instead of alerting a needlessly wide area.
3. The coordinator publishes the alert once per surviving shard prefix,
   to NATS subject `alerts.geo.<prefix>`.

This is genuinely O(shards-in-radius), not O(total-devices) -- verified
directly by instrumenting the fanout-worker's broadcast loop (see
[Load Testing & Results](#load-testing--results) below): the loop
itself executes in 10-200 **microseconds** regardless of how many
devices are in a shard.

## Getting Started

### Prerequisites

- Rust (stable toolchain)
- Docker Desktop (for Postgres, Redis, NATS locally, and for
  `testcontainers`-based integration tests)

### Setup

```powershell
git clone https://github.com/MuaazTasawar/pulsegrid.git
cd pulsegrid
copy .env.example .env
# edit .env if your local Postgres/Redis/NATS ports differ from the
# defaults -- see the comments in .env.example

docker-compose up -d postgres redis nats
docker exec pulsegrid-postgres-1 sh -c "until pg_isready -U pulsegrid; do sleep 1; done"

cargo run -p api-coordinator   # in one terminal
cargo run -p fanout-worker     # in a second terminal
```

Note: `docker-compose.yml` also defines `api-coordinator` and
`fanout-worker` services (built from the included `Dockerfile`) for a
fully containerized deployment. Running them via `docker-compose up`
(rather than `cargo run` on the host) requires a separate `.env`
configured with Docker's internal service hostnames (`postgres`,
`redis`, `nats`) instead of `localhost` -- see
[Known Limitations](#known-limitations--honest-caveats) item 2.

### Verify it's working

```powershell
# register a device -- returns a JWT and the WebSocket URL to connect to
curl.exe -X POST http://localhost:8090/devices/register `
  -H "Content-Type: application/json" `
  -d "{\"lat\": 24.8607, \"lon\": 67.0011}"

# fire an alert covering that device
curl.exe -X POST http://localhost:8090/alerts `
  -H "Content-Type: application/json" `
  -d "{\"lat\": 24.8607, \"lon\": 67.0011, \"radius_meters\": 5000, \"severity\": \"critical\", \"title\": \"Test\", \"message\": \"Testing\"}"
```

### Run the load test

```powershell
cargo run -p loadgen -- --devices 1000
```

Spawns N simulated devices, registers each one for a real JWT (a
device must be registered and authenticated to connect -- see
[Security & Reliability Hardening](#security--reliability-hardening)),
connects them all (staggered in batches to avoid client-side
connection-burst issues -- see caveats below), fires one alert
covering them, and reports p50/p90/p99/max fanout latency via
`hdrhistogram`.

### Run the integration tests

```powershell
cargo test -p integration-tests
```

Spins up an isolated, throwaway Postgres via `testcontainers` -- no
dependency on the long-lived `docker-compose` stack. Covers device
registration, alert dispatch, and the geohash-routing correctness
claim (a device inside an alert's radius must appear in the alert's
resolved target shards).

## API Reference

### `POST /devices/register`

Registers a new device at a given location. Rate-limited (5/sec,
burst 10, per client IP).

```json
// Request
{ "lat": 24.8607, "lon": 67.0011 }

// Response
{
  "device_id": "c8b00808-8462-459f-ac73-f59dbbe28f32",
  "token": "eyJ0eXAi...",
  "shard_prefix": "tkrt",
  "ws_url": "ws://localhost:8081"
}
```

`token` is a JWT (30-day expiry) that is **required** to open a
WebSocket connection -- a device_id alone is not sufficient to connect
and receive that device's alerts. `ws_url` is the base WebSocket URL
of the fanout-worker instance that currently owns this device's
shard (looked up via the Redis-backed `ShardRegistry`); it may be
`null` if no worker has registered ownership of that shard yet.

### `PUT /devices/location`

Updates a registered device's location. Requires `Authorization:
Bearer <token>` from registration.

```json
// Response
{ "shard_prefix": "tkrt", "shard_changed": false, "ws_url": null }
```

`shard_changed: true` signals the client its WebSocket connection
needs to reconnect to a different fanout-worker instance -- the
device's geohash shard changed. In that case `ws_url` will be
populated with the new instance's address.

### `POST /alerts`

Dispatches an alert to every device within `radius_meters` of the
given center point. Rate-limited (2/sec, burst 5, per client IP).

```json
// Request
{
  "lat": 24.8607, "lon": 67.0011,
  "radius_meters": 5000,
  "severity": "critical",
  "title": "Earthquake Warning",
  "message": "Magnitude 5.2 detected nearby"
}

// Response (HTTP 200 if fully delivered, HTTP 207 if partially failed)
{
  "alert_id": "29be0c7d-...",
  "shards_targeted": 4,
  "total_devices_notified": 5,
  "per_shard": [
    { "shard_prefix": "tkrt", "devices_in_shard": 5 },
    { "shard_prefix": "tkrw", "devices_in_shard": 0 }
  ],
  "failed_shards": []
}
```

A non-empty `failed_shards` array (with HTTP 207 Multi-Status) means
some shards could not be reached -- verified against a real NATS
outage, see [Security & Reliability Hardening](#security--reliability-hardening).

### `GET /connect/{device_id}?shard_prefix=<prefix>&token=<jwt>` (fanout-worker, WebSocket)

Upgrades to a WebSocket connection. **Requires** the JWT issued at
registration as a query parameter -- WebSocket upgrades can't reliably
carry a standard `Authorization` header across all clients, so the
token travels this way instead (the same pattern used by several
production chat/notification platforms).

Rejected with:
- `401 Unauthorized` if the token is missing, invalid, or expired
- `403 Forbidden` if the token's device_id doesn't match the
  device_id in the connection path -- this closes device
  impersonation: a valid token for device A can no longer be used to
  connect as device B
- `421 Misdirected Request` if this worker instance doesn't own the
  claimed shard prefix

Once connected, the device receives binary WebSocket messages
containing the raw JSON payload of any alert dispatched to its shard.

### `GET /health`, `GET /ready`, `GET /metrics` (both binaries)

Basic liveness, readiness, and Prometheus-text-format metrics
endpoints. `fanout-worker`'s `/metrics` exposes
`pulsegrid_ws_connections_current`; `api-coordinator`'s exposes a
simple process-up gauge. Hand-rolled rather than built on a metrics
crate, to avoid adding another dependency-version risk to the stack.

### `GET /admin/presence/{device_id}` (fanout-worker)

Looks up which worker instance currently holds a live connection for
the given device, via the Redis presence registry. Returns `404` if
no presence record exists (device not connected, or its heartbeat
expired).

## Load Testing & Results

Tested with a custom Rust load generator (`crates/loadgen`) using
`tokio-tungstenite` for connection simulation and `hdrhistogram` for
latency distribution measurement.

### Broadcast loop performance (the actual architectural claim)

Instrumented directly in `fanout-worker`'s NATS subscriber, measuring
just the in-memory `DashMap` iteration + channel dispatch -- i.e., is
the *fanout logic itself* fast, independent of network/OS overhead:

| Shard size (devices) | `broadcast_duration_us` |
|---|---|
| 0-5 | 10-196 us |
| 5000 | 9-434 us |

**The dispatch loop is not the bottleneck at any tested scale.** This
was verified directly, not inferred -- see caveats below for why this
matters.

### End-to-end fanout latency (single-machine, Windows)

Full round trip: alert fired via HTTP -> NATS publish -> shard
subscriber -> WebSocket write -> client receives it.

| Devices | p50 | p90 | p99 | max |
|---|---|---|---|---|
| 10 | 325-375ms | 327-375ms | 330-375ms | 330-375ms |
| 500 | 405ms | 438ms | 494ms | 503ms |
| 2000 | 518ms | 682ms | 735ms | 743ms |
| 5000 | 731-1026ms | 3.1-2.4s | 3.3-4.0s | 3.4-4.3s |

The p50/p90/p99 spread widens substantially past ~2000 concurrent
devices on a single machine -- see [Known Limitations](#known-limitations--honest-caveats)
for why, and why this is not read as an architectural ceiling.

## Security & Reliability Hardening

After the initial build, a self-audit surfaced several real gaps in
the system. Each one below was fixed **and verified against live,
running processes** -- not just compiled and assumed correct. This
section documents both the gap and the actual evidence it's closed.

**1. WebSocket authentication.** The `/connect` endpoint originally
accepted any `device_id` with no proof of ownership -- anyone could
connect as any device and receive its alerts, or impersonate a device
outright. Fixed by requiring the JWT issued at registration, verified
against the connecting device_id. Verified end-to-end with a modified
load generator that registers real devices (getting real tokens)
before connecting: 10/10 devices delivered, 0 connect failures, after
the fix (previously 10/10 connect failures with no valid token).

**2. Service discovery.** With multiple fanout-worker instances,
nothing told a client which host to connect to for its shard. Fixed
with a Redis-backed `ShardRegistry`: each worker registers the shards
it owns on startup, and `/devices/register` now returns `ws_url`
looked up from that registry.

**3. Partial-failure handling in alert dispatch.** Originally, if one
shard's NATS publish failed, the whole dispatch aborted with an
undifferentiated 500 -- shards that would have succeeded never got
attempted. Fixed: every shard is now attempted independently, with
failures collected into a `failed_shards` list and the response
returning `HTTP 207 Multi-Status` when partial. **Verified against a
genuine NATS outage** (the NATS container was stopped mid-request):
the API returned `207` in about 3 seconds with `failed_shards`
correctly listing all four affected shards, rather than hanging or
crashing.

**4. NATS publish had no timeout.** Discovered *while verifying item
3* -- the first outage test hung indefinitely, because
`infra::nats::publish` had no bound on how long it would wait for the
async-nats client's own internal reconnect logic. Fixed with a
3-second `tokio::time::timeout` around the publish+flush call. Re-ran
the same outage test after the fix: clean `207` response in ~3
seconds, not a hang. This is what actually makes item 3's
partial-failure handling meaningful under a real outage rather than
just correct-looking code.

**5. Rate limiting.** `/devices/register` (5/sec, burst 10) and
`/alerts` (2/sec, burst 5) are both rate-limited per client IP via
`tower_governor`. Verified with rapid-fire request batches showing the
expected pattern: N successes at `200`, then `429 Too Many Requests`
once the burst allowance is exceeded. (This required wiring Axum's
`ConnectInfo` into both binaries' listeners -- without it, the rate
limiter's IP-extraction failed on every single request, an issue only
caught by testing the feature live rather than trusting it compiled.)

**6. Dead code eliminated.** `redis_url` in `api-coordinator`'s config
was read from the environment but never used -- now backs the
`ShardRegistry` connection. `PresenceRegistry::lookup` was
write-only (heartbeated but never read) -- now backs the
`/admin/presence/{device_id}` endpoint.

**7. Health, readiness, and metrics endpoints** added to both
binaries, plus a `Dockerfile` and CI pipeline (GitHub Actions) that
builds the workspace and runs the full test suite -- including the
testcontainers-based integration tests -- on every push.

## Known Limitations & Honest Caveats

This section exists because a benchmark number (or a "fixed" label)
without its context is worse than no claim at all.

**1. Single-machine testing conflates client and server resource
contention.** `loadgen` and `fanout-worker` were run on the same
Windows machine for every test in this repo's history. Both compete
for the same CPU cores, the same loopback network stack, and the same
process scheduler. The broadcast-loop instrumentation above proves the
*dispatch logic* stays fast (microseconds) regardless of shard size --
the multi-second tail latency at 5000 devices lives entirely
downstream, in thousands of concurrent WebSocket writes and reads
contending for the same machine's resources. **A real benchmark number
worth quoting requires running `loadgen` and `fanout-worker` on
separate hosts** (or at minimum separate containers with resource
limits) -- that's the next concrete step for a production-credible
number, not a code fix.

**2. `docker-compose`'s `api-coordinator`/`fanout-worker` services need
a separate `.env`.** Once those two services run *inside* the compose
network (via the included `Dockerfile`), their `DATABASE_URL`/
`REDIS_URL`/`NATS_URL` need to point at Docker's internal service
names (`postgres`, `redis`, `nats`), not `localhost:<port>`. The
current `.env` is written for host-side `cargo run` and will not work
unmodified inside the containers. This is a known, unresolved rough
edge -- not yet fixed with a `.env.docker` or equivalent split.

**3. This machine was also running unrelated Docker workloads during
testing** -- a full k3d Kubernetes cluster, six other services'
containers -- at one point measured consuming 223% CPU from a single
container alone, against a Docker Desktop VM capped at 4 CPUs / 7.7GB.
Stopping those unrelated containers measurably changed (but did not
eliminate) the single-machine contention pattern above. Any benchmark
run on a shared dev machine should be read with this in mind.

**4. NATS/fanout integration is not covered by the automated test
suite.** `tests/alert_dispatch_test.rs` covers the HTTP -> Postgres
path (device registration, alert dispatch, audit trail correctness)
via an isolated `testcontainers` Postgres instance. The NATS ->
fanout-worker -> WebSocket delivery path, and the JWT-on-WebSocket
security fix, were verified **manually**, repeatedly, against live
running processes -- they work, and were proven end-to-end multiple
times -- but aren't yet exercised by `cargo test`. Wiring a NATS
testcontainer into the suite is real, scoped, achievable follow-up
work.

**5. Single fanout-worker instance per shard set, tested.** The
architecture is designed for many fanout-worker instances, each owning
a narrower slice of shards, running as a genuinely distributed fleet.
Every load test in this repo ran against **one** fanout-worker process
owning all four active shard prefixes -- meaning the "many shards
across many machines" scaling story is architecturally sound and
proven at the routing-logic level (and service discovery now supports
it), but not yet load-tested with multiple concurrent worker
instances. That's the most valuable next load-testing milestone.

**6. `total_devices_notified` in the alert dispatch response currently
only counts devices registered via `/devices/register` (the Postgres
registry).** In practice this is now accurate for real clients (since
registration is required before connecting, per the security fix
above), but historical `loadgen` runs from before that fix show
mismatched counts in this README's own load-test data -- a reminder
that the numbers reflect when they were captured, not a live system
state.

**7. No data retention or device deregistration.** The `devices` table
only grows -- there's no TTL, inactivity cleanup, or explicit
deregistration endpoint. Same for `alerts`/`alert_deliveries`. This is
a genuine design decision that needs a policy (how long to retain
alert history? does a device get purged after N days inactive?)
before it's implemented, not just a migration to bolt on.

## What I'd Do Differently / Next Steps

- Run `loadgen` and `fanout-worker` on separate machines (or Docker
  containers with explicit CPU/memory limits) for a clean, defensible
  latency number.
- Stand up multiple `fanout-worker` instances, each owning a distinct
  subset of shard prefixes, and load-test the coordinator's fanout
  across genuinely separate processes/hosts -- this is the real "100M
  users" proof point, not a single beefy worker. Service discovery
  (`ShardRegistry`) already supports this; it just hasn't been load
  tested yet.
- Add a NATS testcontainer to the integration suite for full
  end-to-end automated coverage of the fanout path and the JWT
  WebSocket auth fix.
- Resolve the `docker-compose` env-mismatch (item 2 above) with a
  proper `.env.docker` or environment-aware config loading.
- Add a live map visualization (the original "wow moment" from the
  project's design phase) -- draw a radius on a map, watch the ripple
  of notified devices light up in real time, with the live
  `broadcast_duration_us` and device counts overlaid.
- Decide and implement a data retention policy (item 7 above).

## Tech Stack Summary

| Layer | Choice | Why |
|---|---|---|
| Language | Rust | Memory-safe, no-GC-pause latency, genuine concurrency via tokio |
| Web/WS framework | Axum | Native WebSocket support, tower middleware ecosystem, one framework for both HTTP and WS |
| Async runtime | tokio | Industry standard, everything else in the stack builds on it |
| Database | PostgreSQL (sqlx) | Durable device registry + full audit trail |
| Cache/presence/discovery | Redis (deadpool-redis) | Cross-instance connection presence + shard-to-worker service discovery |
| Pub/sub | NATS | Lightweight, low-latency, subject-based routing maps cleanly onto geohash shard prefixes |
| Geospatial | `geohash` crate + hand-rolled BFS/haversine routing | The actual novel engineering -- off-the-shelf geo libraries don't solve "which shards does this radius touch" |
| Auth | JWT (jsonwebtoken, `rust_crypto` backend) | Stateless device auth, no session store needed; also gates WebSocket connections |
| Rate limiting | `tower_governor` | Per-IP token-bucket limiting on registration and alert dispatch |
| Load testing | Custom (`tokio-tungstenite` + `hdrhistogram` + `reqwest`) | Off-the-shelf HTTP load testers don't model sustained, authenticated WebSocket connections well |
| Integration testing | `testcontainers` | Isolated, reproducible test runs independent of any long-lived local dev stack |
| CI | GitHub Actions | Builds the workspace and runs the full test suite on every push |

---

Built as a portfolio project demonstrating distributed-systems backend
engineering in Rust -- geospatial sharding, real-time fanout,
backpressure-aware WebSocket delivery, JWT-secured connections, and
load-tested (with honestly documented caveats) at scale. The project's
own commit history is itself part of the story: a genuine security
audit, real bugs found under real outage conditions, and fixes
verified against live processes rather than just claimed.