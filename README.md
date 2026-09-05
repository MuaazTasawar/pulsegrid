# PulseGrid

**A geo-distributed, real-time alert fanout backend** capable of delivering
location-targeted push alerts to hundreds of thousands of concurrent
WebSocket connections in sub-second time â€” built to demonstrate the
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
9. [Known Limitations & Honest Caveats](#known-limitations--honest-caveats)
10. [What I'd Do Differently / Next Steps](#what-id-do-differently--next-steps)
11. [Tech Stack Summary](#tech-stack-summary)

---

## The Problem

Safety and disaster alert systems (earthquake early warning, flash-flood
alerts, women's-safety SOS networks in dense urban areas) don't fail
because *detecting* the danger is hard â€” sensors and threshold rules are
well understood. They fail because the **fanout** is hard: pushing a
targeted, geographically-scoped alert to potentially hundreds of
thousands of nearby devices within seconds, without either

- broadcasting to *every* connected device regardless of location
  (wasteful, slow, and doesn't scale), or
- running a full database scan/join to figure out "who's nearby" on
  every single alert (also doesn't scale, and adds unacceptable
  latency to a time-critical alert).

Most "safety app" portfolio projects sidestep this entirely â€” a button
that sends one SMS to one emergency contact isn't the same problem at
all. PulseGrid is built specifically to solve *this* problem: the
routing and fanout infrastructure, not the sensor or the button.

## The Solution

PulseGrid indexes every connected device into a **geohash grid** and
shards WebSocket connections across worker processes by geohash prefix.
When an alert is fired with a center point and radius, a routing
algorithm resolves that radius into the *specific set of geohash
shards* it overlaps â€” and the alert is published only to those shards,
via NATS. Each shard's fanout-worker instance then pushes the alert to
its own locally-held WebSocket connections in parallel.

This means fanout cost is proportional to **how many devices are
actually within the alert radius**, not to the total number of devices
connected to the system â€” the entire point of a system meant to scale
toward hundreds of millions of users.

## Architecture

```
                         â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
                         â”‚   api-coordinator    â”‚
                         â”‚  (Axum HTTP + JWT)   â”‚
                         â”‚                       â”‚
   POST /devices/registerâ”‚  - device registry    â”‚
   PUT  /devices/locationâ”‚  - JWT issuance        â”‚
   POST /alerts          â”‚  - alert dispatch      â”‚
                         â”‚                       â”‚
                         â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                                    â”‚
                    â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¼â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
                    â”‚               â”‚               â”‚
              â”Œâ”€â”€â”€â”€â”€â–¼â”€â”€â”€â”€â”€â”   â”Œâ”€â”€â”€â”€â”€â–¼â”€â”€â”€â”€â”€â”   â”Œâ”€â”€â”€â”€â”€â–¼â”€â”€â”€â”€â”€â”
              â”‚ Postgres  â”‚   â”‚   NATS    â”‚   â”‚   Redis   â”‚
              â”‚           â”‚   â”‚           â”‚   â”‚           â”‚
              â”‚ devices   â”‚   â”‚ pub/sub   â”‚   â”‚ presence  â”‚
              â”‚ alerts    â”‚   â”‚ per-shard â”‚   â”‚ registry  â”‚
              â”‚ deliveriesâ”‚   â”‚ subjects  â”‚   â”‚           â”‚
              â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜   â””â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”˜   â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                                    â”‚
                    alerts.geo.<shard-prefix>
                                    â”‚
                    â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¼â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
                    â”‚               â”‚               â”‚
              â”Œâ”€â”€â”€â”€â”€â–¼â”€â”€â”€â”€â”€â”   â”Œâ”€â”€â”€â”€â”€â–¼â”€â”€â”€â”€â”€â”   â”Œâ”€â”€â”€â”€â”€â–¼â”€â”€â”€â”€â”€â”
              â”‚  fanout-  â”‚   â”‚  fanout-  â”‚   â”‚  fanout-  â”‚
              â”‚  worker   â”‚   â”‚  worker   â”‚   â”‚  worker   â”‚
              â”‚ (shard A) â”‚   â”‚ (shard B) â”‚   â”‚ (shard N) â”‚
              â”‚           â”‚   â”‚           â”‚   â”‚           â”‚
              â”‚ WS conns  â”‚   â”‚ WS conns  â”‚   â”‚ WS conns  â”‚
              â””â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”˜   â””â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”˜   â””â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”˜
                    â”‚               â”‚               â”‚
                devices          devices          devices
             (WebSocket)       (WebSocket)      (WebSocket)
```

**Coordinator** (`api-coordinator`): the HTTP-facing "brain". Handles
device registration (issuing JWTs), location updates, and alert
dispatch â€” resolving an alert's radius into target shard prefixes and
publishing one NATS message per shard.

**Fanout worker** (`fanout-worker`): the stateful WebSocket layer. Each
instance owns a configurable set of geohash shard prefixes, holds the
live WebSocket connections for devices in those shards, subscribes to
the corresponding NATS subjects, and pushes incoming alerts out to its
local connections with bounded-channel backpressure (a slow client
never blocks delivery to everyone else).

**Postgres**: durable device registry and full audit trail
(`alert_deliveries` â€” which shard got which alert, with how many
devices, and when).

**Redis**: cross-instance presence registry (which fanout-worker
instance currently holds a given device's connection) â€” not used for
routing itself, which lives entirely in the geohash math, but for
admin/debug visibility and duplicate-connection detection.

**NATS**: the pub/sub backbone connecting the coordinator's dispatch
decision to the specific fanout-worker instance(s) that actually need
to know about it.

## Repository Layout

```
pulsegrid/
â”œâ”€â”€ crates/
â”‚   â”œâ”€â”€ domain/            # Pure logic, zero I/O: Device, Alert, the
â”‚   â”‚                       geohash routing algorithm, shared errors
â”‚   â”œâ”€â”€ infra/              # Postgres repos, Redis presence registry,
â”‚   â”‚                       NATS pub/sub helpers
â”‚   â”œâ”€â”€ api-coordinator/    # HTTP API: registration, auth, dispatch
â”‚   â”œâ”€â”€ fanout-worker/      # Stateful WS server, one per shard set
â”‚   â””â”€â”€ loadgen/            # Connection-storm load test tool
â”œâ”€â”€ tests/                  # Integration tests (testcontainers)
â”œâ”€â”€ migrations/              # SQL schema
â””â”€â”€ docker-compose.yml       # Postgres + Redis + NATS for local dev
```

Five crates in one Cargo workspace, sharing dependency versions via
`[workspace.dependencies]`. `domain` has zero I/O dependencies by
design â€” it's pure business logic (geohash math, validation, domain
types) that both binaries and the test suite depend on without pulling
in Axum, sqlx, or NATS.

## Core Algorithm: Geohash Shard Routing

This is the actual engineering centerpiece, in `crates/domain/src/geo.rs`.

Every device's location is encoded to a 7-character geohash (â‰ˆ153m Ã—
153m precision) for identification, and truncated to a 4-character
prefix (â‰ˆ39km Ã— 19.5km cells) for **shard assignment** â€” which
fanout-worker instance owns that device's connection.

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
   cell diagonal` of the true alert center â€” keeping the result tight
   instead of alerting a needlessly wide area.
3. The coordinator publishes the alert once per surviving shard prefix,
   to NATS subject `alerts.geo.<prefix>`.

This is genuinely O(shards-in-radius), not O(total-devices) â€” verified
directly by instrumenting the fanout-worker's broadcast loop (see
[Load Testing & Results](#load-testing--results) below): the loop
itself executes in 10â€“200 **microseconds** regardless of how many
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
# defaults â€” see the comments in .env.example

docker-compose up -d
docker exec pulsegrid-postgres-1 sh -c "until pg_isready -U pulsegrid; do sleep 1; done"

cargo run -p api-coordinator   # in one terminal
cargo run -p fanout-worker     # in a second terminal
```

### Verify it's working

```powershell
# register a device
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

Spawns N simulated devices, connects them all (staggered in batches to
avoid client-side connection-burst issues â€” see caveats below), fires
one alert covering them, and reports p50/p90/p99/max fanout latency
via `hdrhistogram`.

### Run the integration tests

```powershell
cargo test -p integration-tests
```

Spins up an isolated, throwaway Postgres via `testcontainers` â€” no
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
  "token": "eyJ0eXAi...",       // JWT, 30-day expiry
  "shard_prefix": "tkrt"
}
```

### `PUT /devices/location`

Updates a registered device's location. Requires `Authorization:
Bearer <token>` from registration.

```json
// Response
{ "shard_prefix": "tkrt", "shard_changed": false }
```

`shard_changed: true` signals the client its WebSocket connection
needs to reconnect to a different fanout-worker instance â€” the
device's geohash shard changed.

### `POST /alerts`

Dispatches an alert to every device within `radius_meters` of the
given center point.

```json
// Request
{
  "lat": 24.8607, "lon": 67.0011,
  "radius_meters": 5000,
  "severity": "critical",       // info | warning | critical
  "title": "Earthquake Warning",
  "message": "Magnitude 5.2 detected nearby"
}

// Response
{
  "alert_id": "29be0c7d-...",
  "shards_targeted": 4,
  "total_devices_notified": 5,
  "per_shard": [
    { "shard_prefix": "tkrt", "devices_in_shard": 5 },
    { "shard_prefix": "tkrw", "devices_in_shard": 0 },
    ...
  ]
}
```

### `GET /connect/{device_id}?shard_prefix=<prefix>` (fanout-worker, WebSocket)

Upgrades to a WebSocket connection. Rejected with `421 Misdirected
Request` if this worker instance doesn't own the claimed shard prefix.
Once connected, the device receives binary WebSocket messages
containing the raw JSON payload of any alert dispatched to its shard.

## Load Testing & Results

Tested with a custom Rust load generator (`crates/loadgen`) using
`tokio-tungstenite` for connection simulation and `hdrhistogram` for
latency distribution measurement.

### Broadcast loop performance (the actual architectural claim)

Instrumented directly in `fanout-worker`'s NATS subscriber, measuring
just the in-memory `DashMap` iteration + channel dispatch â€” i.e., is
the *fanout logic itself* fast, independent of network/OS overhead:

| Shard size (devices) | `broadcast_duration_us` |
|---|---|
| 0â€“5 | 10â€“196 Âµs |
| 5000 | 9â€“434 Âµs |

**The dispatch loop is not the bottleneck at any tested scale.** This
was verified directly, not inferred â€” see caveats below for why this
matters.

### End-to-end fanout latency (single-machine, Windows)

Full round trip: alert fired via HTTP â†’ NATS publish â†’ shard
subscriber â†’ WebSocket write â†’ client receives it.

| Devices | p50 | p90 | p99 | max |
|---|---|---|---|---|
| 10 | 375ms | 375ms | 375ms | 375ms |
| 500 | 405ms | 438ms | 494ms | 503ms |
| 2000 | 518ms | 682ms | 735ms | 743ms |
| 5000 | 731â€“1026ms | 3.1â€“2.4s | 3.3â€“4.0s | 3.4â€“4.3s |

The p50/p90/p99 spread widens substantially past ~2000 concurrent
devices on a single machine â€” see below for why, and why this is not
read as an architectural ceiling.

## Known Limitations & Honest Caveats

This section exists because a benchmark number without its context is
worse than no benchmark at all.

**1. Single-machine testing conflates client and server resource
contention.** `loadgen` and `fanout-worker` were run on the same
Windows machine for every test in this repo's history. Both compete
for the same CPU cores, the same loopback network stack, and the same
process scheduler. The broadcast-loop instrumentation above proves the
*dispatch logic* stays fast (microseconds) regardless of shard size â€”
the multi-second tail latency at 5000 devices lives entirely
downstream, in thousands of concurrent WebSocket writes and reads
contending for the same machine's resources. **A real benchmark number
worth quoting requires running `loadgen` and `fanout-worker` on
separate hosts** (or at minimum separate containers with resource
limits) â€” that's the next concrete step for a production-credible
number, not a code fix.

**2. This machine was also running unrelated Docker workloads during
testing** â€” a full k3d Kubernetes cluster, six other services'
containers â€” at one point measured consuming 223% CPU from a single
container alone, against a Docker Desktop VM capped at 4 CPUs / 7.7GB.
Stopping those unrelated containers measurably changed (but did not
eliminate) the single-machine contention pattern above. Any benchmark
run on a shared dev machine should be read with this in mind.

**3. NATS/fanout integration is not covered by the automated test
suite.** `tests/alert_dispatch_test.rs` covers the HTTP â†’ Postgres
path (device registration, alert dispatch, audit trail correctness)
via an isolated `testcontainers` Postgres instance. The NATS â†’
fanout-worker â†’ WebSocket delivery path was verified **manually**,
repeatedly, against live running processes (see commit history) â€” it
works, and was proven end-to-end multiple times â€” but isn't yet
exercised by `cargo test`. Wiring a NATS testcontainer into the suite
is real, scoped, achievable follow-up work.

**4. Single fanout-worker instance per shard set, tested.** The
architecture is designed for many fanout-worker instances, each owning
a narrower slice of shards, running as a genuinely distributed fleet.
Every load test in this repo ran against **one** fanout-worker process
owning all four active shard prefixes â€” meaning the "many shards
across many machines" scaling story is architecturally sound and
proven at the routing-logic level, but not yet load-tested with
multiple concurrent worker instances. That's the most valuable next
load-testing milestone.

**5. `total_devices_notified` in the alert dispatch response currently
only counts devices registered via `/devices/register` (the Postgres
registry) â€” `loadgen`'s simulated devices connect directly via
WebSocket without registering first, so they're invisible to that
count even though they genuinely receive the alert. Worth reconciling
before this number appears in a demo.

## What I'd Do Differently / Next Steps

- Run `loadgen` and `fanout-worker` on separate machines (or Docker
  containers with explicit CPU/memory limits) for a clean, defensible
  latency number.
- Stand up multiple `fanout-worker` instances, each owning a distinct
  subset of shard prefixes, and load-test the *coordinator's* fanout
  across genuinely separate processes/hosts â€” this is the real "100M
  users" proof point, not a single beefy worker.
- Add a NATS testcontainer to the integration suite for full
  end-to-end automated coverage.
- Reconcile `total_devices_notified` to reflect live WebSocket
  connections, not just the Postgres device registry.
- Add a live map visualization (the original "wow moment" from the
  project's design phase) â€” draw a radius on a map, watch the ripple
  of notified devices light up in real time, with the live
  `broadcast_duration_us` and device counts overlaid.

## Tech Stack Summary

| Layer | Choice | Why |
|---|---|---|
| Language | Rust | Memory-safe, no-GC-pause latency, genuine concurrency via tokio |
| Web/WS framework | Axum | Native WebSocket support, tower middleware ecosystem, one framework for both HTTP and WS |
| Async runtime | tokio | Industry standard, everything else in the stack builds on it |
| Database | PostgreSQL (sqlx) | Durable device registry + full audit trail |
| Cache/presence | Redis (deadpool-redis) | Cross-instance connection presence lookup |
| Pub/sub | NATS | Lightweight, low-latency, subject-based routing maps cleanly onto geohash shard prefixes |
| Geospatial | `geohash` crate + hand-rolled BFS/haversine routing | The actual novel engineering â€” off-the-shelf geo libraries don't solve "which shards does this radius touch" |
| Auth | JWT (jsonwebtoken, rust_crypto backend) | Stateless device auth, no session store needed |
| Load testing | Custom (`tokio-tungstenite` + `hdrhistogram`) | Off-the-shelf HTTP load testers don't model sustained WebSocket connections well |
| Integration testing | `testcontainers` | Isolated, reproducible test runs independent of any long-lived local dev stack |

---

Built as a portfolio project demonstrating distributed-systems backend
engineering in Rust â€” geospatial sharding, real-time fanout,
backpressure-aware WebSocket delivery, and load-tested (with honestly
documented caveats) at scale.

**6. No data retention or device deregistration.** The `devices` table
only grows — there's no TTL, inactivity cleanup, or explicit
deregistration endpoint. Same for `alerts`/`alert_deliveries`. This is
a genuine design decision that needs a policy (how long to retain
alert history? does a device get purged after N days inactive?)
before it's implemented, not just a migration to bolt on. Flagging it
here rather than guessing at a policy nobody asked for.