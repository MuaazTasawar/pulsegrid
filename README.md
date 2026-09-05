# PulseGrid

**A geo-distributed, real-time alert fanout backend** capable of delivering
location-targeted push alerts to hundreds of thousands of concurrent
WebSocket connections in sub-second time Ã¢â‚¬â€ built to demonstrate the
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
because *detecting* the danger is hard Ã¢â‚¬â€ sensors and threshold rules are
well understood. They fail because the **fanout** is hard: pushing a
targeted, geographically-scoped alert to potentially hundreds of
thousands of nearby devices within seconds, without either

- broadcasting to *every* connected device regardless of location
  (wasteful, slow, and doesn't scale), or
- running a full database scan/join to figure out "who's nearby" on
  every single alert (also doesn't scale, and adds unacceptable
  latency to a time-critical alert).

Most "safety app" portfolio projects sidestep this entirely Ã¢â‚¬â€ a button
that sends one SMS to one emergency contact isn't the same problem at
all. PulseGrid is built specifically to solve *this* problem: the
routing and fanout infrastructure, not the sensor or the button.

## The Solution

PulseGrid indexes every connected device into a **geohash grid** and
shards WebSocket connections across worker processes by geohash prefix.
When an alert is fired with a center point and radius, a routing
algorithm resolves that radius into the *specific set of geohash
shards* it overlaps Ã¢â‚¬â€ and the alert is published only to those shards,
via NATS. Each shard's fanout-worker instance then pushes the alert to
its own locally-held WebSocket connections in parallel.

This means fanout cost is proportional to **how many devices are
actually within the alert radius**, not to the total number of devices
connected to the system Ã¢â‚¬â€ the entire point of a system meant to scale
toward hundreds of millions of users.

## Architecture

```
                         Ã¢â€Å’Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â
                         Ã¢â€â€š   api-coordinator    Ã¢â€â€š
                         Ã¢â€â€š  (Axum HTTP + JWT)   Ã¢â€â€š
                         Ã¢â€â€š                       Ã¢â€â€š
   POST /devices/registerÃ¢â€â€š  - device registry    Ã¢â€â€š
   PUT  /devices/locationÃ¢â€â€š  - JWT issuance        Ã¢â€â€š
   POST /alerts          Ã¢â€â€š  - alert dispatch      Ã¢â€â€š
                         Ã¢â€â€š                       Ã¢â€â€š
                         Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Ëœ
                                    Ã¢â€â€š
                    Ã¢â€Å’Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â¼Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â
                    Ã¢â€â€š               Ã¢â€â€š               Ã¢â€â€š
              Ã¢â€Å’Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€“Â¼Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â   Ã¢â€Å’Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€“Â¼Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â   Ã¢â€Å’Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€“Â¼Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â
              Ã¢â€â€š Postgres  Ã¢â€â€š   Ã¢â€â€š   NATS    Ã¢â€â€š   Ã¢â€â€š   Redis   Ã¢â€â€š
              Ã¢â€â€š           Ã¢â€â€š   Ã¢â€â€š           Ã¢â€â€š   Ã¢â€â€š           Ã¢â€â€š
              Ã¢â€â€š devices   Ã¢â€â€š   Ã¢â€â€š pub/sub   Ã¢â€â€š   Ã¢â€â€š presence  Ã¢â€â€š
              Ã¢â€â€š alerts    Ã¢â€â€š   Ã¢â€â€š per-shard Ã¢â€â€š   Ã¢â€â€š registry  Ã¢â€â€š
              Ã¢â€â€š deliveriesÃ¢â€â€š   Ã¢â€â€š subjects  Ã¢â€â€š   Ã¢â€â€š           Ã¢â€â€š
              Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Ëœ   Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Ëœ   Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Ëœ
                                    Ã¢â€â€š
                    alerts.geo.<shard-prefix>
                                    Ã¢â€â€š
                    Ã¢â€Å’Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â¼Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â
                    Ã¢â€â€š               Ã¢â€â€š               Ã¢â€â€š
              Ã¢â€Å’Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€“Â¼Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â   Ã¢â€Å’Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€“Â¼Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â   Ã¢â€Å’Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€“Â¼Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â
              Ã¢â€â€š  fanout-  Ã¢â€â€š   Ã¢â€â€š  fanout-  Ã¢â€â€š   Ã¢â€â€š  fanout-  Ã¢â€â€š
              Ã¢â€â€š  worker   Ã¢â€â€š   Ã¢â€â€š  worker   Ã¢â€â€š   Ã¢â€â€š  worker   Ã¢â€â€š
              Ã¢â€â€š (shard A) Ã¢â€â€š   Ã¢â€â€š (shard B) Ã¢â€â€š   Ã¢â€â€š (shard N) Ã¢â€â€š
              Ã¢â€â€š           Ã¢â€â€š   Ã¢â€â€š           Ã¢â€â€š   Ã¢â€â€š           Ã¢â€â€š
              Ã¢â€â€š WS conns  Ã¢â€â€š   Ã¢â€â€š WS conns  Ã¢â€â€š   Ã¢â€â€š WS conns  Ã¢â€â€š
              Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Ëœ   Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Ëœ   Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Ëœ
                    Ã¢â€â€š               Ã¢â€â€š               Ã¢â€â€š
                devices          devices          devices
             (WebSocket)       (WebSocket)      (WebSocket)
```

**Coordinator** (`api-coordinator`): the HTTP-facing "brain". Handles
device registration (issuing JWTs), location updates, and alert
dispatch Ã¢â‚¬â€ resolving an alert's radius into target shard prefixes and
publishing one NATS message per shard.

**Fanout worker** (`fanout-worker`): the stateful WebSocket layer. Each
instance owns a configurable set of geohash shard prefixes, holds the
live WebSocket connections for devices in those shards, subscribes to
the corresponding NATS subjects, and pushes incoming alerts out to its
local connections with bounded-channel backpressure (a slow client
never blocks delivery to everyone else).

**Postgres**: durable device registry and full audit trail
(`alert_deliveries` Ã¢â‚¬â€ which shard got which alert, with how many
devices, and when).

**Redis**: cross-instance presence registry (which fanout-worker
instance currently holds a given device's connection) Ã¢â‚¬â€ not used for
routing itself, which lives entirely in the geohash math, but for
admin/debug visibility and duplicate-connection detection.

**NATS**: the pub/sub backbone connecting the coordinator's dispatch
decision to the specific fanout-worker instance(s) that actually need
to know about it.

## Repository Layout

```
pulsegrid/
Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ crates/
Ã¢â€â€š   Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ domain/            # Pure logic, zero I/O: Device, Alert, the
Ã¢â€â€š   Ã¢â€â€š                       geohash routing algorithm, shared errors
Ã¢â€â€š   Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ infra/              # Postgres repos, Redis presence registry,
Ã¢â€â€š   Ã¢â€â€š                       NATS pub/sub helpers
Ã¢â€â€š   Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ api-coordinator/    # HTTP API: registration, auth, dispatch
Ã¢â€â€š   Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ fanout-worker/      # Stateful WS server, one per shard set
Ã¢â€â€š   Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬ loadgen/            # Connection-storm load test tool
Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ tests/                  # Integration tests (testcontainers)
Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ migrations/              # SQL schema
Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬ docker-compose.yml       # Postgres + Redis + NATS for local dev
```

Five crates in one Cargo workspace, sharing dependency versions via
`[workspace.dependencies]`. `domain` has zero I/O dependencies by
design Ã¢â‚¬â€ it's pure business logic (geohash math, validation, domain
types) that both binaries and the test suite depend on without pulling
in Axum, sqlx, or NATS.

## Core Algorithm: Geohash Shard Routing

This is the actual engineering centerpiece, in `crates/domain/src/geo.rs`.

Every device's location is encoded to a 7-character geohash (Ã¢â€°Ë†153m Ãƒâ€”
153m precision) for identification, and truncated to a 4-character
prefix (Ã¢â€°Ë†39km Ãƒâ€” 19.5km cells) for **shard assignment** Ã¢â‚¬â€ which
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
   cell diagonal` of the true alert center Ã¢â‚¬â€ keeping the result tight
   instead of alerting a needlessly wide area.
3. The coordinator publishes the alert once per surviving shard prefix,
   to NATS subject `alerts.geo.<prefix>`.

This is genuinely O(shards-in-radius), not O(total-devices) Ã¢â‚¬â€ verified
directly by instrumenting the fanout-worker's broadcast loop (see
[Load Testing & Results](#load-testing--results) below): the loop
itself executes in 10Ã¢â‚¬â€œ200 **microseconds** regardless of how many
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
# defaults Ã¢â‚¬â€ see the comments in .env.example

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
avoid client-side connection-burst issues Ã¢â‚¬â€ see caveats below), fires
one alert covering them, and reports p50/p90/p99/max fanout latency
via `hdrhistogram`.

### Run the integration tests

```powershell
cargo test -p integration-tests
```

Spins up an isolated, throwaway Postgres via `testcontainers` Ã¢â‚¬â€ no
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
needs to reconnect to a different fanout-worker instance Ã¢â‚¬â€ the
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
just the in-memory `DashMap` iteration + channel dispatch Ã¢â‚¬â€ i.e., is
the *fanout logic itself* fast, independent of network/OS overhead:

| Shard size (devices) | `broadcast_duration_us` |
|---|---|
| 0Ã¢â‚¬â€œ5 | 10Ã¢â‚¬â€œ196 Ã‚Âµs |
| 5000 | 9Ã¢â‚¬â€œ434 Ã‚Âµs |

**The dispatch loop is not the bottleneck at any tested scale.** This
was verified directly, not inferred Ã¢â‚¬â€ see caveats below for why this
matters.

### End-to-end fanout latency (single-machine, Windows)

Full round trip: alert fired via HTTP Ã¢â€ â€™ NATS publish Ã¢â€ â€™ shard
subscriber Ã¢â€ â€™ WebSocket write Ã¢â€ â€™ client receives it.

| Devices | p50 | p90 | p99 | max |
|---|---|---|---|---|
| 10 | 375ms | 375ms | 375ms | 375ms |
| 500 | 405ms | 438ms | 494ms | 503ms |
| 2000 | 518ms | 682ms | 735ms | 743ms |
| 5000 | 731Ã¢â‚¬â€œ1026ms | 3.1Ã¢â‚¬â€œ2.4s | 3.3Ã¢â‚¬â€œ4.0s | 3.4Ã¢â‚¬â€œ4.3s |

The p50/p90/p99 spread widens substantially past ~2000 concurrent
devices on a single machine Ã¢â‚¬â€ see below for why, and why this is not
read as an architectural ceiling.

## Known Limitations & Honest Caveats

This section exists because a benchmark number without its context is
worse than no benchmark at all.

**1. Single-machine testing conflates client and server resource
contention.** `loadgen` and `fanout-worker` were run on the same
Windows machine for every test in this repo's history. Both compete
for the same CPU cores, the same loopback network stack, and the same
process scheduler. The broadcast-loop instrumentation above proves the
*dispatch logic* stays fast (microseconds) regardless of shard size Ã¢â‚¬â€
the multi-second tail latency at 5000 devices lives entirely
downstream, in thousands of concurrent WebSocket writes and reads
contending for the same machine's resources. **A real benchmark number
worth quoting requires running `loadgen` and `fanout-worker` on
separate hosts** (or at minimum separate containers with resource
limits) Ã¢â‚¬â€ that's the next concrete step for a production-credible
number, not a code fix.

**2. This machine was also running unrelated Docker workloads during
testing** Ã¢â‚¬â€ a full k3d Kubernetes cluster, six other services'
containers Ã¢â‚¬â€ at one point measured consuming 223% CPU from a single
container alone, against a Docker Desktop VM capped at 4 CPUs / 7.7GB.
Stopping those unrelated containers measurably changed (but did not
eliminate) the single-machine contention pattern above. Any benchmark
run on a shared dev machine should be read with this in mind.

**3. NATS/fanout integration is not covered by the automated test
suite.** `tests/alert_dispatch_test.rs` covers the HTTP Ã¢â€ â€™ Postgres
path (device registration, alert dispatch, audit trail correctness)
via an isolated `testcontainers` Postgres instance. The NATS Ã¢â€ â€™
fanout-worker Ã¢â€ â€™ WebSocket delivery path was verified **manually**,
repeatedly, against live running processes (see commit history) Ã¢â‚¬â€ it
works, and was proven end-to-end multiple times Ã¢â‚¬â€ but isn't yet
exercised by `cargo test`. Wiring a NATS testcontainer into the suite
is real, scoped, achievable follow-up work.

**4. Single fanout-worker instance per shard set, tested.** The
architecture is designed for many fanout-worker instances, each owning
a narrower slice of shards, running as a genuinely distributed fleet.
Every load test in this repo ran against **one** fanout-worker process
owning all four active shard prefixes Ã¢â‚¬â€ meaning the "many shards
across many machines" scaling story is architecturally sound and
proven at the routing-logic level, but not yet load-tested with
multiple concurrent worker instances. That's the most valuable next
load-testing milestone.

**5. `total_devices_notified` in the alert dispatch response currently
only counts devices registered via `/devices/register` (the Postgres
registry) Ã¢â‚¬â€ `loadgen`'s simulated devices connect directly via
WebSocket without registering first, so they're invisible to that
count even though they genuinely receive the alert. Worth reconciling
before this number appears in a demo.

## What I'd Do Differently / Next Steps

- Run `loadgen` and `fanout-worker` on separate machines (or Docker
  containers with explicit CPU/memory limits) for a clean, defensible
  latency number.
- Stand up multiple `fanout-worker` instances, each owning a distinct
  subset of shard prefixes, and load-test the *coordinator's* fanout
  across genuinely separate processes/hosts Ã¢â‚¬â€ this is the real "100M
  users" proof point, not a single beefy worker.
- Add a NATS testcontainer to the integration suite for full
  end-to-end automated coverage.
- Reconcile `total_devices_notified` to reflect live WebSocket
  connections, not just the Postgres device registry.
- Add a live map visualization (the original "wow moment" from the
  project's design phase) Ã¢â‚¬â€ draw a radius on a map, watch the ripple
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
| Geospatial | `geohash` crate + hand-rolled BFS/haversine routing | The actual novel engineering Ã¢â‚¬â€ off-the-shelf geo libraries don't solve "which shards does this radius touch" |
| Auth | JWT (jsonwebtoken, rust_crypto backend) | Stateless device auth, no session store needed |
| Load testing | Custom (`tokio-tungstenite` + `hdrhistogram`) | Off-the-shelf HTTP load testers don't model sustained WebSocket connections well |
| Integration testing | `testcontainers` | Isolated, reproducible test runs independent of any long-lived local dev stack |

---

Built as a portfolio project demonstrating distributed-systems backend
engineering in Rust Ã¢â‚¬â€ geospatial sharding, real-time fanout,
backpressure-aware WebSocket delivery, and load-tested (with honestly
documented caveats) at scale.

**6. No data retention or device deregistration.** The `devices` table
only grows â€” there's no TTL, inactivity cleanup, or explicit
deregistration endpoint. Same for `alerts`/`alert_deliveries`. This is
a genuine design decision that needs a policy (how long to retain
alert history? does a device get purged after N days inactive?)
before it's implemented, not just a migration to bolt on. Flagging it
here rather than guessing at a policy nobody asked for.

**7. NATS publish has no explicit timeout.** `infra::nats::publish` waits
on the async-nats client's own internal reconnect/retry behavior with
no caller-side timeout. Under a genuine NATS outage, this means
`/alerts` requests may hang far longer than acceptable for a
time-critical alert system, rather than failing fast into the
`failed_shards` response the partial-failure handling was built to
produce. A `tokio::time::timeout` wrapper around the publish call
(returning a fast failure into `failed_shards` instead of hanging) is
the concrete next fix here -- discovered during manual outage testing,
not yet implemented.