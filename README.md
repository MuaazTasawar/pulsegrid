<div align="center">

# 🌐 PulseGrid

**A geo-distributed, real-time alert fanout backend written in Rust.**

Deliver location-targeted alerts to only the devices that need them, over WebSockets, by routing on a geohash grid instead of broadcasting to everyone.

![Rust](https://img.shields.io/badge/Rust-2021-orange?logo=rust)
![Axum](https://img.shields.io/badge/Axum-0.8-blue)
![Tokio](https://img.shields.io/badge/async-tokio-purple)
![Postgres](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-7-DC382D?logo=redis&logoColor=white)
![NATS](https://img.shields.io/badge/NATS-2-27AAE1)
![CI](https://img.shields.io/badge/CI-GitHub%20Actions-2088FF?logo=githubactions&logoColor=white)

</div>

---

## Table of Contents

1. [Overview](#1-overview)
2. [The Problem](#2-the-problem)
3. [The Solution in One Paragraph](#3-the-solution-in-one-paragraph)
4. [System Architecture](#4-system-architecture)
5. [End-to-End Flows (Sequence Diagrams)](#5-end-to-end-flows-sequence-diagrams)
6. [Repository Layout & Crate Dependency Graph](#6-repository-layout--crate-dependency-graph)
7. [Core Algorithm: Geohash Shard Routing](#7-core-algorithm-geohash-shard-routing)
8. [Component Deep Dive](#8-component-deep-dive)
9. [Data Model](#9-data-model)
10. [Redis & NATS Key / Subject Reference](#10-redis--nats-key--subject-reference)
11. [Configuration Reference](#11-configuration-reference)
12. [Getting Started](#12-getting-started)
13. [API Reference](#13-api-reference)
14. [Authentication & Security Model](#14-authentication--security-model)
15. [Reliability & Failure Behavior](#15-reliability--failure-behavior)
16. [Observability](#16-observability)
17. [Testing](#17-testing)
18. [Load Testing & Results](#18-load-testing--results)
19. [CI & Containerization](#19-ci--containerization)
20. [Scaling Out: Running Multiple Workers](#20-scaling-out-running-multiple-workers)
21. [Known Limitations & Honest Caveats](#21-known-limitations--honest-caveats)
22. [Roadmap](#22-roadmap)
23. [Tech Stack Summary](#23-tech-stack-summary)
24. [Design Decisions & Trade-offs](#24-design-decisions--trade-offs)
25. [Troubleshooting](#25-troubleshooting)

---

## 1. Overview

PulseGrid is a backend for **location-scoped, real-time alerting**: earthquake early warnings, flash-flood alerts, neighborhood safety networks, anything where a message must reach *the people inside a geographic area, fast*.

It is built as a **Cargo workspace of five crates** that compile to **three runnable programs**:

| Program | Crate | Role |
|---|---|---|
| `api-coordinator` | `crates/api-coordinator` | Stateless HTTP API. Registers devices, issues JWTs, accepts alerts, resolves them to geohash shards, publishes to NATS. |
| `fanout-worker` | `crates/fanout-worker` | Stateful WebSocket server. Owns a set of shards, holds device connections, subscribes to NATS, pushes alerts to local sockets. |
| `loadgen` | `crates/loadgen` | Connection-storm load generator that measures end-to-end fanout latency. |

and two **libraries** shared by those programs:

| Library | Role |
|---|---|
| `domain` | Pure business logic with no I/O: types, validation, JWT, geohash routing. |
| `infra` | Adapters for Postgres, Redis, and NATS. |

---

## 2. The Problem

Detecting a hazard is a solved problem: sensors and threshold rules are well understood. The hard part is the **fanout**: getting a targeted message to potentially hundreds of thousands of nearby devices in seconds.

The two naive approaches both fail:

| Naive approach | Why it fails |
|---|---|
| **Broadcast to every connected device** | Cost is O(all users) for every alert, no matter how small the affected area. Wasteful and slow. |
| **Query the DB for "who is nearby" on every alert** | A geospatial scan or join sits on the critical path of a time-critical message. Latency grows with table size. |

PulseGrid targets this specific problem, the **routing and fanout infrastructure**, rather than the sensor or the "panic button".

## 3. The Solution in One Paragraph

Every device location is encoded as a **geohash**. The first **4 characters** of that geohash (a cell of about 39 km × 19.5 km) are the device's **shard prefix**. Each fanout worker owns a set of shard prefixes and holds the WebSocket connections for devices in them. When an alert arrives with a center and radius, the coordinator computes **which shard cells the circle overlaps** (BFS over geohash neighbors, pruned by haversine distance) and publishes **one NATS message per overlapping shard** on `alerts.geo.<prefix>`. Only workers owning those shards receive it, and each pushes it to its local sockets. Work scales with **the size of the affected area**, not with the total user base.

---

## 4. System Architecture

### 4.1 High-level component diagram

```mermaid
flowchart TB
    subgraph Clients["Clients"]
        D1["Device A<br/>mobile / IoT"]
        D2["Device B"]
        D3["Device N"]
        OP["Alert Operator<br/>or upstream sensor system"]
    end

    subgraph Control["Control Plane (stateless, horizontally scalable)"]
        API["api-coordinator<br/>Axum HTTP :8090<br/>- device registration<br/>- JWT issuing / verification<br/>- location updates<br/>- alert dispatch<br/>- per-IP rate limiting"]
    end

    subgraph Data["Shared Infrastructure"]
        PG[("PostgreSQL<br/>devices<br/>alerts<br/>alert_deliveries")]
        NATS{{"NATS<br/>subjects:<br/>alerts.geo.PREFIX"}}
        REDIS[("Redis<br/>presence:DEVICE_ID<br/>shard-worker:PREFIX")]
    end

    subgraph Fanout["Fanout Plane (stateful, sharded by geohash prefix)"]
        W1["fanout-worker 1<br/>owns tkrt, tkrw<br/>ConnectionRegistry<br/>DashMap + bounded mpsc"]
        W2["fanout-worker 2<br/>owns tkrs, tkrv"]
        WN["fanout-worker N<br/>owns other prefixes"]
    end

    D1 -- "POST /devices/register" --> API
    D2 -- "PUT /devices/location" --> API
    OP -- "POST /alerts" --> API

    API -- "insert / count" --> PG
    API -- "lookup shard owner" --> REDIS
    API -- "publish per shard" --> NATS

    W1 -- "register shards<br/>presence + heartbeat" --> REDIS
    W2 --> REDIS
    WN --> REDIS

    NATS -- "alerts.geo.tkrt / tkrw" --> W1
    NATS -- "alerts.geo.tkrs / tkrv" --> W2
    NATS -- "other subjects" --> WN

    D1 <== "WebSocket<br/>/connect/DEVICE_ID?token=JWT" ==> W1
    D2 <== "WebSocket" ==> W2
    D3 <== "WebSocket" ==> WN
```

### 4.2 Architecture in plain ASCII (for terminals and offline viewing)

```
                              +--------------------------+
   register / location  ----> |      api-coordinator     | <----  POST /alerts
                              |  Axum + JWT + governor   |
                              +----+----------+------+---+
                                   |          |      |
                          devices, |          |      | lookup_shard
                          alerts,  |          |      | (ws_url for client)
                          deliveries          |      |
                              +----v---+  +---v----+ +v--------+
                              |Postgres|  |  NATS  | |  Redis  |
                              +--------+  +---+----+ +----^----+
                                              |           |
                       alerts.geo.<prefix>    |           | register_shard
                       one message per shard  |           | presence + heartbeat
                         +--------------------+---+-------+-------------+
                         |                        |                     |
                   +-----v------+          +------v-----+        +------v-----+
                   |fanout-worker|         |fanout-worker|       |fanout-worker|
                   | tkrt, tkrw  |         | tkrs, tkrv  |       |    ...      |
                   | DashMap<id, |         |             |       |             |
                   |  mpsc(64)>  |         |             |       |             |
                   +-----+------+          +------+------+       +------+------+
                         | WebSocket              | WebSocket           | WebSocket
                      devices                  devices               devices
```

### 4.3 Plane separation

| Plane | State | Scaling model | Failure impact |
|---|---|---|---|
| **Control** (`api-coordinator`) | Stateless; all state in Postgres/Redis/NATS | Add replicas behind any load balancer | A replica dying loses nothing |
| **Fanout** (`fanout-worker`) | Stateful: live sockets in process memory | Partition by shard prefix; add workers and assign prefixes | A worker dying drops its sockets until clients reconnect |
| **Data** (Postgres, Redis, NATS) | Durable (Postgres) or ephemeral (Redis, NATS) | Standard scaling for each | See [§15](#15-reliability--failure-behavior) |

---

## 5. End-to-End Flows (Sequence Diagrams)

### 5.1 Device registration

```mermaid
sequenceDiagram
    autonumber
    participant C as Client device
    participant API as api-coordinator
    participant PG as PostgreSQL
    participant R as Redis

    C->>API: POST /devices/register {lat, lon}
    Note over API: per-IP rate limit (5/s, burst 10)
    API->>API: validate range, Location::new
    API->>API: Device::register -> geohash(7), shard_prefix(4)
    API->>PG: INSERT INTO devices
    API->>API: issue_token(device_id, JWT_SECRET), 30 day exp
    API->>R: GET shard-worker:PREFIX
    R-->>API: ws://worker-host:8081 or nil
    API-->>C: 200 {device_id, token, shard_prefix, ws_url}
```

### 5.2 Opening the WebSocket

```mermaid
sequenceDiagram
    autonumber
    participant C as Client device
    participant W as fanout-worker
    participant R as Redis

    C->>W: GET /connect/DEVICE_ID?shard_prefix=P&token=JWT (Upgrade)
    W->>W: verify_token(token) -> device_id from sub
    alt token invalid or expired
        W-->>C: 401 Unauthorized
    else token sub != path device_id
        W-->>C: 403 Forbidden
    else worker does not own P
        W-->>C: 421 Misdirected Request
    else accepted
        W-->>C: 101 Switching Protocols
        W->>W: ConnectionRegistry.register -> mpsc::channel(64)
        W->>R: SET presence:DEVICE_ID = instance_id (TTL 60s)
        loop every 20s while connected
            W->>R: EXPIRE presence:DEVICE_ID 60
        end
    end
```

### 5.3 Alert dispatch and fanout (the main event)

```mermaid
sequenceDiagram
    autonumber
    participant Op as Operator
    participant API as api-coordinator
    participant PG as PostgreSQL
    participant N as NATS
    participant W1 as fanout-worker (owns shard A)
    participant W2 as fanout-worker (owns shard B)
    participant D as Devices in shard A and B

    Op->>API: POST /alerts {lat, lon, radius_meters, severity, title, message}
    Note over API: per-IP rate limit (2/s, burst 5)
    API->>API: validate, Alert::new
    API->>PG: INSERT INTO alerts
    API->>API: shard_prefixes_for_radius(center, radius) -> [A, B, ...]
    loop for each target prefix (independent, failures isolated)
        API->>N: publish alerts.geo.PREFIX (3s timeout incl. flush)
        alt publish OK
            API->>PG: SELECT COUNT(*) FROM devices WHERE shard_prefix = PREFIX
            API->>PG: INSERT INTO alert_deliveries
        else publish failed or timed out
            API->>API: push PREFIX into failed_shards
        end
    end
    API-->>Op: 200 (all ok) or 207 (some failed) + per_shard + failed_shards
    N-->>W1: alerts.geo.A
    N-->>W2: alerts.geo.B
    W1->>W1: broadcast_to_shard(A): try_send to each local connection
    W2->>W2: broadcast_to_shard(B)
    W1-->>D: WebSocket binary frame (JSON alert)
    W2-->>D: WebSocket binary frame (JSON alert)
```

### 5.4 Location update and shard migration

```mermaid
sequenceDiagram
    autonumber
    participant C as Client device
    participant API as api-coordinator
    participant PG as PostgreSQL
    participant R as Redis

    C->>API: PUT /devices/location (Authorization: Bearer JWT) {lat, lon}
    API->>API: AuthDevice extractor -> device_id
    API->>PG: SELECT device
    API->>API: compare new shard_prefix with old
    API->>PG: UPDATE devices SET lat, lon, geohash, shard_prefix, last_seen
    alt shard changed
        API->>R: GET shard-worker:NEW_PREFIX
        API-->>C: {shard_prefix, shard_changed: true, ws_url: new worker}
        Note over C: client must reconnect its WebSocket to ws_url
    else same shard
        API-->>C: {shard_prefix, shard_changed: false, ws_url: null}
    end
```

### 5.5 WebSocket connection lifecycle (worker side)

```mermaid
stateDiagram-v2
    [*] --> Upgrading: GET /connect/{id}
    Upgrading --> Rejected401: bad or expired token
    Upgrading --> Rejected403: token sub != path id
    Upgrading --> Rejected421: shard not owned here
    Upgrading --> Connected: 101 Switching Protocols
    Connected --> Connected: alert in channel -> send Binary frame
    Connected --> Connected: heartbeat every 20s refreshes Redis TTL
    Connected --> Closing: client closes or socket error
    Closing --> [*]: abort tasks, deregister local and Redis presence
    Rejected401 --> [*]
    Rejected403 --> [*]
    Rejected421 --> [*]
```

---

## 6. Repository Layout & Crate Dependency Graph

```
pulsegrid/
├── Cargo.toml                    # workspace root, shared [workspace.dependencies]
├── Cargo.lock
├── Dockerfile                    # multi-stage build -> both service binaries
├── docker-compose.yml            # postgres + redis + nats (+ the two services)
├── .env.example                  # host-side (cargo run) configuration template
├── migrations/
│   └── 0001_init.sql             # devices, alerts, alert_deliveries
├── .github/workflows/ci.yml      # build + unit + integration tests
├── register_test.json            # sample body for POST /devices/register
├── alert_test.json               # sample body for POST /alerts
├── failure_test.json             # sample body used for NATS-outage testing
├── tests/                        # integration-tests crate (testcontainers)
│   ├── Cargo.toml
│   └── alert_dispatch_test.rs
└── crates/
    ├── domain/                   # pure logic, no I/O frameworks
    │   └── src/{alert,auth,device,errors,geo,lib}.rs
    ├── infra/                    # Postgres / Redis / NATS adapters
    │   └── src/{db,redis,nats,lib}.rs
    ├── api-coordinator/          # HTTP control plane
    │   └── src/
    │       ├── main.rs, config.rs, errors.rs
    │       ├── routes/{devices,alerts}.rs      # routing + rate limiting
    │       ├── handlers/{devices,alerts}.rs    # request/response + validation
    │       ├── services/{device_service,alert_dispatch_service}.rs
    │       ├── extractors/auth_device.rs       # Bearer JWT extractor
    │       └── middleware/request_id.rs
    ├── fanout-worker/            # stateful WebSocket plane
    │   └── src/{main,config,ws,subscriber,connection_registry}.rs
    └── loadgen/                  # load generator
        └── src/main.rs
```

### 6.1 Crate dependency graph

```mermaid
flowchart LR
    domain["domain<br/>(pure logic)"]
    infra["infra<br/>(Postgres, Redis, NATS)"]
    api["api-coordinator<br/>(bin)"]
    worker["fanout-worker<br/>(bin)"]
    loadgen["loadgen<br/>(bin)"]
    tests["integration-tests"]

    infra --> domain
    api --> domain
    api --> infra
    worker --> domain
    worker --> infra
    loadgen --> domain
    tests --> domain
    tests --> infra
```

**Why `domain` is I/O-free:** both binaries and the test suite depend on it without pulling in Axum, sqlx, or NATS. JWT issue/verify lives there on purpose, so the coordinator and the worker can never disagree about token shape or validation. The NATS subject naming (`Alert::nats_subject_for_prefix`) lives there for the same reason: publisher and subscriber cannot drift.

### 6.2 Layering inside `api-coordinator`

```mermaid
flowchart LR
    R["routes<br/>path + rate limit"] --> H["handlers<br/>JSON in/out + validator"]
    H --> S["services<br/>business flow"]
    S --> I["infra repos<br/>SQL / NATS / Redis"]
    H --> E["extractors<br/>AuthDevice (JWT)"]
    H --> ER["errors<br/>AppError -> HTTP status"]
```

---

## 7. Core Algorithm: Geohash Shard Routing

Implemented in [`crates/domain/src/geo.rs`](crates/domain/src/geo.rs). This is the heart of the project.

### 7.1 Two precisions, two purposes

| Constant | Value | Approx. cell size | Purpose |
|---|---|---|---|
| `GEOHASH_PRECISION` | 7 chars | ~153 m × 153 m | Fine-grained device location identity (stored in `devices.geohash`) |
| `SHARD_PREFIX_LENGTH` | 4 chars | ~39 km × 19.5 km | Coarse **shard key**: decides which worker owns a device |

A device's shard prefix is simply the first 4 characters of its 7-character geohash.

### 7.2 Algorithm

Given `center` and `radius_meters` (valid range `(0, 500000]`):

1. **Encode** the center to a 4-char geohash cell.
2. **Estimate cell size** in meters at that latitude: `min(width, height)` of the decoded cell, where width shrinks with `cos(latitude)`.
3. **Compute rings needed**: `rings = clamp(ceil(radius / cell_size), 1, 40)`. The cap of 40 (`MAX_RING_EXPANSION`) guarantees the loop can never run unbounded.
4. **BFS ring expansion**: starting from the center cell, repeat `rings` times: for every cell in the current frontier, add its 8 neighbors (N, NE, E, SE, S, SW, W, NW) that have not been visited. Newly discovered cells become the next frontier.
5. **Haversine prune**: grid expansion over-covers (cells are squares, alerts are circles). Keep a cell only if the great-circle distance from the alert center to the **cell's center** is `<= radius + cell_size * √2` (one cell diagonal of slack, so cells that merely clip the circle are kept).
6. **Return** the surviving prefixes. The coordinator publishes once to `alerts.geo.<prefix>` for each.

```mermaid
flowchart TD
    A["Alert center + radius"] --> B["Validate: 0 < radius <= 500 km"]
    B --> C["Encode center to 4-char geohash"]
    C --> D["Decode cell: estimate cell size in meters"]
    D --> E["rings = clamp(ceil(radius / cell), 1, 40)"]
    E --> F["frontier = center cell, visited = center cell"]
    F --> G{"ring < rings?"}
    G -- yes --> H["For each cell in frontier add 8 unvisited neighbors to next_frontier"]
    H --> I{"next_frontier empty?"}
    I -- yes --> J
    I -- no --> K["frontier = next_frontier"]
    K --> G
    G -- no --> J["Haversine prune: keep cell if dist(center, cell_center) <= radius + cell*sqrt2"]
    J --> L["Target shard prefixes"]
    L --> M["Publish alerts.geo.PREFIX per prefix"]
```

### 7.3 Visualizing the grid

A 5 km alert in Karachi is far smaller than one 39 km × 19.5 km shard cell, so `rings = 1` and the BFS yields the 3×3 neighborhood (9 cells). Haversine pruning then drops the cells whose centers are too far away:

```
        before prune (9 cells)              after prune (typical)

      +-----+-----+-----+                 +-----+-----+-----+
      | nw  |  n  | ne  |                 |     |  n  |     |
      +-----+-----+-----+                 +-----+-----+-----+
      |  w  |  C  |  e  |      ----->     |  w  |  C  |  e  |
      +-----+-----+-----+                 +-----+-----+-----+
      | sw  |  s  | se  |                 |     |  s  |     |
      +-----+-----+-----+                 +-----+-----+-----+
                      C = cell containing alert center
```

(The exact survivors depend on where in the cell the center falls. The sample response in [§13.3](#133-post-alerts) shows 4 shards targeted for Karachi.)

### 7.4 Complexity

Routing cost depends on **rings**, which depends on **radius / cell size**, not on the number of devices. Rings are bounded at 40, so at most `(2·40+1)² = 6561` candidate cells. In practice it is a handful.

### 7.5 What this does and does not guarantee

- ✅ **Coverage**: every shard cell whose center lies within `radius + 1 cell diagonal` is targeted, so a device inside the radius lands in a targeted shard. This is asserted in the integration test.
- ⚠️ **Precision is shard-level, not circle-level.** Workers broadcast to **all connections in a shard**; they do not re-check each device's distance to the alert center. Devices in a targeted shard but outside the exact radius will also receive the alert. See [§21](#21-known-limitations--honest-caveats).

---

## 8. Component Deep Dive

### 8.1 `domain`: pure logic

| Module | Contents |
|---|---|
| `device.rs` | `Location` (validated lat/lon), `DeviceId` (UUID newtype), `Device` with `register()` and `update_location()` (the latter returns whether the shard changed) |
| `alert.rs` | `AlertId`, `AlertSeverity {Info, Warning, Critical}`, `Alert::new` (validates radius and non-empty title), `target_shard_prefixes()`, `nats_subject_for_prefix()` |
| `geo.rs` | Encoding, shard prefix, BFS ring expansion, haversine, cell size estimation |
| `auth.rs` | `issue_token` / `verify_token` (HS256 via `jsonwebtoken` with `rust_crypto`), 30-day TTL, `sub` = device UUID |
| `errors.rs` | `AppError` plus `is_client_error()` so edge crates map to 4xx vs 5xx without duplicating logic |

### 8.2 `infra`: adapters

| Module | Contents |
|---|---|
| `db.rs` | `connect` (pool of up to 20), `run_migrations` (embedded from `migrations/`), `DeviceRepository` (`insert`, `update_location`, `find_by_id`, `count_in_shard`), `AlertRepository` (`insert_alert`, `record_delivery`) |
| `redis.rs` | `PresenceRegistry` (`register`, `heartbeat`, `lookup`, `deregister`, TTL 60 s) and `ShardRegistry` (`register_shard`, `lookup_shard`), both on a `deadpool-redis` pool |
| `nats.rs` | `connect`, `publish` (publish **and flush**, bounded by a **3 s timeout**), `subscribe_and_handle` |

### 8.3 `api-coordinator`: HTTP control plane

- **Routing & rate limits.** `/devices/register` is limited to 5 req/s (burst 10) per client IP; `/alerts` to 2 req/s (burst 5) per IP, both via `tower_governor`. `/devices/location` is not IP-limited (JWT-gated).
- **Validation.** Request structs use `validator` (lat/lon ranges, radius `1..=500000`, title `1..=200` chars, message `1..=2000` chars), and the domain layer re-validates.
- **Error mapping.** `ApiError` wraps `AppError`: `DeviceNotFound` → 404, `Unauthorized` → 401, other client errors → 400, everything else → 500 with a generic body (details are logged, not leaked).
- **Request IDs.** Middleware reads or generates `x-request-id` and echoes it on the response.
- **Graceful shutdown** on Ctrl+C / SIGTERM; the server is created with `ConnectInfo<SocketAddr>` so the rate limiter can see client IPs.
- **Startup order.** load config → connect Postgres → **run migrations** → connect NATS → build Redis pool → serve.

#### Dispatch behavior (`AlertDispatchService::dispatch`)

1. Validate and build the `Alert`; **persist it first** (`alerts` row).
2. Resolve target shard prefixes.
3. Serialize the alert **once** to JSON.
4. For each prefix, independently: publish → on success count devices in shard and write an `alert_deliveries` row; on failure add to `failed_shards`. One shard failing never stops the others.
5. Return `200` when `failed_shards` is empty, else **`207 Multi-Status`**.

### 8.4 `fanout-worker`: the stateful WebSocket plane

**Startup:** load config → connect Redis and NATS → advertise owned shards in `ShardRegistry` (using `WORKER_PUBLIC_URL`, default `ws://localhost:<port>`) → spawn **one NATS subscriber task per owned prefix** → serve HTTP/WS.

**`ConnectionRegistry`:** `Arc<DashMap<Uuid, ConnectionHandle>>`, where each handle holds the device's shard prefix and the sender half of a **bounded `mpsc` channel (capacity 64)**.

**Backpressure design.** `broadcast_to_shard` uses **`try_send`**, never `send().await`. The broadcast loop runs inside the shared subscriber task, so awaiting one slow client would stall every other device. A full channel means *that one device drops this alert*, and the loop moves on. The return value `(delivered, dropped)` is logged per alert.

**Per-connection tasks** (spawned in `handle_socket`):

| Task | Job |
|---|---|
| **Forwarder** | Reads from the connection's `mpsc` receiver and writes `Message::Binary` frames to the socket. Exits on write error. |
| **Heartbeat** | Every 20 s refreshes the Redis presence TTL. |
| **Reader (main)** | Drains inbound frames; exits on error/close, then aborts the other tasks and deregisters from the local registry and Redis. |

**What the client receives:** a binary WebSocket message whose bytes are the JSON-serialized `Alert`:

```json
{
  "id": "29be0c7d-3a52-4b0e-9d1e-7f1f0c6b9a11",
  "center": { "lat": 24.8607, "lon": 67.0011 },
  "radius_meters": 5000.0,
  "severity": "Critical",
  "title": "Earthquake Warning",
  "message": "Magnitude 5.2 detected nearby",
  "created_at": "2026-10-21T10:15:30.123456Z"
}
```

> Note the asymmetry: the **request** takes lowercase severity (`"critical"`), while the **pushed payload** uses the serde default (`"Critical"`).

### 8.5 `loadgen`: the load generator

Spawns N simulated devices that each: pick a random point within a scatter radius (uniform over the disc, using `sqrt` sampling), **register for a real JWT**, open an authenticated WebSocket, and wait for one binary frame. Devices launch in **batches of 50 with a 100 ms stagger** to avoid client-side connection bursts. After connecting (or timing out), it fires one alert and records per-device latency into an **HdrHistogram**, reporting p50 / p90 / p99 / max plus counts of register failures, connect failures, deliveries, and delivery timeouts.

---

## 9. Data Model

```mermaid
erDiagram
    DEVICES {
        uuid id PK
        double lat
        double lon
        varchar12 geohash "7-char full precision"
        varchar8 shard_prefix "4-char shard key (indexed)"
        timestamptz registered_at
        timestamptz last_seen
    }
    ALERTS {
        uuid id PK
        double center_lat
        double center_lon
        double radius_meters
        varchar16 severity "info / warning / critical"
        text title
        text message
        timestamptz created_at
    }
    ALERT_DELIVERIES {
        uuid id PK
        uuid alert_id FK
        varchar8 shard_prefix
        timestamptz published_at
        int devices_in_shard
    }
    ALERTS ||--o{ ALERT_DELIVERIES : "published to"
```

**Indexes:** `devices(shard_prefix)`, `devices(geohash)`, `alert_deliveries(alert_id)`.

**`alert_deliveries` is the audit trail.** One row per shard an alert was *successfully published to*, with the device count at publish time. A statement like "this alert hit N shards covering M devices" should be computed by aggregating this table, not by trusting in-memory state. Shards that failed to publish get **no** row.

Migrations live in `migrations/` and are embedded into the coordinator binary (`sqlx::migrate!`) and applied automatically on startup.

---

## 10. Redis & NATS Key / Subject Reference

| System | Name | Written by | Value / payload | TTL | Purpose |
|---|---|---|---|---|---|
| Redis | `presence:<device_id>` | fanout-worker | worker `instance_id` (random UUID per process start) | 60 s, refreshed every 20 s | Which worker holds this device's socket. Powers `/admin/presence/{id}`. |
| Redis | `shard-worker:<prefix>` | fanout-worker (at startup) | worker public WS URL | none | Service discovery: tells the coordinator which URL a client should connect to for a shard. |
| NATS | `alerts.geo.<prefix>` | api-coordinator | JSON `Alert` | n/a (core NATS, not persisted) | One subject per shard. Workers subscribe to exactly the prefixes they own. |

---

## 11. Configuration Reference

All configuration is via environment variables (a `.env` file is loaded with `dotenvy` if present; real environment variables win over `.env`).

| Variable | Used by | Required | Default | Description |
|---|---|---|---|---|
| `DATABASE_URL` | coordinator | ✅ | none | Postgres connection string. The worker does **not** use Postgres. |
| `REDIS_URL` | both | ✅ | none | Redis URL (shard registry, presence). |
| `NATS_URL` | both | ✅ | none | NATS URL. |
| `JWT_SECRET` | both | ✅ | none | HMAC secret. **Must be identical** for coordinator and every worker. Change it from the example value. |
| `COORDINATOR_PORT` | coordinator | ❌ | `8080` | HTTP listen port (the example `.env` sets `8090`). |
| `FANOUT_WORKER_PORT` | worker | ❌ | `8081` | HTTP/WS listen port. |
| `FANOUT_WORKER_SHARD_PREFIXES` | worker | ✅ | none | Comma-separated list of 4-char geohash prefixes this instance owns. Must list at least one. |
| `WORKER_PUBLIC_URL` | worker | ❌ | `ws://localhost:<port>` | URL advertised to clients via the shard registry. Set this in any real deployment (load balancer / public hostname). Not in `.env.example`. |
| `RUST_LOG` | both | ❌ | none | `tracing` filter, e.g. `info,api_coordinator=debug,fanout_worker=debug`. |

**Default local ports** (from `docker-compose.yml` / `.env.example`):

| Service | Host port | Notes |
|---|---|---|
| PostgreSQL | `5435` → 5432 | Remapped to avoid clashing with a local Postgres |
| Redis | `6382` → 6379 | Remapped for the same reason |
| NATS | `4222` | Default client port |
| api-coordinator | `8090` | |
| fanout-worker | `8081` | |

---

## 12. Getting Started

### 12.1 Prerequisites

- **Rust** (stable toolchain), via [rustup](https://rustup.rs)
- **Docker** with Compose (infrastructure, and for `testcontainers`)
- A C toolchain / OpenSSL headers on Linux if you hit build errors (the Dockerfile installs `pkg-config` and `libssl-dev`)

### 12.2 Run locally

<details open>
<summary><b>Linux / macOS (bash)</b></summary>

```bash
git clone https://github.com/MuaazTasawar/pulsegrid.git
cd pulsegrid
cp .env.example .env

# 1. Start infrastructure only
docker compose up -d postgres redis nats

# 2. Wait for Postgres
docker compose exec postgres sh -c 'until pg_isready -U pulsegrid; do sleep 1; done'

# 3. Terminal 1: the coordinator (runs migrations on boot)
cargo run -p api-coordinator

# 4. Terminal 2: the fanout worker
cargo run -p fanout-worker
```
</details>

<details>
<summary><b>Windows (PowerShell)</b></summary>

```powershell
git clone https://github.com/MuaazTasawar/pulsegrid.git
cd pulsegrid
copy .env.example .env

docker-compose up -d postgres redis nats
docker exec pulsegrid-postgres-1 sh -c "until pg_isready -U pulsegrid; do sleep 1; done"

cargo run -p api-coordinator   # terminal 1
cargo run -p fanout-worker     # terminal 2
```
</details>

**Start order matters slightly:** start the **worker first** if you want `ws_url` to be populated in the registration response, because the worker writes its shard ownership to Redis at startup. If the coordinator registers a device before any worker has claimed that shard, `ws_url` is `null`.

### 12.3 Smoke test, the full path by hand

**1. Register a device** (Karachi) and note the `device_id`, `token`, and `shard_prefix`:

```bash
curl -s -X POST http://localhost:8090/devices/register \
  -H "Content-Type: application/json" \
  -d @register_test.json
# {"device_id":"c8b0...","token":"eyJ0...","shard_prefix":"tkrt","ws_url":"ws://localhost:8081"}
```

**2. Open a WebSocket** with any WS client (e.g. [`websocat`](https://github.com/vi/websocat)):

```bash
websocat "ws://localhost:8081/connect/<device_id>?shard_prefix=tkrt&token=<token>"
```

**3. Fire an alert** from another terminal:

```bash
curl -s -X POST http://localhost:8090/alerts \
  -H "Content-Type: application/json" \
  -d @alert_test.json
```

You should see the JSON alert arrive on the WebSocket within a few hundred milliseconds, and the worker log a line like:

```
INFO fanout_worker::subscriber: alert fanned out to shard shard=tkrt delivered=1 dropped=0 broadcast_duration_us=42
```

**4. Check presence** (while the socket is open):

```bash
curl -i http://localhost:8081/admin/presence/<device_id>
```

### 12.4 Run everything in Docker

`docker-compose.yml` also defines `api-coordinator` and `fanout-worker` services built from the `Dockerfile`. They read `env_file: .env`, but the default `.env` points at `localhost`, which is wrong **inside** containers. Use Docker service names and *container-internal* ports:

```dotenv
# .env.docker (suggested; point env_file at it for the two app services)
DATABASE_URL=postgres://pulsegrid:pulsegrid@postgres:5432/pulsegrid
REDIS_URL=redis://redis:6379
NATS_URL=nats://nats:4222
JWT_SECRET=<a-long-random-secret>
COORDINATOR_PORT=8090
FANOUT_WORKER_PORT=8081
FANOUT_WORKER_SHARD_PREFIXES=tkrt,tkrw,tkrs,tkrv
WORKER_PUBLIC_URL=ws://localhost:8081
```

This split is not yet automated in the repo (see [§21 item 2](#21-known-limitations--honest-caveats)).

### 12.5 Run the load test

```bash
cargo run --release -p loadgen -- --devices 1000
```

| Flag | Default | Meaning |
|---|---|---|
| `--devices` | `1000` | Number of simulated devices |
| `--center-lat` / `--center-lon` | `24.8607` / `67.0011` | Center of the device scatter and of the alert |
| `--scatter-radius-meters` | `3000` | Devices are placed uniformly within this radius |
| `--alert-radius-meters` | `5000` | Radius of the fired alert |
| `--fanout-worker-url` | `ws://localhost:8081` | Where to open WebSockets |
| `--coordinator-url` | `http://localhost:8090` | Where to register and dispatch |
| `--connect-timeout-secs` | `30` | Max wait for all devices to connect |
| `--delivery-timeout-secs` | `20` | Max wait for each device to receive the alert |

Use `--release`; debug builds distort latency numbers. Note the registration rate limit (5/s, burst 10 per IP) applies to loadgen too, since every simulated device calls `/devices/register`. For large runs, raise the limit or run against a build with it relaxed.

### 12.6 Run the tests

```bash
cargo test -p domain              # unit tests (no Docker needed)
cargo test -p integration-tests   # needs Docker (spins up a throwaway Postgres)
cargo test --workspace            # everything
```

---

## 13. API Reference

Base URLs (defaults): coordinator `http://localhost:8090`, worker `http://localhost:8081`.
All JSON errors from the coordinator have the shape `{"error": "<message>"}`.

### 13.1 `POST /devices/register`

Registers a device at a location and issues its JWT. **Rate limit:** 5 req/s, burst 10, per client IP.

```jsonc
// Request
{ "lat": 24.8607, "lon": 67.0011 }

// Response 200
{
  "device_id": "c8b00808-8462-459f-ac73-f59dbbe28f32",
  "token": "eyJ0eXAi...",
  "shard_prefix": "tkrt",
  "ws_url": "ws://localhost:8081"      // null if no worker has claimed this shard
}
```

| Status | When |
|---|---|
| `200` | Registered |
| `400` | `lat` ∉ [-90, 90] or `lon` ∉ [-180, 180] |
| `429` | Rate limit exceeded |
| `500` | Database or other internal failure |

### 13.2 `PUT /devices/location`

Updates the authenticated device's location. **Requires** `Authorization: Bearer <token>`.

```jsonc
// Request
{ "lat": 24.9, "lon": 67.1 }

// Response 200
{ "shard_prefix": "tkrt", "shard_changed": false, "ws_url": null }
```

If `shard_changed` is `true`, the device now belongs to a different shard and **must reconnect its WebSocket** to the returned `ws_url` (which may be `null` if no worker has claimed the new shard). The server does not close or migrate the existing socket by itself.

| Status | When |
|---|---|
| `200` | Updated |
| `400` | Invalid coordinates |
| `401` | Missing, invalid, or expired token |
| `404` | Device id in token not found |

### 13.3 `POST /alerts`

Dispatches an alert to every shard overlapping the circle. **Rate limit:** 2 req/s, burst 5, per client IP.

```jsonc
// Request
{
  "lat": 24.8607, "lon": 67.0011,
  "radius_meters": 5000,                 // 1 .. 500000
  "severity": "critical",                // "info" | "warning" | "critical"
  "title": "Earthquake Warning",         // 1..200 chars
  "message": "Magnitude 5.2 detected nearby"   // 1..2000 chars
}

// Response 200 (all shards published) or 207 (some failed)
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

| Status | When |
|---|---|
| `200` | Every targeted shard was published |
| `207` | Partial: one or more shards listed in `failed_shards` were **not** reached |
| `400` | Validation error |
| `429` | Rate limit exceeded |

> **Reading `total_devices_notified`:** it is the sum of **registered devices** (Postgres rows) in successfully published shards. It is a database count, **not** a count of live sockets that actually received the message. The worker log (`delivered` / `dropped`) is the source of truth for live delivery.

> ⚠️ This endpoint currently has **no authentication**, only IP rate limiting. See [§14](#14-authentication--security-model).

### 13.4 `GET /connect/{device_id}?shard_prefix=<prefix>&token=<jwt>` (WebSocket, worker)

Upgrades to a WebSocket. The JWT travels in the query string because browsers and many clients cannot set an `Authorization` header on a WebSocket handshake.

| Status | When |
|---|---|
| `101` | Upgrade accepted |
| `401` | Token missing, malformed, wrongly signed, or expired |
| `403` | Token is valid but its `sub` differs from `{device_id}` (anti-impersonation) |
| `421` | This worker does not own `shard_prefix` (client was misrouted) |

After the upgrade the server **only sends**: each alert arrives as one **binary** frame containing UTF-8 JSON. Inbound frames are read and discarded (only used to detect disconnects).

### 13.5 Operational endpoints (both binaries)

| Endpoint | Response |
|---|---|
| `GET /health` | `ok` (liveness) |
| `GET /ready` | `ready` (currently static; does not check dependencies) |
| `GET /metrics` | Prometheus text format, see [§16](#16-observability) |

### 13.6 `GET /admin/presence/{device_id}` (worker)

Returns the `instance_id` of the worker holding that device's socket (`200`, plain text), or `404` `no presence record` if it is not connected or its heartbeat expired. `500` if Redis fails. **Unauthenticated**; do not expose publicly.

---

## 14. Authentication & Security Model

### 14.1 What exists today

| Control | Detail |
|---|---|
| **Device JWT** | HS256, `sub` = device UUID, 30-day expiry, issued at registration |
| **REST auth** | `PUT /devices/location` requires `Authorization: Bearer <jwt>` |
| **WebSocket auth** | `/connect` requires the JWT, and `sub` **must equal** the path `device_id`. A valid token for device A cannot connect as device B. |
| **Shard ownership check** | A worker rejects (`421`) connections for prefixes it does not own, so a misrouted client fails fast instead of silently missing alerts |
| **Rate limiting** | Per-IP token bucket on registration and alert dispatch |
| **Input validation** | Range checks, length limits, enum-typed severity |
| **Error hygiene** | 5xx responses return a generic message; details go to logs only |
| **Shared token logic** | One implementation in `domain`, so issuer and verifier cannot diverge |

### 14.2 Security gaps to be aware of

These are real and worth fixing before any production use:

1. **`POST /alerts` is unauthenticated.** Anyone who can reach the coordinator can fire alerts, limited only by IP rate limits. Production needs operator authentication (API keys, mTLS, or a separate privileged JWT audience).
2. **`/admin/presence/{id}` is unauthenticated** and should be network-restricted or authenticated.
3. **Registration is open** (rate-limited only) and `JWT_SECRET` is a symmetric secret shared across all services; compromise of any service compromises token issuance. Consider asymmetric signing (the coordinator signs, workers only verify).
4. **The worker trusts the client-claimed `shard_prefix`.** It checks the worker owns that prefix, not that the device *actually belongs* to it. A client with a valid token could claim a different owned prefix and receive that shard's alerts.
5. **Tokens in the query string** can appear in proxy/access logs. Redact them, or move to a short-lived one-time WS ticket.
6. **No token revocation**; a leaked 30-day token is valid until it expires.
7. **Plain `ws://` / `http://`** in the defaults. Terminate TLS in front of both services in any real deployment.
8. **Default secret** `change-me-in-production` is in `.env.example`.

---

## 15. Reliability & Failure Behavior

| Scenario | Behavior |
|---|---|
| **NATS unreachable during dispatch** | Each shard publish is bounded by a **3 s timeout** (publish + flush). Failed shards are collected into `failed_shards`; the API returns **`207`** rather than hanging or aborting. Verified by stopping the NATS container mid-request. |
| **One shard fails, others succeed** | Shards are attempted independently, so the others are still delivered. |
| **Device-count query fails after a successful publish** | Logged; the shard is recorded with count `0`. The alert is **not** treated as failed. |
| **Audit write fails after a successful publish** | Logged as an error; delivery is not rolled back. |
| **Slow or stalled WebSocket client** | Its 64-slot channel fills; further alerts to **that device** are dropped (`dropped` counter). Other devices are unaffected. |
| **Redis down at worker connect time** | Presence registration failure is logged as a warning and the connection **still proceeds** (presence is for visibility, not correctness). |
| **Redis down at coordinator registration** | The `ws_url` lookup error is swallowed and `ws_url` is `null`; registration still succeeds. |
| **Worker crash / restart** | All its sockets drop; in-memory registry is rebuilt from nothing as clients reconnect. Presence keys expire within 60 s. |
| **Client disconnects** | Heartbeat and forwarder tasks are aborted; the connection is removed from the local registry and Redis. |
| **Graceful shutdown** | Both binaries handle Ctrl+C and SIGTERM and stop accepting new connections cleanly. |

**Delivery semantics:** NATS core pub/sub is **at-most-once** and does not persist. A device that is offline, mid-reconnect, or whose channel is full **will not** receive the alert later. See the roadmap for durable options.

---

## 16. Observability

### 16.1 Logging

Structured `tracing` logs, controlled by `RUST_LOG`. Key events:

| Component | Event | Fields |
|---|---|---|
| worker | `alert fanned out to shard` | `shard`, `delivered`, `dropped`, `broadcast_duration_us` |
| worker | `device connected` / `device disconnected` | `device_id`, `shard_prefix`, `connections` |
| worker | `starting fanout-worker` | `instance_id`, `owned_prefixes` |
| coordinator | `failed to publish alert to shard` | `shard`, `error` |
| coordinator | `alert delivered but audit write failed` | `shard`, `error` |

The coordinator adds an `x-request-id` header (generated or passed through) to every response for client-side correlation.

### 16.2 Metrics (Prometheus text format)

| Binary | Metric | Type | Meaning |
|---|---|---|---|
| fanout-worker | `pulsegrid_ws_connections_current` | gauge | WebSocket connections currently held |
| api-coordinator | `pulsegrid_coordinator_up` | gauge | Always `1` while the process is up |

Metrics are intentionally hand-rolled (no metrics crate) to keep the dependency surface small. Richer metrics (per-shard delivered/dropped counters, dispatch latency histograms) are on the roadmap.

### 16.3 Health

`/health` and `/ready` exist on both services. `/ready` is currently a static response and does **not** probe Postgres/Redis/NATS.

---

## 17. Testing

| Layer | Where | What it covers |
|---|---|---|
| **Unit** | `crates/domain/src/geo.rs` | Center cell is always included in its own radius; larger radius returns ≥ as many shards; invalid radii (0, > 500 km) rejected; haversine of identical points ≈ 0 |
| **Integration** | `tests/alert_dispatch_test.rs` | Uses `testcontainers` to start an isolated Postgres. Registers a device, creates an alert, and asserts the device's shard is among the alert's resolved targets (the routing correctness claim); records deliveries and checks per-shard counts; asserts `DeviceNotFound` for unknown ids |
| **Manual / live** | n/a | WebSocket JWT auth, `421` misrouting, NATS-outage `207` behavior, rate limiting (`429`), end-to-end fanout latency |

**Not yet automated:** the NATS → worker → WebSocket delivery path and WS JWT enforcement. These were verified manually against live processes (see [§21](#21-known-limitations--honest-caveats)).

---

## 18. Load Testing & Results

> **Read this with the caveats.** All numbers below were measured by the project author on a **single Windows machine** running the load generator, worker, coordinator, and infrastructure **together**, alongside unrelated Docker workloads. They are valid as relative signals, not as production capacity figures.

### 18.1 Methodology

`loadgen` registers N devices (real JWTs), connects them over WebSocket, fires **one alert** covering them, and records, per device, the time from "alert request sent" to "binary frame received". Percentiles come from an HdrHistogram.

### 18.2 Dispatch-loop microbenchmark (the architectural claim)

Measured inside the worker around `broadcast_to_shard` only (in-memory iteration + channel `try_send`), isolating the fanout logic from network and OS effects:

| Shard size | `broadcast_duration_us` |
|---|---|
| 0–5 devices | 10–196 µs |
| 5,000 devices | 9–434 µs |

The dispatch loop is **not** the bottleneck at any tested scale.

### 18.3 End-to-end latency (single machine)

Full path: HTTP → NATS → worker → WebSocket write → client receive.

| Devices | p50 | p90 | p99 | max |
|---|---|---|---|---|
| 10 | 325–375 ms | 327–375 ms | 330–375 ms | 330–375 ms |
| 500 | 405 ms | 438 ms | 494 ms | 503 ms |
| 2,000 | 518 ms | 682 ms | 735 ms | 743 ms |
| 5,000 | 731–1026 ms | 2.4–3.1 s | 3.3–4.0 s | 3.4–4.3 s |

The tail widens sharply beyond ~2,000 concurrent devices **on one machine**, where the load generator's thousands of sockets and the worker's thousands of sockets compete for the same cores and loopback stack. The microbenchmark above shows the fanout logic itself stays in microseconds, which is why this is not read as an architectural ceiling.

### 18.4 How to get a defensible number

Run `loadgen` and each `fanout-worker` on **separate hosts** (or containers with explicit CPU/memory limits), and run **multiple workers** owning disjoint shards. That is the next measurement milestone.

---

## 19. CI & Containerization

### 19.1 GitHub Actions (`.github/workflows/ci.yml`)

Triggers on push and PR to `main`:

```mermaid
flowchart LR
    A["checkout"] --> B["install stable Rust"]
    B --> C["restore cargo cache"]
    C --> D["cargo build --workspace"]
    D --> E["cargo test -p domain"]
    E --> F["cargo test -p integration-tests"]
```

### 19.2 Dockerfile

Two stages:

1. **Builder** (`rust:1.82-slim` + `pkg-config`, `libssl-dev`): `cargo build --release -p api-coordinator -p fanout-worker`
2. **Runtime** (`debian:bookworm-slim` + `ca-certificates`): copies both binaries into `/usr/local/bin`

One image holds both binaries; **which one runs is chosen by the `command:`** (`api-coordinator` or `fanout-worker`) in Compose. If you build the image directly with a newer dependency set than Rust 1.82 supports, bump the base image tag.

### 19.3 Docker Compose services

| Service | Image | Ports | Notes |
|---|---|---|---|
| `postgres` | `postgres:16-alpine` | `5435:5432` | User/password/db all `pulsegrid`; named volume `pgdata` |
| `redis` | `redis:7-alpine` | `6382:6379` | |
| `nats` | `nats:2-alpine` | `4222:4222` | |
| `api-coordinator` | built locally | `8090:8090` | `depends_on` the three above |
| `fanout-worker` | built locally | `8081:8081` | `depends_on` the three above |

`depends_on` only orders container *start*; it does not wait for readiness, so the apps may need a restart if they race Postgres on first boot.

---

## 20. Scaling Out: Running Multiple Workers

The design intent is a fleet of workers, each owning a disjoint slice of the geohash space. Service discovery already supports it:

```mermaid
flowchart LR
    subgraph Fleet
        A["worker A<br/>:8081<br/>owns tkrt,tkrw"]
        B["worker B<br/>:8082<br/>owns tkrs,tkrv"]
    end
    R[("Redis<br/>shard-worker:tkrt -> ws://a<br/>shard-worker:tkrs -> ws://b")]
    A --> R
    B --> R
    API["api-coordinator"] -->|"lookup_shard(prefix)"| R
    API -->|"alerts.geo.tkrt"| A
    API -->|"alerts.geo.tkrs"| B
```

Example on one machine (environment variables override `.env`):

```bash
# worker A
FANOUT_WORKER_PORT=8081 \
FANOUT_WORKER_SHARD_PREFIXES=tkrt,tkrw \
WORKER_PUBLIC_URL=ws://localhost:8081 \
cargo run -p fanout-worker

# worker B
FANOUT_WORKER_PORT=8082 \
FANOUT_WORKER_SHARD_PREFIXES=tkrs,tkrv \
WORKER_PUBLIC_URL=ws://localhost:8082 \
cargo run -p fanout-worker
```

Rules to keep in mind:

- **Prefixes must be disjoint across workers.** The shard registry is last-writer-wins and has no ownership lease, so two workers claiming the same prefix will overwrite each other's advertised URL, and both will receive that shard's NATS messages.
- Set **`WORKER_PUBLIC_URL`** to an address clients can actually reach.
- Every worker and every coordinator needs the **same `JWT_SECRET`**.
- Coordinators are stateless; run as many as you need behind a load balancer.

---

## 21. Known Limitations & Honest Caveats

Some of these come from the original project notes, and some are observations from reading the code. They are listed so no one mistakes the system for something it isn't.

**Benchmarking and testing**

1. **Single-machine benchmarks conflate client and server contention.** See [§18](#18-load-testing--results). Quote no latency number from this repo as a capacity claim.
2. **Compose app services need a separate env file.** The default `.env` targets `localhost` and host ports; containers need service names and internal ports (see [§12.4](#124-run-everything-in-docker)).
3. **Benchmarks ran on a busy dev box** with unrelated Docker workloads competing for CPU.
4. **The NATS → worker → WebSocket path is not covered by `cargo test`.** It was verified manually. Adding a NATS (and Redis) testcontainer is the next test milestone.
5. **Only one worker process has been load-tested.** Multi-worker routing and discovery are implemented and reasoned about, not benchmarked.
6. **Historical README numbers reflect the moment they were captured**, including runs from before the registration-required security fix.

**Behavior and design**

7. **Fanout is shard-granular.** A 5 km alert delivers to every connected device in each targeted ~39 × 19.5 km shard, including devices outside the 5 km circle. Tightening this (per-device distance check in the worker, or finer shards) is a known improvement.
8. **At-most-once delivery.** Core NATS does not persist. Offline devices, full channels (`dropped`), and reconnecting clients miss the alert. There are no acks, retries, or replay.
9. **`total_devices_notified` is a Postgres count**, not a count of live deliveries (see [§13.3](#133-post-alerts)).
10. **Broadcast iterates every connection on the worker** and filters by shard prefix, so it is O(connections on that worker), not strictly O(connections in the shard). It also copies the payload per connection (`to_vec`). Fine at the tested scale; an index of `shard_prefix → connections` and a shared `Bytes`/`Arc` payload would be the optimization.
11. **Duplicate connections for one device_id.** A second connection replaces the first's registry entry; when the *older* socket closes, its cleanup removes the device by id, which can drop the newer connection's registration. Needs a connection-generation token.
12. **Shard registry has no TTL or deregistration.** A crashed worker's `shard-worker:<prefix>` entry persists until overwritten, so clients may be told to connect to a dead host.
13. **Shard changes are advisory.** `PUT /devices/location` reports `shard_changed` but does not close or move the existing socket; the client must reconnect.
14. **`/ready` doesn't check dependencies.**
15. **No data lifecycle.** `devices`, `alerts`, and `alert_deliveries` grow without bound; there is no deregistration endpoint, inactivity purge, or retention policy.
16. **Not rate-limited:** `PUT /devices/location` and WebSocket connects.
17. **Request-ID span field.** The request-ID middleware echoes the header, but the tracing span does not declare a `request_id` field, so the ID may not appear on log lines.
18. **Security gaps** listed in [§14.2](#142-security-gaps-to-be-aware-of).

---

## 22. Roadmap

**Correctness & delivery**
- [ ] Per-device distance filter in the worker for circle-accurate delivery
- [ ] Durable delivery: switch to **NATS JetStream** with acks and replay for reconnecting devices
- [ ] Connection-generation tokens to fix the duplicate-connection race
- [ ] Shard-ownership leases/TTL in the registry; deregister on shutdown

**Security**
- [ ] Authenticate `POST /alerts` and `/admin/*` (API keys / mTLS / privileged JWT audience)
- [ ] Asymmetric JWT signing; shorter-lived tokens + refresh; WS one-time tickets
- [ ] Verify the claimed shard against the device's stored shard on connect

**Operations**
- [ ] Real readiness probes; richer Prometheus metrics (per-shard delivered/dropped, dispatch latency)
- [ ] `.env.docker` / environment-aware config split for Compose
- [ ] Data retention policy and device deregistration endpoint
- [ ] Kubernetes manifests / Helm chart; worker autoscaling by connection count

**Testing & performance**
- [ ] NATS + Redis testcontainers; automated end-to-end WS test in CI
- [ ] Multi-worker, multi-host load test with separate load-generator hosts
- [ ] Copy-free payload fanout and per-shard connection index

**Product**
- [ ] Live map visualization: draw a radius, watch notified devices light up in real time with `broadcast_duration_us` overlaid

---

## 23. Tech Stack Summary

| Layer | Choice | Why |
|---|---|---|
| Language | **Rust** | Memory safety, no GC pauses, strong concurrency story |
| Async runtime | **tokio** | De facto standard; the rest of the stack builds on it |
| Web / WebSocket | **Axum 0.8** | HTTP and WebSocket in one framework, tower middleware ecosystem |
| Database | **PostgreSQL 16 via sqlx 0.9** | Durable device registry and audit trail; embedded migrations |
| Cache / discovery | **Redis 7 via deadpool-redis** | Cross-instance presence and shard-to-worker discovery |
| Messaging | **NATS 2 (async-nats)** | Lightweight, low latency; subject hierarchy maps directly onto geohash prefixes |
| Geospatial | **`geohash` crate + custom BFS/haversine** | The routing logic itself is the novel part |
| Concurrency primitives | **DashMap, bounded `tokio::sync::mpsc`** | Lock-sharded connection map; per-connection backpressure |
| Auth | **jsonwebtoken** (`rust_crypto`) | Stateless device auth, also gates WebSockets |
| Validation | **validator** | Declarative request validation |
| Rate limiting | **tower_governor** | Per-IP token buckets |
| TLS provider | **rustls (ring)** | Installed explicitly at startup |
| Observability | **tracing + tracing-subscriber** | Structured logs with env-filter |
| Load testing | **tokio-tungstenite + hdrhistogram + reqwest + clap** | Models sustained authenticated WS connections, which generic HTTP load tools don't |
| Integration tests | **testcontainers** | Isolated, reproducible Postgres per test |
| CI | **GitHub Actions** | Build + test on every push / PR |

---

## 24. Design Decisions & Trade-offs

| Decision | Rationale | Trade-off |
|---|---|---|
| **Geohash prefix as shard key** | Deterministic, no lookup table; neighbors are computable; maps 1:1 onto NATS subjects | Cells are rectangles with latitude-dependent widths; hot cities create hot shards |
| **4-char shard prefix** | ~39 × 19.5 km is large enough to avoid thousands of tiny shards, small enough to keep one worker from owning a country | Coarse targeting (see limitation 7) |
| **Pub/sub per shard, not per device** | Coordinator publishes O(shards) messages regardless of audience size | Fan-in to workers must then be efficient (it is, in-memory) |
| **`try_send` into bounded channels** | One slow client can never stall the broadcast loop | Lossy under backpressure by design |
| **Per-shard independent publish with 207** | Partial delivery is visible and retryable instead of a blanket 500 | Callers must handle `failed_shards` |
| **Timeout around NATS publish + flush** | A hung alert is worse than a fast failure | A transient slowdown >3 s counts as failure |
| **Persist alert before publishing** | Audit trail exists even if dispatch partially fails | An alert row can exist with zero delivery rows |
| **Pure `domain` crate** | Shared, framework-free logic; token and subject names can't drift | A bit more indirection |
| **JWT in WS query string** | Works for all WebSocket clients | Token may be logged by intermediaries |
| **Hand-rolled metrics** | Fewer dependencies | Minimal metrics until expanded |

---

## 25. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `DATABASE_URL not set` (or `REDIS_URL` / `NATS_URL` / `JWT_SECRET`) at startup | Missing `.env` or env var | `cp .env.example .env`; run from the repo root |
| `FANOUT_WORKER_SHARD_PREFIXES must list at least one prefix` | Empty/missing prefix list | Set it, e.g. `tkrt,tkrw,tkrs,tkrv` |
| WebSocket connect returns **421** | The device's shard prefix isn't in the worker's `FANOUT_WORKER_SHARD_PREFIXES` | Add that prefix, or connect to the worker that owns it. Use the `shard_prefix` from the registration response |
| WebSocket connect returns **401** | Token missing/expired/signed with a different `JWT_SECRET` | Re-register; make sure coordinator and worker share the same secret |
| WebSocket connect returns **403** | Token belongs to a different device than the path id | Use the `device_id` and `token` from the same registration response |
| `ws_url` is `null` in the registration response | No worker has claimed that shard in Redis yet | Start the worker first, or add the prefix to its list |
| `POST /alerts` returns **207** | NATS unreachable or slow for some shards | Check `failed_shards`, NATS health, and coordinator logs |
| `429 Too Many Requests` | Per-IP rate limit | Slow down; for load tests, relax the limiter locally |
| Alert returns 200 but device sees nothing | Device not connected to the targeted shard, channel full (`dropped`), or the shard wasn't in the target set | Check worker log line `alert fanned out to shard` (`delivered`/`dropped`) and the response's `per_shard` |
| Compose app services can't reach Postgres/Redis/NATS | `.env` points at `localhost` | Use service names and internal ports ([§12.4](#124-run-everything-in-docker)) |
| Integration tests fail to start | Docker not running | Start Docker; `testcontainers` needs it |
| Port already in use | Another local service | Change the host-side ports in `docker-compose.yml` / `.env` |

---

<div align="center">

**PulseGrid** was built to demonstrate distributed-systems backend engineering in Rust: geospatial sharding, real-time fanout, backpressure-aware WebSocket delivery, and JWT-secured connections, with its limitations documented openly.

Repository: [`github.com/MuaazTasawar/pulsegrid`](https://github.com/MuaazTasawar/pulsegrid)

</div>
