# Distributed MESI Cache Coherency Engine

A high-performance, distributed cache coherency engine implementing a software-level **MESI (Modified, Exclusive, Shared, Invalid) Protocol** across Dockerized Spring Boot cache nodes, synchronized in real-time via Redis Pub/Sub.

This cache engine guarantees strong consistency across distributed nodes under concurrent workloads, node crashes, and network partitions by incorporating global logical clocks (versioning), cold-startup recovery sync, and network partition healing.

---

## 1. System Architecture

The cluster consists of multiple independent cache nodes and a central Redis instance serving as both the Pub/Sub broker and the backing database:

```
                  +-----------------------------------+
                  |        Redis (Port 6381)          |
                  |  - Pub/Sub Topic: cache-events    |
                  |  - Backing Store: Hash cache-data |
                  |  - Global Clock:  global-version  |
                  +-----------------+-----------------+
                                    |
            +-----------------------+-----------------------+
            |                       |                       |
+-----------+-----------+   +-------+-----------+   +-----------+-----------+
| Cache Node 1 (8080)   |   | Cache Node 2 (8081)   |   | Cache Node 3 (8082)   |
| - Local Memory Cache  |   | - Local Memory Cache  |   | - Local Memory Cache  |
| - Connection Watchdog |   | - Connection Watchdog |   | - Connection Watchdog |
+-----------------------+   +-----------------------+   +-----------------------+
```

### Core Components
* **MESI State Machine ([CacheService.java](src/main/java/com/sandeep/cache_node/service/CacheService.java))**: Handles the caching state transitions (`MODIFIED`, `EXCLUSIVE`, `SHARED`, `INVALID`) for read/write requests.
* **Pub/Sub Sync ([CacheEventPublisher.java](src/main/java/com/sandeep/cache_node/service/CacheEventPublisher.java) & [CacheEventSubscriber.java](src/main/java/com/sandeep/cache_node/service/CacheEventSubscriber.java))**: Broadcasts invalidations and data transfers across the cluster on the `cache-events` topic.
* **Global Logical Clock ([VersionService.java](src/main/java/com/sandeep/cache_node/service/VersionService.java))**: Generates monotonic sequential versions using a shared Redis counter (`global-version`).
* **Connection Watchdog ([RecoveryService.java](src/main/java/com/sandeep/cache_node/service/RecoveryService.java))**: Performs periodic resource-safe checks to detect network partitions and orchestrate healing.
* **Write-Through Database ([BackingStoreService.java](src/main/java/com/sandeep/cache_node/service/BackingStoreService.java))**: Backs the cluster with persistent storage inside a Redis Hash (`cache-data`).

---

## 2. MESI State Transitions

Each key in a node's local cache transitions through the following states:

| Current State | Event | Next State | Action / Network Messages |
| :--- | :--- | :--- | :--- |
| **INVALID** / **Miss** | Local Read | **SHARED** (Remote hit)<br>**EXCLUSIVE** (DB hit) | Broadcasts `READ_REQUEST`. Blocks for remote response. If timed out, loads from DB and sets to EXCLUSIVE. |
| **INVALID** / **Miss** | Local Write | **MODIFIED** | Broadcasts `WRITE_REQUEST` (invalidation) and writes to backing database. |
| **SHARED** | Local Read | **SHARED** | Cache Hit. Returns value immediately. |
| **SHARED** | Local Write | **MODIFIED** | Broadcasts `WRITE_REQUEST` (invalidation), updates locally, and writes to backing database. |
| **SHARED** | Remote Read | **SHARED** | Responds with `DATA_RESPONSE` containing the value and version. |
| **SHARED** | Remote Write | **INVALID** | Invalidates local entry. |
| **EXCLUSIVE** | Local Read | **EXCLUSIVE** | Cache Hit. Returns value immediately. |
| **EXCLUSIVE** | Local Write | **MODIFIED** | Updates local entry. No network message needed (exclusive owner). |
| **EXCLUSIVE** | Remote Read | **SHARED** | Flushes value to DB, transitions to SHARED, and responds with `DATA_RESPONSE`. |
| **EXCLUSIVE** | Remote Write | **INVALID** | Invalidates local entry. |
| **MODIFIED** | Local Read | **MODIFIED** | Cache Hit. Returns dirty value immediately. |
| **MODIFIED** | Local Write | **MODIFIED** | Updates local entry and increments version. |
| **MODIFIED** | Remote Read | **SHARED** | Flushes value to DB, transitions to SHARED, and responds with `DATA_RESPONSE`. |
| **MODIFIED** | Remote Write | **INVALID** | Flushes dirty value to DB, invalidates local entry. |

---

## 3. Recovery & Partition Healing Protocols

### Cold Startup Recovery
When a node starts up (or restarts after a crash), its cache memory is empty. To prevent reading stale values from the database when another node has a dirty write (state `MODIFIED`), the node performs a startup sync:
1. On boot, the node broadcasts a `SYNC_REQUEST` event containing its ID.
2. Active nodes receive `SYNC_REQUEST` and publish a `SYNC_RESPONSE` containing their active key-to-version mappings: `{"key1": 12, "key2": 15}`.
3. The recovering node compares the received remote versions with its local versions, invalidating any local entry that is stale.

### Network Partition Healing (Split-brain Recovery)
Because Redis Pub/Sub is fire-and-forget, a node that loses connection to Redis will miss all `WRITE_REQUEST` invalidations sent during the partition. Once the connection is restored, it could serve stale cache hits.
1. The **`RecoveryService`** runs a resource-safe check (using try-with-resources to avoid connection leaks) every 5 seconds, pinging Redis.
2. If connection is lost, it flags the node as disconnected.
3. When the connection is restored (partition healed), the service detects the state transition from disconnected to connected and automatically broadcasts a `SYNC_REQUEST`.
4. The node reconciles the returned version maps and invalidates any stale keys, recovering full cache consistency.

---

## 4. Verified Test Suite (3-Step Validation)

We have verified the cache coherency engine using a 3-step testing protocol simulating writes, cold bootups, and network partitions.

### Step 1: Basic Propagation (MESI Coherence)
**Command**: Store key `color` = `"red"` on Node 1, and read it from Node 2 & Node 3:
```bash
# 1. Put key 'color' on Node 1
curl -s -X PUT -H "Content-Type: application/json" -d '{"value":"red"}' http://localhost:8080/cache/color

# 2. Get key 'color' from Node 2 & 3
curl -s http://localhost:8081/cache/color
curl -s http://localhost:8082/cache/color

# 3. Check states on all nodes
curl -s http://localhost:8080/cache/states
curl -s http://localhost:8081/cache/states
curl -s http://localhost:8082/cache/states
```
**Output**:
```
Stored
red
red
{"color":"SHARED"}
{"color":"SHARED"}
{"color":"SHARED"}
```
*Verification: The write on Node 1 successfully triggered a cache-to-cache transfer to Node 2 and Node 3, transitioning all nodes to `SHARED` state.*

---

### Step 2: Cold Startup Recovery
**Command**: Store `vehicle` = `"car"` on Node 1, stop Node 3, update `vehicle` = `"bike"` on Node 1, restart Node 3:
```bash
# 1. Put key 'vehicle' on Node 1
curl -s -X PUT -H "Content-Type: application/json" -d '{"value":"car"}' http://localhost:8080/cache/vehicle

# 2. Stop Node 3 to simulate crash
docker compose stop cache-node-3

# 3. Update 'vehicle' to 'bike' on Node 1 (Node 3 is offline, missing the invalidation)
curl -s -X PUT -H "Content-Type: application/json" -d '{"value":"bike"}' http://localhost:8080/cache/vehicle

# 4. Restart Node 3
docker compose start cache-node-3
sleep 4

# 5. Read 'vehicle' and its entry status from Node 3
curl -s http://localhost:8082/cache/vehicle
curl -s http://localhost:8082/cache/entry/vehicle
```
**Output**:
```
Stored
[+] Stopping 1/1
 ✔ Container cache-node-cache-node-3-1  Stopped
Stored
[+] Running 2/2
 ✔ Container redis                      Healthy
 ✔ Container cache-node-cache-node-3-1  Started
bike
{"value":"bike","version":3,"state":"SHARED"}
```
*Verification: Upon bootup, Node 3 requested a sync, reconciled its version tracker, and successfully received the updated `"bike"` value (version 3) in `SHARED` state.*

---

### Step 3: Network Partition Healing
**Command**: Disconnect Node 1 from the network, update `vehicle` = `"plane"` on Node 2, reconnect Node 1:
```bash
# 1. Disconnect Node 1 from Docker default network
docker network disconnect cache-node_default cache-node-cache-node-1-1

# 2. Update 'vehicle' on Node 2 (Node 1 misses the invalidation because it's offline)
curl -s -X PUT -H "Content-Type: application/json" -d '{"value":"plane"}' http://localhost:8081/cache/vehicle

# 3. Reconnect Node 1 back to the network
docker network connect cache-node_default cache-node-cache-node-1-1
sleep 7

# 4. Read 'vehicle' and check state from Node 1
curl -s http://localhost:8080/cache/vehicle
curl -s http://localhost:8080/cache/entry/vehicle
```
**Output**:
```
Stored
plane
{"value":"plane","version":4,"state":"SHARED"}
```
*Verification: RecoveryService on Node 1 detected the network reconnect after the partition healed, broadcasted a SYNC_REQUEST, invalidated its local stale entry ("bike"), and successfully fetched the new value "plane" (version 4) on the next read.*

---

## 5. How to Run Locally

### Requirements
* Docker and Docker Compose
* JDK 21 and Maven (to build outside Docker)

### Build & Run the Cluster
Run the following command in the project root to compile the applications and start the cluster:
```bash
docker compose up -d --build
```
This starts 3 cache nodes and 1 Redis instance. You can query each node at:
* Node 1: `http://localhost:8080/cache`
* Node 2: `http://localhost:8081/cache`
* Node 3: `http://localhost:8082/cache`
* Redis: `localhost:6381`
