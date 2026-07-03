# Distributed Key-Value Store

A 5-node key-value store built with FastAPI, HTTP, and Docker Compose to explore core distributed consensus concepts. Implements leader election, log replication, write-ahead logging, and crash recovery. Loosely based on Raft, with some parts simplified for clarity.

## Features

- Leader election using terms and majority voting
- Heartbeat-based failure detection and failover
- Log-aware voting to prevent stale leader election
- Leader-to-follower log replication
- Majority acknowledgement required before commit
- Ordered commits via commit index
- Write-ahead log for durability
- Crash recovery from disk on restart
- Log catch-up for lagging nodes
- Leader-only reads for strong consistency
- REST API with standard HTTP status codes

## Stack

Python, FastAPI, httpx (inter-node communication), Docker Compose, write-ahead log (`w2.log`) for persistence.

## Topology

5 identical nodes — 1 leader, 4 followers, same code on each.


docker build -t my-kv-store .
docker compose up -d



curl -X PUT "http://localhost:8001/put/a?value=100"
curl "http://localhost:8001/get/a"


## Design Notes

- Built from scratch instead of using a consensus library, for learning purposes.
- Prioritized clarity over production-level optimization.
- Kept as a single file until the system was feature-complete, then refactored.
- Leader-only reads chosen for simplicity and strong consistency.

## Limitations

No randomized election timeouts, no `lastLogTerm` check, term/vote state not persisted, no log compaction/snapshots, no dynamic membership, no quorum reads. Omitted to keep focus on core consensus mechanics.
