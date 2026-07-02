# Distributed Key-Value Store

A simplified distributed key-value database built in Python to explore the core concepts behind distributed consensus systems such as Raft.

The project simulates a cluster of three nodes running in Docker containers. It implements leader election, log replication, majority-based commits, write-ahead logging, crash recovery, and node catch-up after failure. While inspired by Raft, the implementation intentionally simplifies several production features to focus on understanding the fundamental ideas behind distributed databases.

## Motivation

The goal of this project was not to build a production-ready database, but to gain a practical understanding of distributed systems by implementing the core mechanisms from scratch. Rather than relying on existing consensus libraries, every major component—including leader election, replication, commit coordination, persistence, and recovery—was designed and implemented manually.

## Tech Stack

* **Language:** Python
* **Framework:** FastAPI
* **Networking:** HTTP (`httpx`)
* **Deployment:** Docker & Docker Compose
* **Persistence:** Write-ahead log (`w2.log`)

By the end of the project, the database supports automatic leader election, fault-tolerant writes through majority acknowledgement, durable storage via a write-ahead log, and automatic recovery and synchronization when failed nodes rejoin the cluster.
