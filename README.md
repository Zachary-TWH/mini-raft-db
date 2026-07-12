# kv-store

A small distributed key-value store, built to learn Raft by implementing (most of) it.

Five FastAPI nodes talk to each other over HTTP. One is elected leader at a time; writes go through the leader and get replicated to a majority of the cluster before they're acknowledged. Reads are served locally by whichever node you hit.

## Running it

```
docker compose up --build
```

Five nodes come up on ports 8001-8005, mapped to `node1`-`node5` internally.

```
curl -X PUT "http://localhost:8001/put/foo?value=bar"
curl http://localhost:8002/get/foo
```

You can hit any node for a write — if it's not the leader, it forwards the request to whoever is.

## How it works

**Leader election.** Every node runs its own election timer with a randomized timeout. If a node doesn't hear from a leader in time, it becomes a candidate, votes for itself, and asks the rest of the cluster for votes. Whoever gets a majority becomes leader. Split votes happen occasionally when two nodes' timers fire close together — that's expected, they just back off and retry with a fresh random timeout.

**Replication.** This is leader-driven, not follower-pull. The leader tracks, per follower, what it thinks that follower has (`nextIndex`) and what's confirmed (`matchIndex`), and pushes missing log entries on a fixed tick (every 0.5s). There's a consistency check on each push (`prevLogIndex`/`prevLogTerm`) so a follower only accepts entries that connect cleanly to its own log — anything conflicting gets truncated and overwritten with the leader's version. This same mechanism handles both routine replication and a follower catching up after being down for a while — there's no separate "recovery mode," it's just what replication looks like when the gap happens to be bigger.

**Commits.** A write is appended to the leader's log immediately, but the client doesn't get a response until a majority of the cluster has replicated it. Once that happens, the leader applies it to its own store, writes it to disk, and tells followers the new commit point on the next tick so they apply it too.

**Heartbeats.** There's no separate heartbeat RPC — an AppendEntries call with an empty entry list *is* the heartbeat. Receiving one resets a follower's election timer.

## Known gaps / things I'd do differently with more time

- No log compaction. `w2.log` grows forever and gets fully replayed on every restart. Real systems snapshot state periodically and truncate the log.
- No `InstallSnapshot` RPC — if a follower falls far enough behind that it needs entries the leader has already compacted away, this design has no way to recover it (moot right now since there's no compaction, but it's the next problem once there is).
- The `/put` endpoint blocks a request thread polling for commit every 50ms instead of using a proper wakeup/callback. Works, but wastes a thread per in-flight write.
- Cluster membership (`PEERS`) is static. Adding or removing a node safely needs Raft's joint-consensus approach, which isn't implemented here.
- Reads aren't linearizable — a partitioned-away former leader, or a follower that hasn't caught up yet, will happily answer a `/get` with stale data. A real implementation would need a lease or read-index mechanism to guarantee reads reflect the latest commit.

## Files

- `node.py` — HTTP routes (`/put`, `/get`, `/vote`, `/internal/append_entries`, ...)
- `consensus.py` — term/vote bookkeeping, election logic
- `cluster.py` — peer list, leader tracking, the replication loop
- `storage.py` — in-memory store + append-only log on disk