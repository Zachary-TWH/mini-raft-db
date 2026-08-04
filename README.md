# kv-store

A small distributed key-value store I built to actually learn Raft, instead of just reading the paper.

Five (or more, now) FastAPI nodes talk to each other over HTTP. One is elected leader at a time. Writes go through the leader and need a majority of the cluster to replicate before they're acknowledged. Reads are served by the leader too, after it double-checks it's still actually the leader.

## Running it

```
docker compose up --build
```

Five nodes come up on ports 8001-8005, mapped to `node1`-`node5` internally.

```
curl -X PUT "http://localhost:8001/put/foo?value=bar"
curl http://localhost:8002/get/foo
```

You can hit any node for a write — if it's not the leader, it forwards to whoever is.

## How it works

**Leader election.** Every node has its own randomized election timer. No heartbeat in time -> becomes a candidate, votes for itself, asks everyone else for a vote. Majority wins. There's a pre-vote step before any of this touches real state, so a node that can't actually win doesn't bump the term and cause pointless re-elections.

**Replication.** Leader-driven. It tracks per-follower `nextIndex`/`matchIndex`, and pushes missing entries on a fixed 0.5s tick. Each push includes a consistency check (`prevLogIndex`/`prevLogTerm`) — if a follower's log doesn't line up, its entries get truncated and overwritten with the leader's version.

**Commits.** A write lands in the leader's log immediately but isn't acknowledged until a majority has it. Only then does the leader apply it locally and let followers know the new commit point on the next tick.

**Reads.** Before answering a `GET`, the leader has to confirm — live, right now — that it can still reach a majority of the cluster. If it can't (say it got partitioned off and doesn't know it yet), it returns a 503 instead of quietly serving a stale value. This is what stops the classic "old leader still thinks it's leader and answers with old data" bug.

**Snapshots.** Every 5 commits, the leader dumps the whole store to `snapshot.json` and truncates the log. Restarting a node loads the snapshot first, then replays whatever log entries came after it. If a follower has fallen too far behind for normal replication to catch it up (the entries it needs got compacted away), the leader sends it the whole snapshot instead (`InstallSnapshot`).

**Cluster membership can change while running.** This was the big addition — nodes can be added or removed without restarting the cluster.

- `PUT /add_node?address=http://node6:8000`
- `PUT /remove_node?address=http://node2:8000`

Under the hood this is joint consensus: a membership change is just a special log entry (`type: "config"` instead of `type: "data"`). The leader first commits a "joint" entry that says both the old and new membership are active — during this window, quorum needs a majority of *both* groups, not just one. Once that's safely committed, it commits a follow-up entry that finalizes the new membership on its own. This two-step dance is what stops a single node from having contradictory opinions about who's in the cluster during the switch.

A brand new node added this way starts with nothing — empty log, empty store. It gets caught up the same way a node returning from a long outage would: either normal replication if it's not too far behind, or a full `InstallSnapshot` if it is.

## Things I know are missing / half-baked

- **Leader removing itself from the cluster isn't handled.** If you remove the currently-active leader's own address, it just keeps leading a cluster it's technically no longer part of. Real Raft has it step down once that config entry commits. Skipped it — the actual usage pattern here (you calling the endpoints yourself) means you'd never realistically remove the node you're talking to.
- **`/put` blocks a request thread, polling every 50ms** waiting for its own write to commit, instead of a proper callback/wakeup. Works fine at this scale, wastes a thread per in-flight write.
- **Snapshotting mid-joint-phase is untested.** Everything else about joint consensus has been tested against a live cluster (add, remove, elections, node failures during both), but I haven't specifically forced a snapshot to happen while a config change is half-committed.
- No real security — nodes trust each other completely, no auth on anything.

## Files

- `node.py` — HTTP routes (`/put`, `/get`, `/vote`, `/add_node`, `/remove_node`, `/internal/append_entries`, ...)
- `consensus.py` — term/vote bookkeeping, election + pre-vote logic
- `cluster.py` — membership (`OLD_CONFIG`/`NEW_CONFIG`), replication loop, joint-quorum math
- `storage.py` — in-memory store, append-only log, snapshots, config-entry application