# mini-raft-db

A small key-value store built on top of a simplified Raft consensus implementation. Multiple nodes replicate writes and elect a leader automatically, so the cluster keeps working even if some nodes go down.

## How it works

### Leader election

Every node runs its own election timer with a randomized timeout. Before starting a real election, a node runs a pre-vote round first. It checks whether it would plausibly win, without bumping its own term. This means a node rejoining after a partition can't disrupt the cluster with a term it can't back up.

Followers also won't vote for anyone within a short window after their last heartbeat. That protects a leader's lease from being undercut by a stray timer firing early. Whoever wins a real majority becomes leader.

### Replication

Replication is leader-driven, not follower-pull. The leader tracks two things per follower: `nextIndex` (what it thinks the follower has) and `matchIndex` (what's confirmed). It pushes missing log entries on a fixed 0.5s tick.

Each push includes a consistency check (`prevLogIndex` / `prevLogTerm`). A follower only accepts entries that connect cleanly to its own log — anything conflicting gets truncated and overwritten with the leader's version. If a follower has fallen too far behind, it gets caught up with a full snapshot instead. There's no separate "recovery mode" — it's the same mechanism.

### Commits

A write is appended to the leader's log immediately, but the client doesn't get a response until a majority has replicated it. Once that happens, the leader applies it to its own store, writes it to disk, and tells followers the new commit point on the next tick.

### Heartbeats

There's no separate heartbeat RPC. An AppendEntries call with an empty entry list *is* the heartbeat. A successful round of heartbeats to a majority also renews the leader's read lease.

### Linearizable reads

A `/get` request first checks whether the leader's lease is still valid — that's a cheap timestamp check, and if it's valid the leader answers immediately. If the lease has expired, the leader falls back to confirming leadership with a live round-trip to a majority before answering. Either way, a stale leader can't serve an outdated value without knowing it's stale.

### Cluster membership changes

Adding or removing a node goes through joint consensus. First, a config entry activates both the old and new membership at once, so quorum during the transition needs a majority of *both* groups. A second entry then finalizes the new membership alone.

This goes through the log exactly like a normal write, so it's replicated, ordered, and crash-safe the same way. A brand-new node starts with nothing and catches up via snapshot once it's added.

### Snapshots

Every 5 commits, the leader — and each follower independently, once it applies the same index — serializes the current store and cluster membership to `snapshot.json`, then truncates the log up to that point. `w2.log` only ever holds entries since the last snapshot, so a restart replays a bounded amount of history instead of the whole log.

## Known gaps

- **Leader self-removal isn't handled.** If the leader removes itself via `/remove_node`, it doesn't step down — it keeps acting as leader for a group it's no longer part of. Adding/removing *other* nodes is safe; removing yourself is not.
- **`/put` blocks a thread per in-flight write.** It polls for commit every 50ms instead of using a proper wakeup/callback. Works, but wastes a thread while waiting.
- **No compare-and-swap or transactions.** Every write is unconditional. That's fine for now since nothing here needs conditional writes yet, but it's the natural next primitive if two clients ever need to race for the same key.
- **Test coverage is thin.** `test_cluster.py` runs a handful of integration tests against a live cluster — good for catching obvious regressions, but not enough to catch narrow timing races (a few were found and fixed by hand during development). Real coverage here would need fault injection or chaos testing.

## Files

- `node.py` — HTTP routes: `/put`, `/get`, `/vote`, `/pre_vote`, `/internal/append_entries`, `/internal/install_snapshot`, `/add_node`, `/remove_node`, `/whoami`
- `consensus.py` — term/vote bookkeeping, election and pre-vote logic
- `cluster.py` — cluster membership (`OLD_CONFIG` / `NEW_CONFIG`), leader tracking, replication loop, lease state
- `storage.py` — in-memory store, append-only log, and disk snapshotting
- `kv_client.py` — small Python client that finds the working node automatically