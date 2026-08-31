
## How it works

**Leader election.** Every node runs its own election timer with a randomized timeout. Before running a real election a node does a pre-vote round first — checks if it'd plausibly win before bumping its own term, so a partitioned node rejoining doesn't disrupt the cluster with a term it can't back up. Followers also won't vote for anyone within a short window after their last heartbeat, so a leader's lease can't be undercut by a stray timer firing early. Whoever gets a real majority becomes leader.

**Replication.** Leader-driven, not follower-pull. The leader tracks, per follower, what it thinks that follower has (`nextIndex`) and what's confirmed (`matchIndex`), and pushes missing log entries on a fixed tick (every 0.5s). There's a consistency check on each push (`prevLogIndex`/`prevLogTerm`) so a follower only accepts entries that connect cleanly to its own log — anything conflicting gets truncated and overwritten with the leader's version. A follower that's fallen too far behind (past what the leader still has in its log) gets caught up via a full snapshot instead — same mechanism, no separate "recovery mode."

**Commits.** A write is appended to the leader's log immediately, but the client doesn't get a response until a majority has replicated it. Once that happens, the leader applies it to its own store, writes it to disk, and tells followers the new commit point on the next tick.

**Heartbeats.** No separate heartbeat RPC — an AppendEntries call with an empty entry list *is* the heartbeat. A successful round of heartbeats to a majority also renews the leader's read lease.

**Linearizable reads.** A `/get` first checks if the leader's lease is still valid (cheap, just a timestamp check) and answers immediately if so. If the lease has expired, it falls back to confirming leadership with a live round-trip to a majority before answering. Either way, a stale leader can't serve an outdated value without knowing it's stale.

**Cluster membership changes.** Adding or removing a node goes through joint consensus — a config entry that activates both the old and new membership at once (so quorum during the transition needs a majority of *both* groups), followed by a second entry that finalizes the new membership alone. This goes through the log exactly like a normal write, so it's replicated, ordered, and crash-safe the same way. A brand-new node starts with nothing and catches up via snapshot once it's added.

**Snapshots.** Every 5 commits, the leader (and each follower independently, once it applies the same index) serializes the current store + cluster membership to `snapshot.json` and truncates the log up to that point. `w2.log` only ever holds entries since the last snapshot, so a restart replays a bounded amount of history instead of the whole log.

## Known gaps

- If the leader removes itself from the cluster via `/remove_node`, it doesn't step down — it'll keep acting as leader for a group it's no longer technically part of. Only add/remove-others is handled safely; self-removal is a known unhandled edge case.
- The `/put` endpoint blocks a request thread polling for commit every 50ms instead of using a proper wakeup/callback. Works, but wastes a thread per in-flight write.
- No compare-and-swap or transactions — every write is unconditional. Fine for this project since nothing here actually needs conditional writes yet, but it's the natural next primitive if two clients ever needed to race for the same key safely.
- Test coverage is a handful of integration tests hitting a live cluster (`test_cluster.py`) — good for catching obvious regressions, but nowhere near enough to catch narrow timing races (e.g. the handful of race conditions found and fixed during development). Real coverage for that would need something closer to fault injection / chaos testing.

## Files

- `node.py` — HTTP routes (`/put`, `/get`, `/cas`... wait no, drop that — `/vote`, `/pre_vote`, `/internal/append_entries`, `/internal/install_snapshot`, `/add_node`, `/remove_node`, `/whoami`, ...)
- `consensus.py` — term/vote bookkeeping, election and pre-vote logic
- `cluster.py` — cluster membership (`OLD_CONFIG`/`NEW_CONFIG`), leader tracking, replication loop, lease state
- `storage.py` — in-memory store + append-only log + snapshotting on disk
- `kv_client.py` — small Python client that finds the working node automatically