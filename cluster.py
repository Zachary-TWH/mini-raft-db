"""
Cluster layer: peer list, leader tracking, and the leader-driven
replication loop (canonical Raft AppendEntries push, doubles as heartbeat).

Owns: PEERS, LEADER, next_index, match_index, monitor_leader,
replication_loop.

catch_up_logs / send_heartbeats / LEADER_LOG_INDEX are gone — replication
is now leader-push, not follower-pull, so there is nothing left for a
follower to "catch up" on its own.
"""

import time
import httpx
import storage
import random

PEERS = [
    "http://node1:8000",
    "http://node2:8000",
    "http://node3:8000",
    "http://node4:8000",
    "http://node5:8000"
]

LEADER = "http://node1:8000"
LAST_HEARTBEAT = time.time()

next_index = {}   # peer -> next log index the leader will send that peer
match_index = {}  # peer -> highest log index known replicated on that peer


def set_leader(new_leader):
    global LEADER
    LEADER = new_leader


def quorum_size():
    """Majority needed out of the full configured cluster (not just alive nodes)."""
    return (len(PEERS) // 2) + 1


def reset_leader_state(my_address):
    """Called whenever we become leader. next_index/match_index only make
    sense for whoever currently holds leadership, so they get reset fresh."""
    last_index = storage.last_log_index()
    for peer in PEERS:
        if peer != my_address:
            next_index[peer] = last_index + 1
            match_index[peer] = 0

def note_heartbeat():
    global LAST_HEARTBEAT
    LAST_HEARTBEAT = time.time()

def get_alive_nodes(my_address):
    alive = []

    for peer in PEERS:
        try:
            with httpx.Client(timeout=1.0) as client:
                response = client.get(f"{peer}/health")
                if response.status_code == 200:
                    alive.append(peer)
        except:
            pass

    return alive


def replicate_to_peer(client, my_address, peer, current_term):
    """
    Send this peer whatever it's missing, starting from next_index[peer].
    On success, advance match_index/next_index. On log mismatch, back off
    next_index by one and let the next tick retry further back.
    """

    ni = next_index.get(peer, storage.last_log_index() + 1)
    prev_index = ni - 1
    prev_term = storage.term_at(prev_index)
    #return entries that are at or after next_index[peer]
    entries = [e for e in storage.LOG_ENTRIES if e["index"] >= ni]

    try:
        response = client.put(
            f"{peer}/internal/append_entries",
            json={
                "term": current_term,
                "leader": my_address,
                "prev_log_index": prev_index,
                "prev_log_term": prev_term,
                "entries": entries,
                "leader_commit": storage.COMMIT_INDEX
            },
            timeout=2.0
        )
        result = response.json()

        if result.get("success"):
            if entries:
                match_index[peer] = entries[-1]["index"]
                next_index[peer] = match_index[peer] + 1
            return True

        next_index[peer] = max(1, ni - 1)
        return False

    except:
        return False


def advance_commit_index(my_address, current_term):
    """
    Raft commit rule: an entry is committed once a majority of nodes have
    matched it, AND it's from the leader's current term (this is the part
    that's easy to get wrong — you cannot commit older-term entries purely
    by count, only by riding along with a current-term entry that commits).
    """
    for n in range(storage.last_log_index(), storage.COMMIT_INDEX, -1):
        if storage.term_at(n) != current_term:
            continue

        matched = 1  # ourselves
        for peer in PEERS:
            if peer != my_address and match_index.get(peer, 0) >= n:
                matched += 1

        if matched >= quorum_size():
            storage.apply_committed(n)
            break


def replication_loop(my_address, current_term_getter):
    """
    Runs forever. While we're leader, push AppendEntries to every peer on
    a fixed tick — this is both replication and the heartbeat (an empty
    entries list is just a heartbeat). This replaces send_heartbeats.
    """
    while True:
        if my_address == LEADER:
            with httpx.Client() as client:
                for peer in PEERS:
                    if peer != my_address:
                        replicate_to_peer(client, my_address, peer, current_term_getter())

            advance_commit_index(my_address, current_term_getter())

        time.sleep(0.5)



def monitor_leader(my_address, start_election):
    global LEADER, LAST_HEARTBEAT

    time.sleep(5)
    timeout = random.uniform(10, 25)

    while True:
        if my_address != LEADER:
            if time.time() - LAST_HEARTBEAT > timeout:
                print("Election timeout! Becoming candidate.")

                new_leader = start_election()

                if new_leader == my_address:
                    LEADER = my_address
                    reset_leader_state(my_address)

                LAST_HEARTBEAT = time.time()   # reset regardless of outcome
                timeout = random.uniform(10, 25)
        else:
            timeout = random.uniform(10, 25)

        time.sleep(1)