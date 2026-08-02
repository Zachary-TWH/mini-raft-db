
import time
import httpx
import storage
import random
import json
import threading

PEERS = [
    "http://node1:8000",
    "http://node2:8000",
    "http://node3:8000",
    "http://node4:8000",
    "http://node5:8000"
]
OLD_CONFIG = list(PEERS)   # starts as your current 5-node list
NEW_CONFIG = None          # None = no config change in progress
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
    for peer in all_known_peers():
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

    if ni <= storage.LAST_INCLUDED_INDEX:
        return send_install_snapshot(client, my_address, peer, current_term)

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

        matched_nodes = {my_address}
        for peer in all_known_peers():
            if peer != my_address and match_index.get(peer, 0) >= n:
                matched_nodes.add(peer)

        if quorum_met(matched_nodes):
            storage.apply_committed(n)
            break

def replication_loop(my_address, current_term_getter):
    while True:
        if my_address == LEADER:
            client = httpx.Client()
            threads = []
            for peer in all_known_peers():
                if peer != my_address:
                    t = threading.Thread(
                        target=replicate_to_peer,
                        args=(client, my_address, peer, current_term_getter())
                    )
                    t.start()
                    threads.append(t)

            for t in threads:
                t.join(timeout=2.0)

            client.close()
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

def confirm_leadership(my_address, current_term):
    results = {}

    def check_peer(peer):
        with httpx.Client() as client:
            results[peer] = replicate_to_peer(client, my_address, peer, current_term)

    threads = []
    for peer in all_known_peers():
        if peer != my_address:
            t = threading.Thread(target=check_peer, args=(peer,))
            t.start()
            threads.append(t)

    for t in threads:
        t.join(timeout=3.0)

    confirmed_nodes = {my_address}
    confirmed_nodes.update(peer for peer, ok in results.items() if ok)

    return quorum_met(confirmed_nodes)

def send_install_snapshot(client, my_address, peer, current_term):
    try:
        with open(storage.SNAPSHOT_FILE, "r") as f:
            snapshot_data = json.load(f)

        response = client.put(
            f"{peer}/internal/install_snapshot",
            json={
                "term": current_term,
                "leader": my_address,
                "last_included_index": snapshot_data["last_included_index"],
                "last_included_term": snapshot_data["last_included_term"],
                "store": snapshot_data["store"]
            },
            timeout=5.0
        )
        result = response.json()

        if result.get("success"):
            match_index[peer] = snapshot_data["last_included_index"]
            next_index[peer] = match_index[peer] + 1
            return True

        return False

    except:
        return False

def get_current_configs():
    """Returns the list(s) of configs currently in effect."""
    if NEW_CONFIG is None:
        return [OLD_CONFIG]
    return [OLD_CONFIG, NEW_CONFIG]

def majority_reached(voters_or_matchers, config):
    """Given a set of nodes that said yes/matched, check if that's a
    majority of this specific config."""
    count = sum(1 for node in config if node in voters_or_matchers)
    needed = (len(config) // 2) + 1
    return count >= needed

def quorum_met(voters_or_matchers):
    """
    Joint-aware quorum check. voters_or_matchers is a set of node
    addresses that said yes / are caught up. Returns True only if
    majority is reached in EVERY currently-active config (just one
    config normally, two during a joint phase).
    """
    for config in get_current_configs():
        if not majority_reached(voters_or_matchers, config):
            return False
    return True

def start_config_change(my_address, current_term, new_members):
    """
    Leader-only. Kicks off adding/removing nodes: appends the joint-phase
    entry (old+new both active), waits for it to commit, then appends the
    follow-up entry that finalizes new_members as the only config.
    """
    global OLD_CONFIG, NEW_CONFIG

    entry = storage.append_config_entry(current_term, list(OLD_CONFIG), list(new_members))
    print("JOINT PHASE STARTED:", entry)

    for member in new_members:
        if member not in next_index:
            next_index[member] = 1
            match_index[member] = 0

    joint_index = entry["index"]
    deadline = time.time() + 10.0
    while time.time() < deadline:
        if storage.COMMIT_INDEX >= joint_index:
            break
        time.sleep(0.05)
    else:
        return False  # joint phase never committed — bail, don't finalize

    final_entry = storage.append_config_entry(current_term, list(new_members), None)
    print("FINALIZING CONFIG:", final_entry)

    final_index = final_entry["index"]
    deadline = time.time() + 10.0
    while time.time() < deadline:
        if storage.COMMIT_INDEX >= final_index:
            return True
        time.sleep(0.05)

    return False


def all_known_peers():
    """Union of OLD_CONFIG and NEW_CONFIG (if joint phase active).
    This is 'everyone we might need to talk to right now.'"""
    peers = set(OLD_CONFIG)
    if NEW_CONFIG is not None:
        peers.update(NEW_CONFIG)
    return peers