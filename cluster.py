"""
Cluster layer: peer list, leader/heartbeat tracking, log catch-up,
and the background loops that watch the leader and send heartbeats.

Owns: PEERS, LEADER, LEADER_LOG_INDEX, LAST_HEARTBEAT,
get_alive_nodes, catch_up_logs, send_heartbeats, monitor_leader.

monitor_leader takes elect_leader as a parameter (it lives in
consensus.py / main.py) to avoid a circular import.
"""

import time
import httpx
import storage

PEERS = [
    "http://node1:8000",
    "http://node2:8000",
    "http://node3:8000"
]

LEADER = "http://node1:8000"
LEADER_LOG_INDEX = 0
LAST_HEARTBEAT = time.time()


def set_leader(new_leader):
    global LEADER
    LEADER = new_leader


def note_heartbeat(log_index):
    """Record that a heartbeat was just received, and the leader's log index."""
    global LAST_HEARTBEAT
    global LEADER_LOG_INDEX
    LAST_HEARTBEAT = time.time()
    LEADER_LOG_INDEX = log_index


def get_alive_nodes(my_address):
    alive = []

    for peer in PEERS:
        try:
            with httpx.Client(timeout=10.0) as client:
                response = client.get(f"{peer}/health")

                if response.status_code == 200:
                    alive.append(peer)

        except:
            pass

    return alive


def catch_up_logs(my_address):
    try:
        with httpx.Client() as client:

            for peer in PEERS:

                if peer == my_address:
                    continue

                response = client.get(
                    f"{peer}/logs",
                    params={"after": storage.LOG_INDEX}
                )

                entries = response.json()

                if entries:

                    print(
                        "CATCHING UP:",
                        len(entries),
                        "entries"
                    )

                for entry in entries:
                        
                    storage.store[entry["key"]] = entry["value"]

                    storage.write_to_log(
                        entry["index"],
                        entry["key"],
                        entry["value"]
                    )

                    storage.bump_log_index(entry["index"])

                break

    except:
        pass


def send_heartbeats(my_address, current_term_getter):
    """
    Loop forever, sending heartbeats to peers whenever we are the leader.
    current_term_getter is a zero-arg callable returning the current Raft
    term (consensus.CURRENT_TERM at call time, since it changes over time).
    """
    while True:

        if my_address == LEADER:

            with httpx.Client() as client:

                for peer in PEERS:

                    if peer != my_address:

                        try:
                            client.put(
                                f"{peer}/heartbeat",
                                params={
                                    "term": current_term_getter(),
                                    "leader": LEADER,
                                    "log_index": storage.LOG_INDEX
                                }
                            )

                        except:
                            pass

        time.sleep(2)


def monitor_leader(my_address, elect_leader):
    """
    Loop forever. If we're not the leader and haven't heard a heartbeat
    in 5s, run an election. elect_leader is a zero-arg callable
    (main.elect_leader, which already knows how to call consensus.elect_leader).
    """
    global LEADER
    global LAST_HEARTBEAT

    time.sleep(5)

    while True:

        if my_address != LEADER:

            if time.time() - LAST_HEARTBEAT > 5:

                print("Heartbeat timeout!")

                new_leader = elect_leader()

                if new_leader:

                    LEADER = new_leader

                    with httpx.Client() as client:
                        for peer in PEERS:
                            try:
                                client.put(
                                    f"{peer}/leader",
                                    params={"new_leader": new_leader}
                                )
                            except:
                                pass

                    LAST_HEARTBEAT = time.time()

        time.sleep(1)