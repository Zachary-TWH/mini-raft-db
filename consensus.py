
import time

import httpx
import cluster

CURRENT_TERM = 0
VOTED_TERM = -1
VOTED_FOR = None
STATE = "follower"   # "follower" | "candidate" | "leader"



def handle_pre_vote_request(candidate, term, candidate_log_index, candidate_log_term, my_log_index, my_log_term):
    """
    Answer 'would you vote for me if I asked for real' — WITHOUT touching
    CURRENT_TERM, VOTED_TERM, or VOTED_FOR. Pure hypothetical check.
    """
    if candidate not in cluster.all_known_peers():
        return {"vote_granted": False}

    time_since_heartbeat = time.time() - cluster.LAST_HEARTBEAT
    if time_since_heartbeat < cluster.LEASE_DURATION:
        return {"vote_granted": False}
    
    log_ok = (candidate_log_term > my_log_term) or (
        candidate_log_term == my_log_term and candidate_log_index >= my_log_index
    )

    term_ok = term > CURRENT_TERM

    return {"vote_granted": log_ok and term_ok}


# Handle incoming vote requests from other nodes and determine whether to grant the vote based on the Raft voting rules.
def handle_vote_request(candidate, term, candidate_log_index, candidate_log_term, my_log_index, my_log_term):
    """
    Decide how to respond to a /vote request from `candidate`.
    A candidate's log is "at least as up to date" if its last entry has a
    higher term, or the same term with an index >= ours.
    """
    global CURRENT_TERM, VOTED_TERM, VOTED_FOR, STATE

    if candidate not in cluster.all_known_peers():
        return {"term": CURRENT_TERM, "vote_for": None}

    time_since_heartbeat = time.time() - cluster.LAST_HEARTBEAT
    if time_since_heartbeat < cluster.LEASE_DURATION:
        print("Rejecting vote — still within old leader's lease window:", time_since_heartbeat)
        return {"term": CURRENT_TERM, "vote_for": None}
    
    # If the candidate's term is greater than our current term, we update our term and reset our vote.
    if term > CURRENT_TERM:
        CURRENT_TERM = term
        VOTED_TERM = -1
        VOTED_FOR = None
        STATE = "follower"

    # check if log is at least as up to date as ours
    log_ok = (candidate_log_term > my_log_term) or (
        candidate_log_term == my_log_term and candidate_log_index >= my_log_index
    )

    if not log_ok or term < CURRENT_TERM:
        print(
            "Rejecting vote. Candidate log:", (candidate_log_term, candidate_log_index),
            "My log:", (my_log_term, my_log_index)
        )
        return {"term": CURRENT_TERM, "vote_for": None}

    if VOTED_TERM != term:
        VOTED_TERM = term
        VOTED_FOR = candidate

    print(
        "TERM:", term, "MY_TERM:", CURRENT_TERM,
        "CANDIDATE:", candidate, "VOTE_FOR:", VOTED_FOR
    )

    return {"term": CURRENT_TERM, "vote_for": VOTED_FOR}


def start_election(my_address, peers, my_log_index, my_log_term, get_alive_nodes, request_pre_vote):
    """
    Called when a node's own election timer fires. Becomes a candidate,
    votes for itself, and requests votes for itself only (not for anyone
    else) — unlike the old elect_leader, which asked about every candidate.
    """
    global CURRENT_TERM, STATE, VOTED_TERM, VOTED_FOR

    hypothetical_term = CURRENT_TERM + 1
    pre_voter_set = {my_address}

    for peer in get_alive_nodes():
        if peer != my_address:
            if request_pre_vote(peer, hypothetical_term, my_log_index, my_log_term):
                pre_voter_set.add(peer)

    if not cluster.quorum_met(pre_voter_set):
        # Wouldn't win even hypothetically — don't touch real term at all.
        return None

    # Pre-vote passed — proceed to the real election.
    CURRENT_TERM += 1
    STATE = "candidate"
    VOTED_TERM = CURRENT_TERM
    VOTED_FOR = my_address   # vote for self

    term = CURRENT_TERM
    voter_set = {my_address}

    for peer in get_alive_nodes():
        if peer != my_address:
            try:
                with httpx.Client(timeout=2.0) as client:
                    response = client.put(
                        f"{peer}/vote",
                        params={
                            "candidate": my_address,
                            "term": term,
                            "log_index": my_log_index,
                            "log_term": my_log_term
                        }
                    )
                    if response.json()["vote_for"] == my_address:
                        voter_set.add(peer)
            except:
                pass

    if STATE == "candidate" and cluster.quorum_met(voter_set):
        STATE = "leader"
        return my_address

    STATE = "follower"
    return None