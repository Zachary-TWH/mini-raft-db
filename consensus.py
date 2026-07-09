"""
Consensus layer: Raft term/vote bookkeeping and leader election.

Owns: CURRENT_TERM, VOTED_TERM, VOTED_FOR, elect_leader, request_votes,
and the decision logic behind the /vote route.

Vote comparisons now use (log_term, log_index) instead of log_index alone —
index-only comparison isn't safe once entries carry terms (a follower can
have a longer log with an older, wrong term after a crash/partition).
"""

import httpx

CURRENT_TERM = 0
VOTED_TERM = -1
VOTED_FOR = None


def start_new_term():
    global CURRENT_TERM
    CURRENT_TERM += 1
    return CURRENT_TERM


def request_votes(candidate, term, my_address, peers, my_log_index, my_log_term):
    """Ask all peers to vote for `candidate` in `term`. Returns vote count (including self)."""

    self_result = handle_vote_request(
        candidate, term, my_log_index, my_log_term, my_log_index, my_log_term
    )
    
    votes = 1 if self_result["vote_for"] == candidate else 0

    for peer in peers:
        if peer != my_address:
            try:
                with httpx.Client(timeout=2.0) as client:
                    response = client.put(
                        f"{peer}/vote",
                        params={
                            "candidate": candidate,
                            "term": term,
                            "log_index": my_log_index,
                            "log_term": my_log_term
                        }
                    )
                    if response.json()["vote_for"] == candidate:
                        votes += 1
            except:
                pass

    print("Starting election term", term)
    return votes


def elect_leader(my_address, peers, my_log_index, my_log_term, get_alive_nodes, quorum):
    """
    Run one election round: bump term, ask all alive nodes for votes,
    return the candidate with a majority, or None.
    """
    term = start_new_term()

    best_candidate = None
    best_votes = 0

    for candidate in get_alive_nodes():
        votes = request_votes(candidate, term, my_address, peers, my_log_index, my_log_term)
        print(candidate, "got votes:", votes)
        # first come first serve because the first one meet the quorum will be elected as the leader 
        # and based on peer list order, the first one will be the best candidate due to the usage of ">"
        if votes > best_votes:
            best_votes = votes
            best_candidate = candidate

    if best_votes >= quorum:
        return best_candidate

    return None

# Handle incoming vote requests from other nodes and determine whether to grant the vote based on the Raft voting rules.
def handle_vote_request(candidate, term, candidate_log_index, candidate_log_term, my_log_index, my_log_term):
    """
    Decide how to respond to a /vote request from `candidate`.
    A candidate's log is "at least as up to date" if its last entry has a
    higher term, or the same term with an index >= ours.
    """
    global CURRENT_TERM, VOTED_TERM, VOTED_FOR

    # If the candidate's term is greater than our current term, we update our term and reset our vote.
    if term > CURRENT_TERM:
        CURRENT_TERM = term
        VOTED_TERM = -1
        VOTED_FOR = None

    # check if log is at least as up to date as ours
    log_ok = (candidate_log_term > my_log_term) or (
        candidate_log_term == my_log_term and candidate_log_index >= my_log_index
    )

    if not log_ok:
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


def reset_vote(term):
    """Used by /leader when a new leader is set externally (not via election)."""
    global VOTED_FOR, VOTED_TERM
    VOTED_FOR = None
    VOTED_TERM = term