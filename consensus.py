"""
Consensus layer: Raft term/vote bookkeeping and leader election.

Owns: CURRENT_TERM, VOTED_TERM, VOTED_FOR, elect_leader, request_votes, and the decision logic behind the /vote route.

elect_leader/request_votes still make HTTP calls to peers (option 1: pragmatic grouping by "election concern" 
rather than strict no-network purity). get_alive_nodes stays in node.py for now and is passed in.
"""

import httpx

CURRENT_TERM = 0
VOTED_TERM = -1
VOTED_FOR = None

def start_new_term():
    """Bump CURRENT_TERM (called when starting an election as candidate)."""
    global CURRENT_TERM
    CURRENT_TERM += 1
    return CURRENT_TERM


def request_votes(candidate, term, my_address, peers, my_log_index):
    """Ask all peers to vote for `candidate` in `term`. Returns vote count (including self)."""
    votes = 1

    for peer in peers:
        if peer != my_address:
            try:
                with httpx.Client(timeout=2.0) as client:
                    response = client.put(
                        f"{peer}/vote",
                        params={
                            "candidate": candidate,
                            "term": term,
                            "log_index": my_log_index
                        }
                    )

                    if response.json()["vote_for"] == candidate:
                        votes += 1

            except:
                pass

    print("Starting election term", term)

    return votes


def elect_leader(my_address, peers, my_log_index, get_alive_nodes):
    """
    Run one election round: bump term, ask all alive nodes for votes,
    return the candidate with a majority (>=2), or None.
    """
    term = start_new_term()

    best_candidate = None
    best_votes = 0

    for candidate in get_alive_nodes():

        votes = request_votes(candidate, term, my_address, peers, my_log_index)

        print(candidate, "got votes:", votes)

        if votes > best_votes:
            best_votes = votes
            best_candidate = candidate

    if best_votes >= 2:
        return best_candidate

    return None


def handle_vote_request(candidate, term, candidate_log_index, my_log_index):
    """
    Decide how to respond to a /vote request from `candidate`.
    Returns a dict shaped like the old route's response:
    {"term": CURRENT_TERM, "vote_for": ...}
    """
    global CURRENT_TERM
    global VOTED_TERM
    global VOTED_FOR

    if term > CURRENT_TERM:
        CURRENT_TERM = term
        VOTED_TERM = -1
        VOTED_FOR = None

    if candidate_log_index < my_log_index:

        print(
            "Rejecting vote.",
            "Candidate log:", candidate_log_index,
            "My log:", my_log_index
        )

        return {
            "term": CURRENT_TERM,
            "vote_for": None
        }

    if VOTED_TERM != term:
        VOTED_TERM = term
        VOTED_FOR = candidate

    print(
        "TERM:", term,
        "MY_TERM:", CURRENT_TERM,
        "CANDIDATE:", candidate,
        "CANDIDATE_LOG:", candidate_log_index,
        "MY_LOG:", my_log_index
    )

    return {
        "term": CURRENT_TERM,
        "vote_for": VOTED_FOR
    }


def reset_vote(term):
    """Used by /leader when a new leader is set externally (not via election)."""
    global VOTED_FOR
    global VOTED_TERM
    VOTED_FOR = None
    VOTED_TERM = term