import asyncio
import time
import os
import storage
import consensus
import cluster
import httpx
from contextlib import asynccontextmanager
from typing import List, Dict
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


MY_ADDRESS = os.getenv("MY_ADDRESS")

def current_term():
    return consensus.CURRENT_TERM

class AppendEntriesRequest(BaseModel):
    term: int
    leader: str
    prev_log_index: int
    prev_log_term: int
    entries: List[Dict]
    leader_commit: int

class InstallSnapshotRequest(BaseModel):
    term: int
    leader: str
    last_included_index: int
    last_included_term: int
    store: Dict


@asynccontextmanager
async def lifespan(app: FastAPI):
    storage.recover_from_log()

    if MY_ADDRESS == cluster.LEADER:
        cluster.reset_leader_state(MY_ADDRESS)
        
    # This is a long-running task that will keep checking if the leader is alive and start an election if not.
    asyncio.create_task(asyncio.to_thread(cluster.monitor_leader, MY_ADDRESS, start_election))

    # This is a long-running task that will keep sending AppendEntries to peers if we're the leader.
    asyncio.create_task(asyncio.to_thread(cluster.replication_loop, MY_ADDRESS, current_term))

    yield

app = FastAPI(lifespan=lifespan)

def start_election():
    return consensus.start_election(
        MY_ADDRESS,
        cluster.PEERS,
        storage.last_log_index(),
        storage.last_log_term(),
        lambda: cluster.get_alive_nodes(MY_ADDRESS),
        request_pre_vote
    )

@app.get("/health")
def health():
    return {"status": "ok"}

@app.put("/put/{key}")
def put(key: str, value: str):

    if MY_ADDRESS != cluster.LEADER:
        try:
            with httpx.Client() as client:
                # forward the request to the leader
                response = client.put(f"{cluster.LEADER}/put/{key}", params={"value": value})
            return response.json()
        except httpx.RequestError:
            raise HTTPException(status_code=503, detail="Leader unavailable")

    entry = storage.append_entry(current_term(), key, value)
    print("APPENDED LOCALLY:", entry)
    
    # The replication_loop pushes this out and advances COMMIT_INDEX on its
    # own tick; we just wait here for it to catch up to our entry.
    deadline = time.time() + 5.0
    with storage.COMMIT_CONDITION:
        while storage.COMMIT_INDEX < entry["index"]:
            remaining = deadline - time.time()
            if remaining <= 0:
                raise HTTPException(status_code=503, detail="Commit timed out")
            storage.COMMIT_CONDITION.wait(timeout=remaining)

    return {"stored": key, "index": entry["index"]}

@app.put("/internal/append_entries")
def append_entries(req: AppendEntriesRequest):
    # leader's term must be at least as large as our own, or we reject the request
    if req.term < consensus.CURRENT_TERM:
        return {"success": False, "term": consensus.CURRENT_TERM}
    # reassign our term and state to follower if the leader's term is greater than ours
    consensus.CURRENT_TERM = req.term
    consensus.STATE = "follower"   # a valid leader exists — stop being a candidate
    cluster.LEASE_EXPIRES_AT = 0  
    cluster.set_leader(req.leader)
    cluster.note_heartbeat()

    success = storage.append_entries_from_leader(
        req.prev_log_index, req.prev_log_term, req.entries, req.leader_commit
    )

    return {"success": success, "term": consensus.CURRENT_TERM}

@app.put("/vote")
def vote(candidate: str, term: int, log_index: int, log_term: int):
    result = consensus.handle_vote_request(
        candidate, term, log_index, log_term,
        storage.last_log_index(), storage.last_log_term()
    )

    if result["vote_for"] == candidate:
        cluster.note_heartbeat()   # granted a vote — reset our own timer

    return result

@app.get("/store")
def get_store():
    return storage.store

@app.put("/pre_vote")
def pre_vote(candidate: str, term: int, log_index: int, log_term: int):
    return consensus.handle_pre_vote_request(
        candidate, term, log_index, log_term,
        storage.last_log_index(), storage.last_log_term()
    )

def request_pre_vote(peer, term, my_log_index, my_log_term):
    try:
        with httpx.Client(timeout=2.0) as client:
            response = client.put(
                f"{peer}/pre_vote",
                params={
                    "candidate": MY_ADDRESS,
                    "term": term,
                    "log_index": my_log_index,
                    "log_term": my_log_term
                }
            )
            return response.json().get("vote_granted", False)
    except:
        return False
    
@app.get("/debug/status")
def debug_status():
    return {
        "leader": cluster.LEADER,
        "current_term": consensus.CURRENT_TERM,
        "state": consensus.STATE,
        "commit_index": storage.COMMIT_INDEX,
        "last_log_index": storage.last_log_index(),
        "last_included_index": storage.LAST_INCLUDED_INDEX,
        "next_index": cluster.next_index,
        "match_index": cluster.match_index,
        "last_heartbeat_seconds_ago": time.time() - cluster.LAST_HEARTBEAT
    }

@app.get("/get/{key}")
def get(key: str):
    if MY_ADDRESS != cluster.LEADER:
        raise HTTPException(status_code=503, detail="Not the leader — retry against the current leader")

    read_index = storage.COMMIT_INDEX

    if not cluster.lease_valid():
        if not cluster.confirm_leadership(MY_ADDRESS, current_term()):
            raise HTTPException(status_code=503, detail="Could not confirm leadership — try again")

    # Rare edge case: make sure our own commit has caught up to the point
    # we recorded before confirming leadership.
    deadline = time.time() + 2.0
    while storage.COMMIT_INDEX < read_index and time.time() < deadline:
        time.sleep(0.01)

    if key not in storage.store:
        raise HTTPException(status_code=404, detail="Key not found")
    return storage.store[key]

@app.put("/internal/install_snapshot")
def install_snapshot(req: InstallSnapshotRequest):
    if req.term < consensus.CURRENT_TERM:
        return {"success": False, "term": consensus.CURRENT_TERM}

    consensus.CURRENT_TERM = req.term
    consensus.STATE = "follower"
    cluster.LEASE_EXPIRES_AT = 0   # any lease we held as ex-leader is no longer trustworthy
    cluster.set_leader(req.leader)
    cluster.note_heartbeat()

    storage.install_snapshot(
        req.last_included_index,
        req.last_included_term,
        req.store
    )

    return {"success": True, "term": consensus.CURRENT_TERM}

@app.put("/add_node")
def add_node(address: str):
    # Only the leader can add a new node to the cluster. If this node is not the leader, it will return a 503 error.
    if MY_ADDRESS != cluster.LEADER:
        raise HTTPException(status_code=503, detail="Not the leader — retry against the current leader")

    new_members = list(cluster.OLD_CONFIG) + [address]
    success = cluster.start_config_change(MY_ADDRESS, current_term(), new_members)

    if success:
        return {"status": "added", "new_config": cluster.OLD_CONFIG}
    raise HTTPException(status_code=503, detail="Config change did not commit in time")

@app.put("/remove_node")
def remove_node(address: str):

    if address == MY_ADDRESS:
            raise HTTPException(status_code=400, detail="Leader cannot remove itself (not supported)")
    
    if MY_ADDRESS != cluster.LEADER:
        raise HTTPException(status_code=503, detail="Not the leader — retry against the current leader")

    if address not in cluster.OLD_CONFIG:
        raise HTTPException(status_code=400, detail="Address not in current config")

    new_members = [p for p in cluster.OLD_CONFIG if p != address]
    success = cluster.start_config_change(MY_ADDRESS, current_term(), new_members)

    if success:
        return {"status": "removed", "new_config": cluster.OLD_CONFIG}
    raise HTTPException(status_code=503, detail="Config change did not commit in time")

@app.get("/whoami")
def whoami():
    return {
        "address": MY_ADDRESS,
        "is_leader": MY_ADDRESS == cluster.LEADER,
        "leader": cluster.LEADER
    }