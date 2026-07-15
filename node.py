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
        cluster.quorum_size()
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
    while time.time() < deadline:
        if storage.COMMIT_INDEX >= entry["index"]:
            return {"stored": key, "index": entry["index"]}
        time.sleep(0.05)

    raise HTTPException(status_code=503, detail="Commit timed out")

@app.get("/get/{key}")
def get(key: str):
    if key not in storage.store:
        raise HTTPException(status_code=404, detail="Key not found")
    return storage.store[key]

@app.put("/internal/append_entries")
def append_entries(req: AppendEntriesRequest):
    if req.term < consensus.CURRENT_TERM:
        return {"success": False, "term": consensus.CURRENT_TERM}

    consensus.CURRENT_TERM = req.term
    consensus.STATE = "follower"   # a valid leader exists — stop being a candidate
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


@app.post("/debug/snapshot")
def debug_snapshot():
    storage.take_snapshot()
    return {"snapshotted_at": storage.LAST_INCLUDED_INDEX}