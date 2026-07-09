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

    asyncio.create_task(asyncio.to_thread(cluster.monitor_leader, MY_ADDRESS, elect_leader))
    asyncio.create_task(asyncio.to_thread(cluster.replication_loop, MY_ADDRESS, current_term))

    yield

app = FastAPI(lifespan=lifespan)

# elect leader via consensus.elect_leader and return the best candidate if any, else return None
def elect_leader():
    return consensus.elect_leader(
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

@app.put("/leader")
def set_leader(new_leader: str):

    # check if the new leader is in the cluster peers
    if new_leader not in cluster.PEERS:
        raise HTTPException(status_code=400, detail="Unknown leader")
    # tell followers who the new leader is, and reset our vote for this term
    cluster.set_leader(new_leader)
    consensus.reset_vote(consensus.CURRENT_TERM)
    print("NEW LEADER:", cluster.LEADER)

    return {"leader": cluster.LEADER}

@app.put("/put/{key}")
def put(key: str, value: str):

    if MY_ADDRESS != cluster.LEADER:
        try:
            with httpx.Client() as client:
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
    cluster.set_leader(req.leader)
    cluster.note_heartbeat()

    success = storage.append_entries_from_leader(
        req.prev_log_index, req.prev_log_term, req.entries, req.leader_commit
    )

    return {"success": success, "term": consensus.CURRENT_TERM}

@app.put("/vote")
def vote(candidate: str, term: int, log_index: int, log_term: int):
    return consensus.handle_vote_request(
        candidate, term, log_index, log_term,
        storage.last_log_index(), storage.last_log_term()
    )
