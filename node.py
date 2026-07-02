import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
import os
import httpx
import storage
import consensus
import cluster

MY_ADDRESS = os.getenv("MY_ADDRESS")

def current_term():
    return consensus.CURRENT_TERM


@asynccontextmanager
async def lifespan(app: FastAPI):

    storage.recover_from_log()

    cluster.catch_up_logs(MY_ADDRESS)

    asyncio.create_task(asyncio.to_thread(cluster.monitor_leader, MY_ADDRESS, elect_leader))
    
    asyncio.create_task(asyncio.to_thread(cluster.send_heartbeats, MY_ADDRESS, current_term))

    yield

app = FastAPI(lifespan=lifespan)

@app.get("/sync")
def sync():
    return storage.store

@app.get("/health")
def health():
    return {"status": "ok"}


def elect_leader():
    return consensus.elect_leader(
        MY_ADDRESS,
        cluster.PEERS,
        storage.LOG_INDEX,
        lambda: cluster.get_alive_nodes(MY_ADDRESS)
    )

@app.put("/leader")
def set_leader(new_leader: str):

    if new_leader not in cluster.PEERS:
        raise HTTPException(
            status_code=400,
            detail="Unknown leader"
        )

    cluster.set_leader(new_leader)
    # Reset the vote for the new term, since we have a new leader
    consensus.reset_vote(consensus.CURRENT_TERM)
    print("NEW LEADER:", cluster.LEADER)

    return {"leader": cluster.LEADER}

@app.put("/put/{key}")
def put(key: str, value: str):

    if MY_ADDRESS != cluster.LEADER:

        try:

            with httpx.Client() as client:

                response = client.put(
                    f"{cluster.LEADER}/put/{key}",
                    params={
                        "value": value
                    }
                )

            return response.json()


        except httpx.RequestError:

            raise HTTPException(
                status_code=503,
                detail="Leader unavailable"
            )

    entry_index = storage.next_log_index()
    print("NEW LOG INDEX:", entry_index)

    acks = 1
    # Replicate the entry to other nodes
    with httpx.Client() as client:

        for peer in cluster.PEERS:
            if peer != MY_ADDRESS:
                try:
                    response = client.put(
                        f"{peer}/internal/replicate/{key}",
                            params={
                            "value": value,
                            "index": entry_index
                        }
                    )

                    if response.status_code == 200:
                        acks += 1

                except:
                    pass

    # If we have enough acknowledgments, commit the entry
    if acks >= 3:

        storage.pending_entries.append({
            "index": entry_index,
            "key": key,
            "value": value
        })

        commit(
            key=key,
            value=value,
            index=entry_index
        )

        print("LEADER COMMIT:", entry_index)

        with httpx.Client() as client:
            for peer in cluster.PEERS:
                if peer != MY_ADDRESS:
                    try:
                        client.put(
                            f"{peer}/commit/{key}",
                            params={
                                "value": value,
                                "index": entry_index
                            }
                        )
                    except:
                        pass

        return {
            "stored": key,
            "acks": acks
        }

    raise HTTPException(
    status_code=503,
    detail={
        "error": "Not enough replicas",
        "acks": acks
    }
    )

@app.get("/get/{key}")
def get(key: str):
    if key not in storage.store:
        raise HTTPException(
            status_code=404,
            detail="Key not found"
        )
    return storage.store[key]

@app.put("/internal/replicate/{key}")
def replicate(key: str, value: str, index: int):

    storage.pending_entries.append({
        "index": index,
        "key": key,
        "value": value
    })
    print("PENDING:", storage.pending_entries)
    return {"replicated": key}

@app.put("/vote")
def vote(candidate: str, term: int, log_index: int):
    return consensus.handle_vote_request(
        candidate,
        term,
        log_index,
        storage.LOG_INDEX
    )

@app.get("/store")
def get_store():
    return storage.store

@app.get("/logs")
def get_logs(after: int = 0):

    return [
        entry
        for entry in storage.LOG_ENTRIES
        if entry["index"] > after
    ]

@app.put("/commit/{key}")
def commit(key: str, value: str, index: int):

    committed = storage.commit_entry(key, value, index)

    if committed:
        return {
            "committed": key,
            "index": index
        }

    return {
        "error": "entry not found",
        "index": index,
        "last_committed": storage.LAST_COMMITTED_INDEX
    }


@app.put("/heartbeat")
def heartbeat(term: int, leader: str, log_index: int):
    # Record the heartbeat and update the leader's log index
    cluster.note_heartbeat(log_index)
    print(
        "MY LOG:", storage.LOG_INDEX,
        "LEADER LOG:", cluster.LEADER_LOG_INDEX
    )

    if term >= consensus.CURRENT_TERM:
        consensus.CURRENT_TERM = term
        cluster.set_leader(leader)

    return {"ok": True}