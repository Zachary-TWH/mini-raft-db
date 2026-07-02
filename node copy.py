import asyncio
from contextlib import asynccontextmanager
from urllib import response
from fastapi import FastAPI, HTTPException
import os
import httpx
import time

# Global variables
MY_ADDRESS = os.getenv("MY_ADDRESS")
PEERS = [
    "http://node1:8000",
    "http://node2:8000",
    "http://node3:8000"
]
LEADER = "http://node1:8000"
store = {}
LOG_INDEX = 0
LEADER_LOG_INDEX = 0
pending_entries = []
CURRENT_TERM = 0
VOTED_TERM = -1
VOTED_FOR = None
LAST_COMMITTED_INDEX = 0
LOG_ENTRIES = []
LAST_HEARTBEAT = time.time()



@asynccontextmanager
async def lifespan(app: FastAPI):

    recover_from_log()

    catch_up_logs()

    asyncio.create_task(asyncio.to_thread(monitor_leader))
    asyncio.create_task(asyncio.to_thread(send_heartbeats))

    yield

app = FastAPI(lifespan=lifespan)    

def monitor_leader():
    global LEADER
    global LAST_HEARTBEAT

    time.sleep(5)

    while True:

        if MY_ADDRESS != LEADER:

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

@app.get("/sync")
def sync():
    return store

def recover_from_log():

    global LOG_INDEX
    global LAST_COMMITTED_INDEX
    global LOG_ENTRIES

    try:

        with open("w2.log", "r") as f:

            for line in f:

                index, key, value = line.strip().split(", ")

                index = int(index)

                # RAM database
                store[key] = value

                # RAM copy of the log
                LOG_ENTRIES.append({
                    "index": index,
                    "key": key,
                    "value": value
                })

                # Highest log index
                LOG_INDEX = max(LOG_INDEX, index)

        # Everything already on disk is committed
        LAST_COMMITTED_INDEX = LOG_INDEX

        print("Recovered:", store)
        print("Recovered LOG_INDEX:", LOG_INDEX)
        print("Recovered LAST_COMMITTED_INDEX:", LAST_COMMITTED_INDEX)

    except FileNotFoundError:

        print("No log found, starting empty")

@app.get("/health")
def health():
    return {"status": "ok"}



def elect_leader():

    global CURRENT_TERM

    CURRENT_TERM += 1

    best_candidate = None
    best_votes = 0

    for candidate in get_alive_nodes():

        votes = request_votes(candidate, CURRENT_TERM)

        print(candidate, "got votes:", votes)

        if votes > best_votes:
            best_votes = votes
            best_candidate = candidate

    if best_votes >= 2:
        return best_candidate

    return None

@app.put("/leader")
def set_leader(new_leader: str):

    if new_leader not in PEERS:
        raise HTTPException(
            status_code=400,
            detail="Unknown leader"
        )

    global LEADER
    global VOTED_FOR
    global VOTED_TERM

    LEADER = new_leader

    VOTED_FOR = None
    VOTED_TERM = CURRENT_TERM
    print("NEW LEADER:", LEADER)

    return {"leader": LEADER}

@app.put("/put/{key}")
def put(key: str, value: str):

    global LOG_INDEX

    if MY_ADDRESS != LEADER:

        try:

            with httpx.Client() as client:

                response = client.put(
                    f"{LEADER}/put/{key}",
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

    LOG_INDEX += 1
    entry_index = LOG_INDEX
    print("NEW LOG INDEX:", entry_index)

    acks = 1
    # Replicate the entry to other nodes
    with httpx.Client() as client:

        for peer in PEERS:
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
    if acks >= 2:

        pending_entries.append({
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
            for peer in PEERS:
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

    if MY_ADDRESS != LEADER:
        raise HTTPException(
            status_code=307,
            detail={
                "error": "Not the leader",
                "leader": LEADER
            }
        )

    if key not in store:
        raise HTTPException(
            status_code=404,
            detail="Key not found"
        )

    return store[key]

@app.put("/internal/replicate/{key}")
def replicate(key: str, value: str, index: int):

    pending_entries.append({
        "index": index,
        "key": key,
        "value": value
    })
    print("PENDING:", pending_entries)
    return {"replicated": key}

def write_to_log(index, key, value):

    entry = {
        "index": index,
        "key": key,
        "value": value
    }

    LOG_ENTRIES.append(entry)

    with open("w2.log", "a") as f:
        f.write(f"{index}, {key}, {value}\n")

@app.put("/vote")
def vote(candidate: str, term: int, log_index: int):

    global CURRENT_TERM
    global VOTED_TERM
    global VOTED_FOR

    if term > CURRENT_TERM:
        CURRENT_TERM = term
        VOTED_TERM = -1
        VOTED_FOR = None
    
    if log_index < LOG_INDEX:

        print(
            "Rejecting vote.",
            "Candidate log:", log_index,
            "My log:", LOG_INDEX
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
        "CANDIDATE_LOG:", log_index,
        "MY_LOG:", LOG_INDEX
    )

    return {
        "term": CURRENT_TERM,
        "vote_for": VOTED_FOR
    }

def request_votes(candidate, term):

    votes = 1

    for peer in PEERS:
        if peer != MY_ADDRESS:
            try:
                with httpx.Client(timeout=2.0) as client:
                    response = client.put(
                        f"{peer}/vote",
                        params={
                            "candidate": candidate,
                            "term": term,
                            "log_index": LOG_INDEX
                        }
                    )

                    if response.json()["vote_for"] == candidate:
                        votes += 1

            except:
                pass

    print("Starting election term", term)

    return votes

def get_alive_nodes():
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

@app.get("/store")
def get_store():
    return store

@app.get("/logs")
def get_logs(after: int = 0):

    return [
        entry
        for entry in LOG_ENTRIES
        if entry["index"] > after
    ]

def catch_up_logs():

    global LOG_INDEX

    try:
        with httpx.Client() as client:

            for peer in PEERS:

                if peer == MY_ADDRESS:
                    continue

                response = client.get(
                    f"{peer}/logs",
                    params={"after": LOG_INDEX}
                )

                entries = response.json()

                if entries:

                    print(
                        "CATCHING UP:",
                        len(entries),
                        "entries"
                    )

                for entry in entries:

                    store[entry["key"]] = entry["value"]

                    write_to_log(
                        entry["index"],
                        entry["key"],
                        entry["value"]
                    )

                    LOG_INDEX = max(
                        LOG_INDEX,
                        entry["index"]
                    )

                break

    except:
        pass

@app.put("/commit/{key}")
def commit(key: str, value: str, index: int):

    global pending_entries
    global LAST_COMMITTED_INDEX
    global LOG_INDEX
    for entry in pending_entries:

        if (
            entry["index"] == index
            and index == LAST_COMMITTED_INDEX + 1
        ):

            store[key] = value

            write_to_log(index, key, value)

            
            # Update LOG_INDEX to the committed index if it's higher than the current LOG_INDEX
            LOG_INDEX = max(
                LOG_INDEX,
                index
            )
            # Remove the entry from pending_entries after committing
            pending_entries.remove(entry)

            print("COMMITTING INDEX:", index)
            LAST_COMMITTED_INDEX = index
            print(
                "COMMITTED:",
                index,
                "LAST:",
                LAST_COMMITTED_INDEX
            )
            return {
                "committed": key,
                "index": index
            }

    return {
    "error": "entry not found",
    "index": index,
    "last_committed": LAST_COMMITTED_INDEX
    }


@app.put("/heartbeat")
def heartbeat(term: int, leader: str, log_index: int):

    global LAST_HEARTBEAT
    global LEADER
    global CURRENT_TERM
    global LEADER_LOG_INDEX

    LAST_HEARTBEAT = time.time()
    LEADER_LOG_INDEX = log_index
    print(
        "MY LOG:", LOG_INDEX,
        "LEADER LOG:", LEADER_LOG_INDEX
    )

    if term >= CURRENT_TERM:
        CURRENT_TERM = term
        LEADER = leader

    return {"ok": True}

def send_heartbeats():

    while True:

        if MY_ADDRESS == LEADER:

            with httpx.Client() as client:

                for peer in PEERS:

                    if peer != MY_ADDRESS:

                        try:
                            client.put(
                                f"{peer}/heartbeat",
                                params={
                                    "term": CURRENT_TERM,
                                    "leader": LEADER,
                                    "log_index": LOG_INDEX
                                }
                            )

                        except:
                            pass

        time.sleep(2)