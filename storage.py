
import json
import os

LOG_FILE = "w2.log"
store = {}
LOG_ENTRIES = []       # [{index, term, key, value}], contiguous from index 1
COMMIT_INDEX = 0
SNAPSHOT_FILE = "snapshot.json"
LAST_INCLUDED_INDEX = 0
LAST_INCLUDED_TERM = 0

# Storage API
def recover_from_log():
    global COMMIT_INDEX, LAST_INCLUDED_INDEX, LAST_INCLUDED_TERM

    # Step 1: load snapshot first, if it exists
    if os.path.exists(SNAPSHOT_FILE):
        with open(SNAPSHOT_FILE, "r") as f:
            snapshot_data = json.load(f)

        store.update(snapshot_data["store"])
        LAST_INCLUDED_INDEX = snapshot_data["last_included_index"]
        LAST_INCLUDED_TERM = snapshot_data["last_included_term"]
        COMMIT_INDEX = LAST_INCLUDED_INDEX

        print("Recovered from snapshot at index", LAST_INCLUDED_INDEX)

    # Step 2: replay whatever's left in the log (entries after the snapshot point)
    try:
        with open(LOG_FILE, "r") as f:
            for line in f:
                index, term, key, value = line.strip().split(", ")
                index, term = int(index), int(term)

                store[key] = value
                LOG_ENTRIES.append({
                    "index": index,
                    "term": term,
                    "key": key,
                    "value": value
                })

        if LOG_ENTRIES:
            COMMIT_INDEX = LOG_ENTRIES[-1]["index"]

        print("Recovered:", store)
        print("Recovered COMMIT_INDEX:", COMMIT_INDEX)

    except FileNotFoundError:
        print("No log found, starting empty")

def last_log_index():
    if LOG_ENTRIES:
        return LOG_ENTRIES[-1]["index"]
    return LAST_INCLUDED_INDEX

def last_log_term():
    if LOG_ENTRIES:
        return LOG_ENTRIES[-1]["term"]
    return LAST_INCLUDED_TERM

def term_at(index):
    if index == 0:
        return 0
    if index == LAST_INCLUDED_INDEX:
        return LAST_INCLUDED_TERM
    entry = get_entry(index)
    return entry["term"] if entry else 0

def get_entry(index):
    pos = index - 1 - LAST_INCLUDED_INDEX
    if 0 <= pos < len(LOG_ENTRIES) and LOG_ENTRIES[pos]["index"] == index:
        return LOG_ENTRIES[pos]
    return None

def append_entry(term, key, value):
    """Leader-only: append a new uncommitted entry, return it."""
    entry = {
        "index": last_log_index() + 1,
        "term": term,
        "key": key,
        "value": value
    }
    LOG_ENTRIES.append(entry)
    return entry

def append_entries_from_leader(prev_log_index, prev_log_term, entries, leader_commit):
    """
    Follower-side AppendEntries handling.
    Returns False (leader should back off nextIndex and retry) if our log
    doesn't agree with the leader's at prev_log_index.
    """
    if prev_log_index > 0 and term_at(prev_log_index) != prev_log_term:
        return False

    for entry in entries:
        existing = get_entry(entry["index"])
        # if existing data and its term don't match, delete everything from that index onwards
        if existing and existing["term"] != entry["term"]:

            del LOG_ENTRIES [entry["index"] - 1:]

            existing = None
        # and append new entry
        if not existing:
            LOG_ENTRIES.append(entry)

    if leader_commit > COMMIT_INDEX:
        # apply the new entries to the store, up to the minimum of leader_commit and last_log_index, and update COMMIT_INDEX accordingly
        apply_committed(min(leader_commit, last_log_index()))

    return True

def apply_committed(index):
    """Advance COMMIT_INDEX up to `index`, applying entries to store + disk."""
    global COMMIT_INDEX

    for i in range(COMMIT_INDEX + 1, index + 1):
        entry = get_entry(i)
        if entry is None:
            break

        store[entry["key"]] = entry["value"]

        with open(LOG_FILE, "a") as f:
            f.write(f"{entry['index']}, {entry['term']}, {entry['key']}, {entry['value']}\n")

        COMMIT_INDEX = i
        print("COMMITTED:", i)
        if COMMIT_INDEX % 5 == 0:
            take_snapshot()        

def take_snapshot():

    #Serialize current store + COMMIT_INDEX to disk, then truncate the log
    #up to COMMIT_INDEX. Auto-triggered from apply_committed every 5 commits.#"""
    global LOG_ENTRIES, LAST_INCLUDED_INDEX, LAST_INCLUDED_TERM

    snapshot_data = {
        "store": store,
        "last_included_index": COMMIT_INDEX,
        "last_included_term": term_at(COMMIT_INDEX)
    }

    # Write safely: temp file first, then atomic rename
    tmp_file = SNAPSHOT_FILE + ".tmp"
    with open(tmp_file, "w") as f:
        json.dump(snapshot_data, f)
    os.replace(tmp_file, SNAPSHOT_FILE)

    LAST_INCLUDED_INDEX = COMMIT_INDEX
    LAST_INCLUDED_TERM = term_at(COMMIT_INDEX)

    # Truncate in-memory log: keep only entries after the snapshot point
    LOG_ENTRIES[:] = [e for e in LOG_ENTRIES if e["index"] > LAST_INCLUDED_INDEX]

    # Rewrite the on-disk log to match
    with open(LOG_FILE, "w") as f:
        for entry in LOG_ENTRIES:
            f.write(f"{entry['index']}, {entry['term']}, {entry['key']}, {entry['value']}\n")

    print("SNAPSHOT TAKEN at index", LAST_INCLUDED_INDEX)



def install_snapshot(last_included_index, last_included_term, incoming_store):
    """
    Follower-side: overwrite our own state with a snapshot the leader sent,
    because we've fallen too far behind for normal AppendEntries to help
    (the entries we need were already truncated on the leader's side).
    """
    global LOG_ENTRIES, COMMIT_INDEX, LAST_INCLUDED_INDEX, LAST_INCLUDED_TERM

    if last_included_index <= LAST_INCLUDED_INDEX:
        # We already have this snapshot (or a newer one) — nothing to do.
        return

    store.clear()
    store.update(incoming_store)

    LOG_ENTRIES[:] = [e for e in LOG_ENTRIES if e["index"] > last_included_index]

    LAST_INCLUDED_INDEX = last_included_index
    LAST_INCLUDED_TERM = last_included_term
    COMMIT_INDEX = max(COMMIT_INDEX, last_included_index)

    tmp_file = SNAPSHOT_FILE + ".tmp"
    with open(tmp_file, "w") as f:
        json.dump({
            "store": store,
            "last_included_index": last_included_index,
            "last_included_term": last_included_term
        }, f)
    os.replace(tmp_file, SNAPSHOT_FILE)

    with open(LOG_FILE, "w") as f:
        for entry in LOG_ENTRIES:
            f.write(f"{entry['index']}, {entry['term']}, {entry['key']}, {entry['value']}\n")

    print("INSTALLED SNAPSHOT at index", last_included_index)