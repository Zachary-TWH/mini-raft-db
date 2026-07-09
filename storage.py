"""
Storage layer: in-memory key-value store + append-only log on disk.

Log entries now carry a `term` (canonical Raft requirement). LOG_ENTRIES is
the full log — committed and uncommitted tail — indexed contiguously from 1.
Disk only ever receives entries once they're committed.
"""

LOG_FILE = "w2.log"
store = {}
LOG_ENTRIES = []       # [{index, term, key, value}], contiguous from index 1
COMMIT_INDEX = 0

# Storage API
def recover_from_log():
    global COMMIT_INDEX

    try:
        with open(LOG_FILE, "r") as f:
            for line in f:
                index, term, key, value = line.strip().split(", ")
                index, term = int(index), int(term)

                store[key] = value
                # it append everything right now
                LOG_ENTRIES.append({
                    "index": index,
                    "term": term,
                    "key": key,
                    "value": value
                })

        COMMIT_INDEX = LOG_ENTRIES[-1]["index"] if LOG_ENTRIES else 0

        print("Recovered:", store)
        print("Recovered COMMIT_INDEX:", COMMIT_INDEX)

    except FileNotFoundError:
        print("No log found, starting empty")


def last_log_index():
    return LOG_ENTRIES[-1]["index"] if LOG_ENTRIES else 0

def last_log_term():
    return LOG_ENTRIES[-1]["term"] if LOG_ENTRIES else 0

def term_at(index):
    if index == 0:
        return 0
    entry = get_entry(index)
    return entry["term"] if entry else 0


def get_entry(index):
    """O(1) lookup assuming LOG_ENTRIES stays contiguous from index 1."""
    pos = index - 1
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