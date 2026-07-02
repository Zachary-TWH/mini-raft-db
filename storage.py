"""
Storage layer: in-memory key-value store + append-only log on disk.

Owns: store, LOG_INDEX, LOG_ENTRIES, pending_entries, LAST_COMMITTED_INDEX.
Nothing in this module knows about HTTP, peers, or Raft elections.
"""

LOG_FILE = "w2.log"
store = {}
LOG_INDEX = 0
LOG_ENTRIES = []
pending_entries = []
LAST_COMMITTED_INDEX = 0


def recover_from_log():
    """Rebuild `store` and `LOG_ENTRIES` from disk on startup."""
    global LOG_INDEX
    global LAST_COMMITTED_INDEX

    try:
        with open(LOG_FILE, "r") as f:
            for line in f:
                index, key, value = line.strip().split(", ")
                index = int(index)

                store[key] = value

                LOG_ENTRIES.append({
                    "index": index,
                    "key": key,
                    "value": value
                })

                LOG_INDEX = max(LOG_INDEX, index)

        # Everything already on disk is committed
        LAST_COMMITTED_INDEX = LOG_INDEX

        print("Recovered:", store)
        print("Recovered LOG_INDEX:", LOG_INDEX)
        print("Recovered LAST_COMMITTED_INDEX:", LAST_COMMITTED_INDEX)

    except FileNotFoundError:
        print("No log found, starting empty")


def write_to_log(index, key, value):
    """Append a committed entry to the in-memory log and to disk."""
    entry = {
        "index": index,
        "key": key,
        "value": value
    }

    LOG_ENTRIES.append(entry)

    with open(LOG_FILE, "a") as f:
        f.write(f"{index}, {key}, {value}\n")


def bump_log_index(new_index):
    """Raise LOG_INDEX to new_index if it's higher than the current value."""
    global LOG_INDEX
    LOG_INDEX = max(LOG_INDEX, new_index)


def next_log_index():
    """Increment and return LOG_INDEX (used by the leader when accepting a write)."""
    global LOG_INDEX
    LOG_INDEX += 1
    return LOG_INDEX


def commit_entry(key, value, index):
    """
    Apply a pending entry to the store if it's the next one expected
    in commit order. Returns the committed entry dict, or None if no
    matching pending entry was found.
    """
    global LAST_COMMITTED_INDEX

    for entry in pending_entries:
        if entry["index"] == index and index == LAST_COMMITTED_INDEX + 1:
            store[key] = value
            write_to_log(index, key, value)
            bump_log_index(index)

            pending_entries.remove(entry)

            LAST_COMMITTED_INDEX = index
            print("COMMITTING INDEX:", index)
            print("COMMITTED:", index, "LAST:", LAST_COMMITTED_INDEX)

            return entry

    return None