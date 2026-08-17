import httpx
import time

NODE = {
    1: "http://localhost:8001",
    2: "http://localhost:8002",
    3: "http://localhost:8003",
    4: "http://localhost:8004",
    5: "http://localhost:8005",
    6: "http://localhost:8006",
}

def find_leader():
    for i in range(1, 6):
        try:
            r = httpx.get(f"{NODE[i]}/debug/status", timeout=2.0)
            data = r.json()
            if data["leader"]:
                leader_url = data["leader"]
                for port, url in NODE.items():
                    node_internal = f"http://node{port}:8000"
                    if node_internal == leader_url:
                        return NODE[port]
        except Exception:
            continue
    raise RuntimeError("No leader found")


def test_put_and_get():
    leader = find_leader()
    httpx.put(f"{leader}/put/test_a", params={"value": "100"})
    r = httpx.get(f"{leader}/get/test_a")
    assert r.json() == "100"


def test_add_node_joins_and_catches_up():
    leader = find_leader()

    r = httpx.put(f"{leader}/add_node", params={"address": "http://node6:8000"})
    assert r.status_code == 200
    assert "http://node6:8000" in r.json()["new_config"]

    deadline = time.time() + 10
    while time.time() < deadline:
        store = httpx.get(f"{NODE[6]}/store").json()
        if store.get("test_b") == "200":
            break
        time.sleep(0.5)
    else:
        assert False, "node6 never caught up"


def test_remove_node_shrinks_quorum():
    leader = find_leader()
    r = httpx.put(f"{leader}/remove_node", params={"address": "http://node6:8000"})
    assert r.status_code == 200
    assert "http://node6:8000" not in r.json()["new_config"]


def test_removed_node_cannot_win_vote():
    # node6 should still be alive (process running) but excluded from config
    r = httpx.get(f"{NODE[6]}/debug/status")
    data = r.json()
    assert data["state"] == "follower"


def test_data_survives_across_removed_node():
    leader = find_leader()
    r = httpx.get(f"{leader}/get/test_b")
    assert r.json() == "200"