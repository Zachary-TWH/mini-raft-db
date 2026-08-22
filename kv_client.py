
import httpx


class KVClient:
    """
    Thin client for the mini-Raft KV store. Tries each known node in turn
    until one responds successfully — callers don't need to track which
    node is currently leader.

    Note on /put: non-leader nodes forward internally to the leader but
    (as of the current node.py) always return HTTP 200 to the caller,
    even if the leader's actual response was an error. So for put we
    can't trust status_code alone — we also check the response body for
    a "stored" key to confirm real success.
    """

    def __init__(self, node_addresses, timeout=5.0):
        self.nodes = list(node_addresses)
        self.timeout = timeout
        self._last_working_node = None

    def _candidates(self):
        ordered = list(self.nodes)
        if self._last_working_node in ordered:
            ordered.remove(self._last_working_node)
            ordered.insert(0, self._last_working_node)
        return ordered

    def put(self, key, value):
        last_error = None
        for node in self._candidates():
            try:
                r = httpx.put(f"{node}/put/{key}", params={"value": value}, timeout=self.timeout)
                body = r.json()
                if r.status_code == 200 and "stored" in body:
                    self._last_working_node = node
                    return body
                last_error = f"{node}: {r.status_code} {body}"
            except Exception as e:
                last_error = f"{node}: {e}"
        raise RuntimeError(f"put failed on all nodes. Last error: {last_error}")

    def get(self, key):
        return self._request("GET", f"get/{key}", {})

    def add_node(self, address):
        return self._request("PUT", "add_node", {"address": address})

    def remove_node(self, address):
        return self._request("PUT", "remove_node", {"address": address})

    def whoami(self, node):
        r = httpx.get(f"{node}/whoami", timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def _request(self, method, path, params):
        last_error = None
        for node in self._candidates():
            try:
                url = f"{node}/{path}"
                r = httpx.get(url, params=params, timeout=self.timeout) if method == "GET" \
                    else httpx.put(url, params=params, timeout=self.timeout)

                if r.status_code == 200:
                    self._last_working_node = node
                    return r.json()
                if r.status_code == 404:
                    r.raise_for_status()  # key genuinely missing — leader confirmed it, don't retry
                last_error = f"{node}: {r.status_code} {r.text}"
            except httpx.HTTPStatusError:
                raise
            except Exception as e:
                last_error = f"{node}: {e}"

        raise RuntimeError(f"Request failed on all nodes. Last error: {last_error}")