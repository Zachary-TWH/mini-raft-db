#WIP

from kv_client import KVClient

client = KVClient([f"http://localhost:800{i}" for i in range(1, 7)])
client.put("x", "1")
print(client.get("x"))
