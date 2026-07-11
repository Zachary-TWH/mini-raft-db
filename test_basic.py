#pytest

import httpx

def test_put_and_get():

    httpx.put(
        "http://localhost:8001/put/a",
        params={"value": "100"}
    )

    response = httpx.get(
        "http://localhost:8001/get/a"
    )

    assert response.json() == "100"