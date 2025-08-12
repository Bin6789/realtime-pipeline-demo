import requests
import time

ES_URL = "http://localhost:9200/user_event_logs/_search"
last_timestamp = None

while True:
    query = {
        "size": 10,
        "sort": [{"timestamp": "desc"}],
        "query": {
            "range": {
                "timestamp": {
                    "gte": last_timestamp if last_timestamp else "now-5m"
                }
            }
        }
    }

    res = requests.get(ES_URL, json=query)
    hits = res.json().get("hits", {}).get("hits", [])

    for hit in reversed(hits):  # từ cũ đến mới
        doc = hit["_source"]
        print(f"[{doc['timestamp']}] {doc}")

        last_timestamp = doc["timestamp"]

    time.sleep(2)  # đợi 2 giây rồi truy vấn lại