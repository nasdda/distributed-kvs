import requests
import time

 
ports = [8080, 8081, 8082, 8083, 8084]

def get_counts(shards):
    tot = 0
    for port in ports:
        if shards <= 0:
            break
        resp = requests.get(f'http://localhost:{port}/kvs/data', json={})
        data = resp.json()
        print(data.get('count'))
        tot += int(data.get('count'))
        shards -= 1
    print(f'total: {tot}')


stall = 5
for shards in range(5, 0, -1):
    print(f'view change, Shards: {shards}')
    body = {
        "num_shards": shards,
        "nodes": ["10.10.0.2:8080", "10.10.0.3:8080", "10.10.0.4:8080", "10.10.0.5:8080", "10.10.0.6:8080"]
    }
    resp = requests.put('http://localhost:8080/kvs/admin/view', json=body)
    get_counts(shards)
    if stall > 0:
        time.sleep(3)
    stall -= 1