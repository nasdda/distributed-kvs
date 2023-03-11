import requests

 
ports = [8080, 8081, 8082, 8083, 8084]

def get_counts():
    s = set()
    for port in ports:
        resp = requests.get(f'http://localhost:{port}/kvs/data', json={})
        data = resp.json()
        print(f"{port}: {data.get('count')}")
        s.add(int(data.get('count')))
    print(f'total: {sum(s)}')


get_counts()
print('view change')
body = {
	"num_shards": 3,
	"nodes": ["10.10.0.2:8080", "10.10.0.3:8080", "10.10.0.4:8080", "10.10.0.5:8080", "10.10.0.6:8080"]
}
resp = requests.put('http://localhost:8080/kvs/admin/view', json=body)
get_counts()