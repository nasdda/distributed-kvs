import requests
import time

 
ports = [8080, 8081, 8082, 8083, 8084]

keys = []

for port in ports:
    resp = requests.get(f'http://localhost:{port}/kvs/data', json={})
    data = resp.json()
    keys.append(set(data.get('keys')))

tot = 0

for k in keys:
    tot += len(k)


print(f'Total: {tot}')

for i in range(len(keys) - 1):
    for j in range(i+1, len(keys)):
        for key in keys[i]:
            assert key not in keys[j], 'key in multiple shards'

print('No duplicate keys')
