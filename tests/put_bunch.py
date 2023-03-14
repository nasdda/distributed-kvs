import requests

import string
import random


for _ in range(1000):
    port = random.randint(8080, 8084)
    N = random.randint(2, 10)
    key = ''.join(random.choices(string.ascii_lowercase + string.digits, k=N))
    url = f'http://localhost:{port}/kvs/data/{key}'
    requests.put(url, json={"val": "placeholder value"})


