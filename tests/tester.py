import subprocess
import os
from pathlib import Path
import docker #need to pip install docker
import requests

#docker operations
location = Path(os.getcwd())
# parent_location = location.parent
os.chdir(location.parent)
#print(os.getcwd()) #directory changed while running
subprocess.run('docker network create --subnet=10.10.0.0/16 kv_subnet')
subprocess.run('docker build -t kvs:3.0 .')

client = docker.from_env()
# print(client.containers.list())

# client.containers.run('kvs:3.0'.)

# def create_new_replica(ip, name, port, address):
#     replica = subprocess.Popen("""docker run \
#     --net kv_subnet \
#     --ip {} \
#     --name "{}" \
#     --publish {}:8080 \
#     --env ADDRESS="{}" \
#     kvs:3.0""".format(ip, name, port, address))

ip = 2
kvs_node_name = 1
port_val = 8080
address_val = 2

def create_easy_replica(ip, kvs_node_name, port_val, address_val):
    ip += 1
    kvs_node_name += 1
    port_val += 1
    address_val += 1
    subprocess.Popen("""docker run \
    --net kv_subnet \
    --ip 10.10.0.{} \
    --name "kvs-node-{}" \
    --publish {}:8080 \
    --env ADDRESS="10.10.0.{}:8080" \
    kvs:3.0""".format(ip, kvs_node_name, port_val, address_val))
    return (ip, kvs_node_name, port_val, address_val)

replica1 = subprocess.Popen("""docker run \
--net kv_subnet \
--ip 10.10.0.2 \
--name "kvs-node1" \
--publish 8080:8080 \
--env ADDRESS="10.10.0.2:8080" \
kvs:3.0""")

command = ""
while command != "quit": #type "quit" to get out
    command = input("Type Command Here: ")
    if (command == "list"): #list all containers
        # print(client.containers.list())
        print([container.name for container in client.containers.list()])
    elif (command == "new container"):
        ip, kvs_node_name, port_val, address_val = create_easy_replica(ip, kvs_node_name, port_val, address_val) #create a new replica
    elif "/kvs/admin/view" in command.split() and "put" in command.split(): #PUT admin view
        if len(command.split()) < 4:
            print("Try: put /kvs/admin/view NUM_SHARDS PORT")
            continue
        body = {
        "num_shards": int((command.split())[2]),
        "nodes": ["10.10.0.{}:8080".format(x) for x in range(2,address_val+1)]
        }
        resp = requests.put('http://localhost:{}/kvs/admin/view'.format((command.split())[3]), json=body)
        print(resp.content)
    elif "/kvs/admin/view" in command.split() and "get" in command.split(): #GET admin view
        if len(command.split()) < 3:
            print("Try: get /kvs/admin/view PORT")
            continue
        resp = requests.get('http://localhost:{}/kvs/admin/view'.format((command.split())[2]), json={})
        print(resp.content)
    elif "/kvs/data" in command.split() and "get" in command.split(): #GET kvs data
        if len(command.split()) < 3:
            print("Try: get /kvs/data PORT")
            continue
        resp = requests.get('http://localhost:{}/kvs/data'.format((command.split())[2]), json={})
        print(resp.content)
    elif "/kvs/data" in command.split() and "put" in command.split(): #PUT kvs data
        if len(command.split()) < 5:
            print("NOTE: CAUSAL-METADATA part not properly implemented.")
            print("Try: put /kvs/data PORT KEY VALUE CAUSAL-METADATA")
            continue
        url = 'http://localhost:{}/kvs/data/{}'.format((command.split())[2], (command.split())[3])
        #resp = requests.put(url, json={"val":(command.split())[4], "causal-metadata":(command.split())[5]})
        resp = requests.put(url, json={"val":(command.split())[4]})
        print(resp.content)
    elif "help" in command.split():
        print("Try: put /kvs/admin/view NUM_SHARDS PORT")
        print("Try: get /kvs/admin/view PORT")
        print("Try: get /kvs/data PORT")
        print("NOTE: CAUSAL-METADATA part not properly implemented. for put /kvs/data")
        print("Try: put /kvs/data PORT KEY VALUE CAUSAL-METADATA")
        print("Try: list -> list all containers")
        print("Try: new container -> creates a new container")
        print("Try: quit -> ends testing program")
    else:
        pass

    # print(command.split())




#clean up everything.
containers = client.containers.list()
for container in containers:
    container.remove(force=True) #force remove all containers

subprocess.call('docker system prune', shell=True) #prune all containers and stuff.
