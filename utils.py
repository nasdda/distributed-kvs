import builtins
import traceback
import os
import hashlib

current_address = os.getenv('ADDRESS')

# Global variables


def init():
    global kvs
    kvs = {}
    global key_vc
    key_vc = {}  # { key: [vc, addr] }
    global cm  # Causal metadata to be passed
    cm = {}

    # Assignment 4
    global nodes  # All nodes in cluster
    nodes = []
    global shards  # Shards mapped to their nodes
    shards = []  # shards[shard_id] = [addr]
    global current_shard_id
    current_shard_id = -1


# Paths
ADMIN_VIEW_PATH = '/kvs/admin/view'
KVS_DATA_PATH = '/kvs/data'


def format_url(address, path):
    return f'http://{address}{path}'


# Results for vc comparison
SMALLER = 'SMALLER'
GREATER = 'GREATER'
CONCURRENT = 'CONCURRENT'
EQUAL = 'EQUAL'


def get_vc_order(vc_a, vc_b):
    ''' Returns how vc_a compares to vc_b '''
    smaller = greater = False
    for addr in shards[current_shard_id]:
        if vc_a[addr] > vc_b[addr]:
            greater = True
        if vc_a[addr] < vc_b[addr]:
            smaller = True
    if smaller and greater:
        return CONCURRENT
    if smaller:
        return SMALLER
    # Either greater or equal, either way can continue
    return GREATER


def init_vc():
    res = {}
    for addr in shards[current_shard_id]:
        res[addr] = 0
    return res


def key_hash(key):
    '''
    To ensure that the hash value is the same across nodes.
    Reference: https://docs.python.org/3.5/library/hashlib.html?highlight=hashlib
    '''
    seed = b'4ced03dfa048945273f61b62b363db9f'  # Same seed every time
    hash_func = hashlib.sha256(seed)
    hash_func.update(key.encode('utf-8'))
    digest_bytes = hash_func.digest()
    hash_value = int.from_bytes(digest_bytes, byteorder='big')
    return hash_value


def print(*objs, **kwargs):
    my_prefix = f'{current_address}: '
    builtins.print(my_prefix, *objs, **kwargs)
