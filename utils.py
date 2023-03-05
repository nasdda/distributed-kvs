import builtins
import traceback
import os

current_address = os.getenv('ADDRESS')

# Global variables


def init():
    global view
    view = []
    global kvs
    kvs = {}
    global key_vc
    key_vc = {}  # { key: [vc, addr] }
    global cm  # Causal metadata to be passed
    cm = {}


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
    for addr in view:
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


def print(*objs, **kwargs):
    my_prefix = f'{current_address}: '
    builtins.print(my_prefix, *objs, **kwargs)
