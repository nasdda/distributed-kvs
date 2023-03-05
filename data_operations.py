from flask import Flask, request, Blueprint
from utils import *
import utils
import requests
import time
from collections import defaultdict

import atexit

from copy import deepcopy

# APScheduler reference: https://stackoverflow.com/questions/21214270/how-to-schedule-a-function-to-run-every-hour-on-flask
from apscheduler.schedulers.background import BackgroundScheduler


data_ops = Blueprint('data_operations', __name__, template_folder='templates')

scheduler = BackgroundScheduler()
# Shut down the gossip scheduler when exiting the app
atexit.register(lambda: scheduler.shutdown())


def is_uninitialized():
    return len(utils.view) == 0


def gossip_job():
    ''' Gossips the current kvs and key_vc state to other nodes '''
    sync_body = {
        'kvs': utils.kvs,
        'key_vc': utils.key_vc,
        'causal-metadata': utils.cm
    }
    for addr in utils.view:
        if addr == current_address:
            continue
        try:
            requests.put(format_url(addr, KVS_DATA_PATH +
                                    '/sync'), json=sync_body, timeout=0.01)
        except Exception as _:
            pass


# Gossips with an interval of every 1 second
scheduler.add_job(func=gossip_job, trigger="interval", seconds=1)
scheduler.start()


def init_vc():
    res = {}
    for addr in utils.view:
        res[addr] = 0
    return res


def merge_cm(incoming_cm):
    ''' Merges the incoming causal-metadata with the local '''
    for key, vc in incoming_cm.items():
        if key not in utils.cm:
            utils.cm[key] = init_vc()
        for addr in utils.view:
            utils.cm[key][addr] = max(utils.cm[key][addr], vc[addr])


def stall_for_consistency(keys, req_vcs):
    # Ensure causality for all of the given keys
    seconds_stalled = 0
    for key in keys:
        k_vc = req_vcs.get(key, init_vc())
        if key not in utils.key_vc:
            utils.key_vc[key] = (init_vc(), current_address)
        while get_vc_order(utils.key_vc[key][0], k_vc) != GREATER:
            if seconds_stalled == 20:
                return False
            # Stall while current vc is strictly less than req_vc
            time.sleep(1)
            seconds_stalled += 1
    return True


@data_ops.route(KVS_DATA_PATH + '/sync', methods=['PUT'])
def put_kvs_data_sync():
    '''
    Syncs current state with incoming data.
    '''
    # print('PUT /kvs/data/sync')
    data = request.get_json()
    req_kvs = data.get('kvs')
    req_key_vc = data.get('key_vc')
    req_cm = data.get('causal-metadata')
    for key, (req_vc, req_addr) in req_key_vc.items():
        delete = key not in req_kvs # Delete operation if key is not in req_kvs
        val = req_kvs.get(key, None)
        # Determine order or tie break, then apply update if necessary
        (current_key_vc, current_key_addr) = utils.key_vc.get(
            key, (init_vc(), current_address))
        # Current vc v.s. request vc
        order = get_vc_order(current_key_vc, req_vc)
        if order != GREATER:
            print(f'key:{key}, val:{val}, order:{order}')
        # Tie break using address.
        tiebreak_cond = order == CONCURRENT and current_key_addr < req_addr
        new_addr = current_key_addr
        if order == CONCURRENT:
            # If concurrent, new origin address is max of the current key address or incoming
            new_addr = max(current_key_addr, req_addr)
        if order == SMALLER or tiebreak_cond:
            if delete:
                if key in utils.kvs:
                    del utils.kvs[key]
            else:
                utils.kvs[key] = val
        # Update vc to take max of incoming vc or current
        for addr in utils.view:
            current_key_vc[addr] = max(current_key_vc[addr], req_vc[addr])
        utils.key_vc[key] = deepcopy((current_key_vc, new_addr))
    # Update causal metadata after sync
    merge_cm(req_cm)

    return {'message': 'OK'}, 200


@data_ops.route(KVS_DATA_PATH + '/<key>', methods=['PUT'])
def put_kvs_data_key(key):
    print(f'PUT /kvs/data/{key}')
    if is_uninitialized():
        return {"error": "uninitialized"}, 418
    data = request.get_json()
    causal_metadata = data.get('causal-metadata')
    if causal_metadata == None:
        causal_metadata = {}
    merge_cm(causal_metadata)
    val = data.get('val')
    if val == None:  # Does not contain required information
        return {"error": "bad request"}, 400
    # Max val size is 8MB, 1 char = 1 byte
    if len(val) > 8000000:
        return {'error': 'val too large'}, 400
    code = 200 if key in utils.kvs else 201
    utils.kvs[key] = val
    if key not in utils.cm:
        utils.cm[key] = init_vc()
    utils.cm[key][current_address] += 1
    # The local causal-metadata is merged with the incoming at this point,
    # and the new vector clock for the key is now 1 count ahead for the current node
    utils.key_vc[key] = deepcopy((utils.cm[key], current_address))
    ret_body = {
        'causal-metadata': utils.cm
    }
    return ret_body, code


@data_ops.route(KVS_DATA_PATH + '/<key>', methods=['GET'])
def get_kvs_data_key(key):
    print(f'GET /kvs/data/{key}')
    if is_uninitialized():
        return {"error": "uninitialized"}, 418
    data = request.get_json()
    causal_metadata = data.get('causal-metadata')
    if causal_metadata == None:
        causal_metadata = {}
    # Stall until the current vc for the given key is causally consistent with cm
    stall_success = stall_for_consistency([key], causal_metadata)
    merge_cm(causal_metadata)
    if stall_success == False:
        return {"error": "timed out while waiting for depended updates"}, 500
    ret_body = {
        'causal-metadata': utils.cm
    }
    if key not in utils.kvs:
        return ret_body, 404
    ret_body['val'] = utils.kvs[key]
    return ret_body, 200


@data_ops.route(KVS_DATA_PATH + '/<key>', methods=['DELETE'])
def delete_kvs_data_key(key):
    print(f'DELETE /kvs/data/{key}')
    if is_uninitialized():
        return {"error": "uninitialized"}, 418
    data = request.get_json()
    causal_metadata = data.get('causal-metadata')
    if causal_metadata == None:
        causal_metadata = {}
    merge_cm(causal_metadata)
    ret_body = {
        'causal-metadata': utils.cm
    }
    if key not in utils.kvs:
        return ret_body, 404
    del utils.kvs[key]  # Delete entry for given key
    if key not in utils.cm:
        utils.cm[key] = init_vc()
    utils.cm[key][current_address] += 1
    utils.key_vc[key] = deepcopy((utils.cm[key], current_address))
    ret_body = {
        'causal-metadata': utils.cm
    }
    return ret_body, 200


@data_ops.route(KVS_DATA_PATH, methods=['GET'])
def get_kvs_data():
    print('GET /kvs/data')
    if is_uninitialized():
        return {"error": "uninitialized"}, 418
    data = request.get_json()
    causal_metadata = data.get('causal-metadata')
    if causal_metadata == None:
        causal_metadata = {}
    keys = list(causal_metadata.keys())  # All keys in the incoming causal metadata
    # Wait for all dependencies to be satisfied
    stall_success = stall_for_consistency(keys, causal_metadata)
    merge_cm(causal_metadata)
    if stall_success == False:
        return {"error": "timed out while waiting for depended updates"}, 500
    ret_keys = list(utils.kvs.keys()) # Current keys in kvs
    ret_body = {
        'causal-metadata': utils.cm,
        'keys': ret_keys,
        'count': len(ret_keys)
    }
    return ret_body, 200
