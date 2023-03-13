from flask import Flask, request, Blueprint, Response
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
    return len(utils.nodes) == 0


def gossip_job():
    ''' Gossips the current kvs and key_vc state to other nodes '''
    if is_uninitialized():
        return
    sync_body = {
        'kvs': utils.kvs,
        'key_vc': utils.key_vc,
        'causal-metadata': {
            'cm': utils.cm,
            'ver': utils.view_version
        }
    }
    for addr in utils.shards[utils.current_shard_id]:
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


def key_belongs_to_shard(key):
    shard_id = key_hash(key) % len(utils.shards)
    return shard_id == utils.current_shard_id


def merge_cm(incoming_cm):
    ''' Merges the incoming causal-metadata with the local '''
    for key, vc in incoming_cm.items():
        shard_id = key_hash(key) % len(utils.shards)
        if key not in utils.cm:
            utils.cm[key] = init_vc(keys=utils.shards[shard_id])
        for addr in utils.shards[shard_id]:
            utils.cm[key][addr] = max(utils.cm[key][addr], vc[addr])


def stall_for_consistency(keys, req_vcs, max_stall=20):
    # Ensure causality for all of the given keys
    seconds_stalled = 0
    for key in keys:
        if not key_belongs_to_shard(key):
            continue
        k_vc = req_vcs.get(key, init_vc())
        if key not in utils.key_vc:
            utils.key_vc[key] = (init_vc(), current_address)
        while get_vc_order(utils.key_vc[key][0], k_vc) != GREATER:
            if seconds_stalled == max_stall:
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
    causal_metadata = data.get('causal-metadata')
    if causal_metadata['ver'] < utils.view_version:
        causal_metadata['cm'].clear()
    req_cm = causal_metadata['cm']
    for key, (req_vc, req_addr) in req_key_vc.items():
        delete = key not in req_kvs  # Delete operation if key is not in req_kvs
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
        for addr in utils.shards[utils.current_shard_id]:
            current_key_vc[addr] = max(current_key_vc[addr], req_vc[addr])
        utils.key_vc[key] = deepcopy((current_key_vc, new_addr))
    # Update causal metadata after sync
    merge_cm(req_cm)

    return {'message': 'OK'}, 200


def redirect_to_host(fwd_address, req, timeout_seconds=1):
    # Host URL to forward the requests to
    fwd_host = f'http://{fwd_address}/'
    fwd_headers = {k: v for k, v in req.headers}
    # Update the host header
    fwd_headers['Host'] = fwd_address
    print(f'FORWARDING TO: {req.url.replace(req.host_url, fwd_host)}')
    fwd_json = req.get_json()
    fwd_json['proxied'] = True  # For GET requests
    res = requests.request(
        method=req.method,
        url=req.url.replace(req.host_url, fwd_host),
        headers=fwd_headers,
        json=fwd_json,
        cookies=req.cookies,
        allow_redirects=False,
        timeout=timeout_seconds
    )
    # Exclude all "hop-by-hop headers"
    excluded_headers = ['content-encoding',
                        'content-length', 'transfer-encoding', 'connection']
    headers = [
        (k, v) for k, v in res.raw.headers.items()
        if k.lower() not in excluded_headers
    ]

    response = Response(res.content, res.status_code, headers)
    return response


@data_ops.route(KVS_DATA_PATH + '/<key>', methods=['PUT'])
def put_kvs_data_key(key):
    print(f'PUT /kvs/data/{key}')
    if is_uninitialized():
        return {"error": "uninitialized"}, 418

    shard_id = key_hash(key) % len(utils.shards)  # Shard ID of given key
    print(f'key: {key}, shard_id: {shard_id}')
    if shard_id != utils.current_shard_id:
        # 20 seconds to attempt all shard nodes
        timeout_seconds = max(20.0 / len(utils.shards), 0.1)
        # Forward to a node in the associated shard
        for node in utils.shards[shard_id]:
            try:
                resp = redirect_to_host(
                    node, request, timeout_seconds=timeout_seconds)
                return resp
            except Exception as e:
                print(e)
                # Request to upstream timed out or can't connect
                pass
        # Attempted all nodes in shard after 20 seconds
        return {
            "error": "upstream down",
            "upstream": {
                "shard_id": str(shard_id),
                "nodes": utils.shards[shard_id]
            }
        }, 503

    # Key belongs to current shard, handle the request
    print(f'key: {key}, shard_id: {utils.current_shard_id}')
    data = request.get_json()

    causal_metadata = data.get('causal-metadata')
    if not causal_metadata:
        causal_metadata = {'cm': {}, 'ver': utils.view_version}
    if causal_metadata['ver'] < utils.view_version:
        causal_metadata['cm'].clear()
    causal_metadata = causal_metadata['cm']

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
        'causal-metadata': {
            'cm': utils.cm,
            'ver': utils.view_version
        }
    }
    return ret_body, code


@data_ops.route(KVS_DATA_PATH + '/<key>', methods=['GET'])
def get_kvs_data_key(key):
    print(f'GET /kvs/data/{key}')
    if is_uninitialized():
        return {"error": "uninitialized"}, 418

    shard_id = key_hash(key) % len(utils.shards)  # Shard ID of given key
    if shard_id != utils.current_shard_id:
        start = time.time()
        stalled_deps = False
        stalled_resp = {}
        # Forward to a node in the associated shard
        while time.time() - start < 20:
            for node in utils.shards[shard_id]:
                try:
                    resp = redirect_to_host(
                        node, request, timeout_seconds=1)
                    if resp.status_code == 500:  # Waiting for deps
                        stalled_deps = True
                        stalled_resp = deepcopy(resp.get_json())
                    else:
                        return resp
                except Exception as _:
                    pass
        if stalled_deps:
            return stalled_resp, 500
        # Attempted all nodes in shard after 20 seconds
        return {
            "error": "upstream down",
            "upstream": {
                "shard_id": str(shard_id),
                "nodes": utils.shards[shard_id]
            }
        }, 503

    data = request.get_json()

    causal_metadata = data.get('causal-metadata')
    if not causal_metadata:
        causal_metadata = {'cm': {}, 'ver': utils.view_version}
    if causal_metadata['ver'] < utils.view_version:
        causal_metadata['cm'].clear()
    causal_metadata = causal_metadata['cm']

    proxied = data.get('proxied')

    # Stall until the current vc for the given key is causally consistent with cm
    stall_success = stall_for_consistency(
        [key], causal_metadata, max_stall=(0 if proxied else 20))
    merge_cm(causal_metadata)
    if stall_success == False:
        return {"error": "timed out while waiting for depended updates"}, 500
    ret_body = {
        'causal-metadata': {
            'cm': utils.cm,
            'ver': utils.view_version
        }
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

    shard_id = key_hash(key) % len(utils.shards)
    if shard_id != utils.current_shard_id:
        timeout_seconds = max(20.0 / len(utils.shards), 0.1)
        for node in utils.shards[shard_id]:
            try:
                resp = redirect_to_host(
                    node, request, timeout_seconds=timeout_seconds)
                return resp
            except Exception as _:
                pass
        return {
            "error": "upstream down",
            "upstream": {
                "shard_id": str(shard_id),
                "nodes": utils.shards[shard_id]
            }
        }, 503

    data = request.get_json()

    causal_metadata = data.get('causal-metadata')
    if not causal_metadata:
        causal_metadata = {'cm': {}, 'ver': utils.view_version}
    if causal_metadata['ver'] < utils.view_version:
        causal_metadata['cm'].clear()
    causal_metadata = causal_metadata['cm']
    
    merge_cm(causal_metadata)
    ret_body = {
        'causal-metadata': {
            'cm': utils.cm,
            'ver': utils.view_version
        }
    }
    if key not in utils.kvs:
        return ret_body, 404
    del utils.kvs[key]  # Delete entry for given key
    if key not in utils.cm:
        utils.cm[key] = init_vc()
    utils.cm[key][current_address] += 1
    utils.key_vc[key] = deepcopy((utils.cm[key], current_address))
    ret_body = {
        'causal-metadata': {
            'cm': utils.cm,
            'ver': utils.view_version
        }
    }
    return ret_body, 200


@data_ops.route(KVS_DATA_PATH, methods=['GET'])
def get_kvs_data():
    print('GET /kvs/data')
    if is_uninitialized():
        return {"error": "uninitialized"}, 418

    data = request.get_json()
    causal_metadata = data.get('causal-metadata')
    if not causal_metadata:
        causal_metadata = {'cm': {}, 'ver': utils.view_version}
    if causal_metadata['ver'] < utils.view_version:
        causal_metadata['cm'].clear()
    causal_metadata = causal_metadata['cm']
    # All keys in the incoming causal metadata
    keys = list(causal_metadata.keys())
    # Wait for all dependencies to be satisfied
    stall_success = stall_for_consistency(keys, causal_metadata)
    merge_cm(causal_metadata)
    if stall_success == False:
        return {"error": "timed out while waiting for depended updates"}, 500
    ret_keys = list(utils.kvs.keys())  # Current keys in kvs
    ret_body = {
        'shard_id': utils.current_shard_id,
        'causal-metadata': {
            'cm': utils.cm,
            'ver': utils.view_version
        },
        'keys': ret_keys,
        'count': len(ret_keys)
    }
    return ret_body, 200
