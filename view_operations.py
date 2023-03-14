from flask import Flask, request, Blueprint
from utils import *
import utils
import requests
from copy import deepcopy

view_ops = Blueprint('view_operations', __name__, template_folder='templates')


def reshard_keys():
    ''' Send each key to the node of its corresponding shard '''
    shard_kv = [{} for _ in range(len(utils.shards))]
    # Determine new shard of each key
    for key, value in utils.kvs.items():
        shard_id = key_hash(key) % len(utils.shards)
        shard_kv[shard_id][key] = value

    for shard_id in range(len(shard_kv)):
        for addr in utils.shards[shard_id]:
            if addr == utils.current_address:
                continue
            put_body = {
                'kvs': shard_kv[shard_id]
            }
            requests.put(format_url(addr, '/kvs/reshard'), json=put_body)

    # Remove keys that do not belong to current shard
    for shard_id in range(len(shard_kv)):
        if shard_id == utils.current_shard_id:
            continue
        for key in shard_kv[shard_id].keys():
            del utils.kvs[key]

    # Reset all vector clocks and causal metedata
    utils.cm.clear()
    for key in utils.key_vc.keys():
        utils.key_vc[key] = deepcopy((init_vc(), '0'))


@view_ops.route(ADMIN_VIEW_PATH, methods=['PUT'])
def put_kvs_admin_view():
    print(f'PUT /kvs/admin/view')
    data = request.get_json()
    new_nodes = data.get('nodes')
    num_shards = data.get('num_shards')
    if new_nodes == None or num_shards == None:
        return {"error": "bad request"}, 400

    # Reset shards
    utils.shards = [[] for _ in range(num_shards)]
    shard_id = 0
    # Assign nodes to new shards
    for node in new_nodes:
        utils.shards[shard_id].append(node)
        if node == current_address:  # Current node's shard
            utils.current_shard_id = shard_id
        shard_id = (shard_id + 1) % num_shards

    # Reset removed nodes
    for addr in utils.nodes:
        if addr in new_nodes:
            # Node remains in new view
            continue
        try:
            del_url = format_url(addr, ADMIN_VIEW_PATH)
            print(f'Sending DELETE request to {del_url}')
            requests.delete(del_url, timeout=0.01)
        except Exception as _:
            # Doesn't matter if deleted node is down
            pass

    # Update current nodes
    utils.nodes = deepcopy(new_nodes)
    print(f'New nodes: {utils.nodes}')

    # Update view version
    utils.view_version += 1

    # Notify other nodes in the new view
    patch_body = {
        'nodes': utils.nodes,
        'shards': utils.shards,
        'ver': utils.view_version
    }
    for addr in new_nodes:
        if addr == current_address:
            continue
        url = format_url(addr, ADMIN_VIEW_PATH)
        print(f"Sending PATCH request to {url}")
        try:
            requests.patch(url, json=patch_body)
        except Exception as _:
            pass
    # Redistribute keys
    reshard_keys()

    return {'message': 'success'}, 200


@view_ops.route(ADMIN_VIEW_PATH, methods=['PATCH'])
def patch_kvs_admin_view():
    '''Custom route for nodes that are part of the new view'''
    print(f'PATCH /kvs/admin/view')
    data = request.get_json()
    # Get copy of the vector clock and kvs from existing nodes
    nodes_copy = data.get('nodes')
    shards_copy = data.get('shards')
    new_ver = data.get('ver')
    if nodes_copy == None or shards_copy == None:
        return {"error": "bad request"}, 400
    utils.nodes.clear()
    for shard_id in range(len(shards_copy)):
        # Update current shard ID
        if current_address in shards_copy[shard_id]:
            utils.current_shard_id = shard_id
    utils.nodes = deepcopy(nodes_copy)
    utils.shards = deepcopy(shards_copy)
    utils.view_version = new_ver

    reshard_keys()

    return {'message': 'success'}, 200


@view_ops.route(ADMIN_VIEW_PATH, methods=['GET'])
def get_kvs_admin_view():
    print('GET /kvs/admin/view')
    view = []
    for shard_id in range(len(utils.shards)):
        view.append({
            'shard_id': str(shard_id),
            'nodes': utils.shards[shard_id]
        })
    ret_body = {
        'view': view
    }
    return ret_body, 200


@view_ops.route(ADMIN_VIEW_PATH, methods=['DELETE'])
def delete_kvs_admin_view():
    print('DELETE /kvs/admin/view')
    # Reset kvs, and vector clock
    utils.nodes.clear()
    utils.shards.clear()
    utils.kvs.clear()
    utils.key_vc.clear()
    utils.cm.clear()
    return {'message': 'success'}, 200


@view_ops.route('/kvs/reshard', methods=['PUT'])
def put_kvs_reshard():
    print('PUT /kvs/reshard')
    data = request.get_json()
    new_kvs = data.get('kvs')

    for key, value in new_kvs.items():
        utils.kvs[key] = value
        utils.key_vc[key] = deepcopy((init_vc(), '0'))

    return {'message': 'success'}, 200
