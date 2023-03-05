from flask import Flask, request, Blueprint
from utils import *
import utils
import requests
from copy import deepcopy

view_ops = Blueprint('view_operations', __name__, template_folder='templates')


@view_ops.route(ADMIN_VIEW_PATH, methods=['PUT'])
def put_kvs_admin_view():
    print(f'PUT /kvs/admin/view')
    data = request.get_json()
    new_view = data.get('view')
    if not new_view:
        return {"error": "bad request"}, 400

    # Reset removed nodes
    for addr in utils.view:
        if addr in new_view:
            # Node remains in new view
            continue
        try:
            del_url = format_url(addr, ADMIN_VIEW_PATH)
            print(f'Sending DELETE request to {del_url}')
            requests.delete(del_url, timeout=0.01)
        except Exception as _:
            # Doesn't matter if deleted node is down
            pass

    # Remove address counter of removed nodes
    for addr in utils.view:
        if addr not in new_view:
            for k in utils.key_vc.keys():
                del utils.key_vc[k][0][addr]
            for k in utils.cm.keys():
                del utils.cm[k][addr]

    # Update the vector clocks to include new addresses
    for addr in new_view:
        if addr not in utils.view:
            for k in utils.key_vc.keys():
                utils.key_vc[k][0][addr] = 0
            for k in utils.cm.keys():
                utils.cm[k][addr] = 0

    # Update current view
    utils.view = deepcopy(new_view)
    print(f'New view: {utils.view}')

    # Notify other nodes in the new view
    patch_body = {
        'view': utils.view,
        'kvs': utils.kvs,
        'key_vc': utils.key_vc,
        'cm': utils.cm
    }
    for addr in new_view:
        if addr == current_address:
            continue
        url = format_url(addr, ADMIN_VIEW_PATH)
        print(f"Sending PATCH request to {url}")
        try:
            resp = requests.patch(url, json=patch_body, timeout=2)
        except Exception as _:
            pass
    return {'message': 'success'}, 200


@view_ops.route(ADMIN_VIEW_PATH, methods=['PATCH'])
def patch_kvs_admin_view():
    '''Custom route for nodes that are part of the new view'''
    print(f'PATCH /kvs/admin/view')
    data = request.get_json()
    # Get copy of the vector clock and kvs from existing nodes
    view_copy = data.get('view')
    kvs_copy = data.get('kvs')
    key_vc_copy = data.get('key_vc')
    cm_copy = data.get('cm')

    if view_copy == None or kvs_copy == None or key_vc_copy == None or cm_copy == None:
        return {"error": "bad request"}, 400
    utils.view = deepcopy(view_copy)
    utils.kvs = deepcopy(kvs_copy)
    utils.key_vc = deepcopy(key_vc_copy)
    utils.cm = deepcopy(cm_copy)
    return {'message': 'success'}, 200


@view_ops.route(ADMIN_VIEW_PATH, methods=['GET'])
def get_kvs_admin_view():
    print('GET /kvs/admin/view')
    ret_body = {
        'view': utils.view
    }
    return ret_body, 200


@view_ops.route(ADMIN_VIEW_PATH, methods=['DELETE'])
def delete_kvs_admin_view():
    print('DELETE /kvs/admin/view')
    # Reset kvs, and vector clock
    utils.view.clear()
    utils.kvs.clear()
    utils.key_vc.clear()
    utils.cm.clear()
    return {'message': 'success'}, 200
