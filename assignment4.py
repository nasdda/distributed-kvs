from flask import Flask, request
import os
import utils

address = os.getenv('ADDRESS')

if __name__ == '__main__':
    if not address: 
        # Address not supplied, exit with code 1
        exit(1)
    utils.init()
    from waitress import serve
    port = os.getenv('PORT')
    port = port if port != None else 8080
    from replica_server import app
    print(f'{address}: Server listening on port {port}')
    serve(app, host='0.0.0.0', port=port) 