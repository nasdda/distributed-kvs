from flask import Flask, request
import requests
import os
from utils import *
from view_operations import view_ops
from data_operations import data_ops

app = Flask(__name__)

app.register_blueprint(view_ops)
app.register_blueprint(data_ops)


@app.route('/')
def index():
    print('GET /')
    return '<h1>At Index</h1>'


