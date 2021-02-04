import _thread
import json
from flask_cors import CORS
from flask import Flask, request, jsonify
import os

from dataService.dataService import DataService, cache

dm = DataService()
app = Flask(__name__)
CORS(app)
cache.init_app(app, config={'CACHE_TYPE': 'simple'})

FILE_ABS_PATH = os.path.dirname(__file__)
ROOT_PATH = os.path.join(FILE_ABS_PATH, '../')
INSIGHT_PATH = os.path.join(ROOT_PATH, 'data/insight')


@app.route('/api/get_data_names', methods=['POST'])
def get_folder_name():
    names = dm.read_data_names()
    return json.dumps(names)


@app.route('/api/get_data_by_name', methods=['POST'])
def get_data_by_name():
    params = request.json
    dataName = params['dataName']
    data = dm.get_data_by_name(dataName)
    return json.dumps(data)


@app.route('/api/get_insight_count_for_record', methods=['POST'])
def get_insight_count_for_record():
    params = request.json
    dataName = params['dataName']
    data = dm.get_insight_count_for_record_by_name(dataName)
    return json.dumps(data, sort_keys=False)


@app.route('/api/get_graph_data_by_iid', methods=['POST'])
def get_graph_data_by_iid():
    params = request.json
    iid = params['iid']
    name = params['name']
    data = dm.get_insight_by_iid(iid, name)
    return json.dumps(data, sort_keys=False)


@app.route('/api/get_insight_count_for_subspace', methods=['POST'])
def get_insight_count_for_subspace():
    params = request.json
    dataName = params['dataName']
    data = dm.get_insight_count_for_subspace_by_name(dataName)
    return json.dumps(data, sort_keys=False)

@app.route('/api/get_subspace_count_for_record', methods=['POST'])
def get_subspace_count_for_record():
    params = request.json
    dataName = params['dataName']
    data = dm.get_subspace_count_for_record_by_name(dataName)
    return json.dumps(data, sort_keys=False)



if __name__ == '__main__':
    app.run(debug=True, port=8888)
