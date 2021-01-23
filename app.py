import _thread
import json
from flask_cors import CORS
from flask import Flask, request, jsonify
import os

from lib.dataService import DataService

dm = DataService()
app = Flask(__name__)
CORS(app)

FILE_ABS_PATH = os.path.dirname(__file__)
ROOT_PATH = os.path.join(FILE_ABS_PATH, '../')
INSIGHT_PATH = os.path.join(ROOT_PATH, 'data/insight')

@app.route('/api/get_data_names', methods=['POST'])
def get_folder_name():
    names = dm.read_data_names()
    return json.dumps(names)

if __name__ == '__main__':
    app.run(debug=True, port=8888)
