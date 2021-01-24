import os
import json
import pandas as pd

FILE_ABS_PATH = os.path.dirname(__file__)
ROOT_PATH = os.path.join(FILE_ABS_PATH, '../')
RAW_DATA_FOLDER = os.path.join(ROOT_PATH, 'data/rawData')
INSIGHT_FOLDER = os.path.join(ROOT_PATH, 'data/insight')


class DataService():
    def __init__(self):
        pass

    def read_data_names(self):
        print('insight folder', INSIGHT_FOLDER)
        return [p.split('.')[0].split('_')[-1] for p in os.listdir(INSIGHT_FOLDER)]

    def __get_raw_data_by_name(self, name):
        data_path = os.path.join(RAW_DATA_FOLDER, '{}.csv'.format(name))
        df = pd.read_csv(data_path)
        return df.to_dict('records')

    def __get_insight_by_name(self, name):
        insight_path = os.path.join(INSIGHT_FOLDER, 'insight_graph_{}.json'.format(name))
        with open(insight_path, 'r') as input_file:
            data = json.load(input_file)
            return data

    def get_data_by_name(self, name):
        raw_data = self.__get_raw_data_by_name(name)
        insight_data = self.__get_insight_by_name(name)
        return {
            'raw': raw_data,
            'insight': insight_data
        }