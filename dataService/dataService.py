import os
import json
import pandas as pd

FILE_ABS_PATH = os.path.dirname(__file__)
ROOT_PATH = os.path.join(FILE_ABS_PATH, '../')
DATA_PATH = os.path.join(ROOT_PATH, 'data')
RAW_DATA_FOLDER = os.path.join(ROOT_PATH, 'data/rawData')
INSIGHT_FOLDER = os.path.join(ROOT_PATH, 'data/insight')


class DataService():
    def __init__(self):
        pass

    def read_data_names(self):
        print('insight folder', INSIGHT_FOLDER)
        return [p.split('.')[0].split('_')[-1] for p in os.listdir(INSIGHT_FOLDER)]

    def __get_raw_data_by_name(self, name):
        data_path = os.path.join(DATA_PATH, 'record_{}.csv'.format(name))
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


    def get_subspace_count_for_record_by_name(self, name):
        sid_cid_path = os.path.join(DATA_PATH, 'sid_cid_{}.csv'.format(name))
        sid_cid_df = pd.read_csv(sid_cid_path)
        iid_sid_path = os.path.join(DATA_PATH, 'iid_sid_{}.csv'.format(name))
        iid_sid_df = pd.read_csv(iid_sid_path)
        iid_cid_df = pd.merge(sid_cid_df, iid_sid_df, on='sid', how='inner')
        iid_cid_df = iid_cid_df.groupby('cid')['iid'].apply(list).reset_index(name='iids')
        iid_cid_df['iid_count'] = [len(id_list) for id_list in iid_cid_df['iids']]
        iid_cid_df.sort_values(by='iid_count', inplace=True, ascending=False)
        iid_cid_df.reset_index(inplace=True, drop=True)
        res = iid_cid_df.to_dict('index')
        print(res)
        return res
