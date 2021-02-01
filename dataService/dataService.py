import os
import json
import pandas as pd

FILE_ABS_PATH = os.path.dirname(__file__)
ROOT_PATH = os.path.join(FILE_ABS_PATH, '../')
DATA_PATH = os.path.join(ROOT_PATH, 'data')
RAW_DATA_FOLDER = os.path.join(ROOT_PATH, 'data/rawData')
INSIGHT_FOLDER = os.path.join(ROOT_PATH, 'data/insight')
SID_CID_FOLDER = os.path.join(ROOT_PATH, 'data/sid_cid')


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
        sid_cid_path = os.path.join(SID_CID_FOLDER, 'sid_cid_{}.csv'.format(name))
        sid_cid_df = pd.read_csv(sid_cid_path)
        insight_data = self.__get_insight_by_name(name)
        iids_df = pd.merge(insight_data, sid_cid_df, on='sid', how='inner')
        iids_df = iids_df.groupby('cid')['iid'].apply(list).reset_index(name='iids')
        iids_df['iid_count'] = [len(id_list) for id_list in iids_df['iids']]
        iids_df.sort_values(by='iid_count', inplace=True, ascending=False)
        iids_df.reset_index(inplace=True, drop=True)
        res = iids_df.to_dict('index')
        print(res)
        return res
