import os
import json
import pandas as pd
from flask_caching import Cache

cache = Cache()

FILE_ABS_PATH = os.path.dirname(__file__)
ROOT_PATH = os.path.join(FILE_ABS_PATH, '../')
EDGE_FOLDER = os.path.join(ROOT_PATH, 'data/edge')
INSIGHT_FOLDER = os.path.join(ROOT_PATH, 'data/insight')
RECORD_FOLDER = os.path.join(ROOT_PATH, 'data/record')
SID_CID_FOLDER = os.path.join(ROOT_PATH, 'data/sid_cid')
SUBSPACE_FOLDER = os.path.join(ROOT_PATH, 'data/subspace')


class DataService():
    def __init__(self):
        pass

    def read_data_names(self):
        return [p.split('.')[0].split('_')[-1] for p in os.listdir(INSIGHT_FOLDER)]

    @cache.memoize(timeout=50)
    def __get_edge_by_name(self, name):
        edges_path = os.path.join(EDGE_FOLDER, 'edge_{}.csv'.format(name))
        df = pd.read_csv(edges_path)
        return df

    @cache.memoize(timeout=50)
    def __get_insight_by_name(self, name):
        insight_path = os.path.join(INSIGHT_FOLDER, 'insight_{}.csv'.format(name))
        df = pd.read_csv(insight_path)
        return df, df['insight'].unique().tolist(), df['insight_type'].unique().tolist()

    @cache.memoize(timeout=50)
    def __get_record_by_name(self, name):
        record_path = os.path.join(RECORD_FOLDER, 'record_{}.csv'.format(name))
        df = pd.read_csv(record_path)
        return df

    @cache.memoize(timeout=50)
    def __get_sid_cid_by_name(self, name):
        record_path = os.path.join(SID_CID_FOLDER, 'sid_cid_{}.csv'.format(name))
        df = pd.read_csv(record_path)
        return df

    @cache.memoize(timeout=50)
    def __get_subspace_by_name(self, name):
        subspace_path = os.path.join(SUBSPACE_FOLDER, 'subspace_{}.csv'.format(name))
        df = pd.read_csv(subspace_path)
        return df, df.head(0).columns.values.tolist()[0:-1]

    def get_data_by_name(self, name):
        edge_data = self.__get_edge_by_name(name)
        insight_data, insight_name, insight_type = self.__get_insight_by_name(name)
        record_data = self.__get_record_by_name(name)
        subspace_data, feature_data = self.__get_subspace_by_name(name)

        return {
            'record': record_data.to_dict('records'),
            'insight': insight_data.to_dict('records'),
            'edge': edge_data.to_dict('records'),
            'feature': feature_data,
            'insight_name': insight_name,
            'insight_type': insight_type,
            'subspace': subspace_data.to_dict('records')
        }

    def get_insight_count_for_record_by_name(self, name):
        sid_cid_df = self.__get_sid_cid_by_name(name)
        insight_data, _, _ = self.__get_insight_by_name(name)
        iids_df = pd.merge(insight_data, sid_cid_df, on='sid', how='inner')
        iids_df = iids_df.groupby('cid')['iid'].apply(list).reset_index(name='iids')
        iids_df['iid_count'] = [len(id_list) for id_list in iids_df['iids']]
        iids_df.sort_values(by='iid_count', inplace=True, ascending=False)
        iids_df.reset_index(inplace=True, drop=True)
        res = iids_df.to_dict('index')
        return res

    def get_insight_by_iid(self, iid, name):
        insight_data, insight_name, insight_type = self.__get_insight_by_name(name)
        insight = insight_data.loc[insight_data['iid'] == iid]
        print(insight)
        return 0

    def get_insight_count_for_subspace_by_name(self, name):
        insight_data, _, _ = self.__get_insight_by_name(name)
        iid_sid_df = insight_data[['iid', 'sid']].groupby('sid')['iid'].apply(list).reset_index(name='iids')
        iid_sid_df['iid_count'] = [len(id_list) for id_list in iid_sid_df['iids']]

        subspace_data, _ = self.__get_subspace_by_name(name)
        subspace_data['star_count'] = ""
        for index, row in subspace_data.iterrows():
            if '*' in row.tolist():
                subspace_data['star_count'][index] = row.tolist().count('*')
            else:
                subspace_data['star_count'][index] = 0

        iid_sid_df = subspace_data[['sid', 'star_count']].merge(iid_sid_df, on='sid', how='inner')
        iid_sid_df.sort_values(by=['iid_count', 'star_count'], inplace=True, ascending=False)
        iid_sid_df.reset_index(inplace=True, drop=True)
        res = iid_sid_df.to_dict('index')
        print("get_insight_count_for_subspace_by_name")
        print(iid_sid_df)
        return res

    def get_subspace_count_for_record_by_name(self, name):
        sid_cid_df = self.__get_sid_cid_by_name(name)
        insight_data, _, _ = self.__get_insight_by_name(name)
        iids_df = pd.merge(insight_data, sid_cid_df, on='sid', how='inner')
        iids_df = iids_df.groupby('cid')['iid'].apply(list).reset_index(name='iids')
        iids_df['iid_count'] = [len(id_list) for id_list in iids_df['iids']]
        iids_df.sort_values(by='iid_count', inplace=True, ascending=False)
        iids_df.reset_index(inplace=True, drop=True)
        res = iids_df.to_dict('index')
        return res