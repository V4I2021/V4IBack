import os
import pandas as pd
from flask_caching import Cache
import seaborn as sns

cache = Cache()

FILE_ABS_PATH = os.path.dirname(__file__)
ROOT_PATH = os.path.join(FILE_ABS_PATH, '../')
EDGE_FOLDER = os.path.join(ROOT_PATH, 'data/edge')
INSIGHT_FOLDER = os.path.join(ROOT_PATH, 'data/insight')
RECORD_FOLDER = os.path.join(ROOT_PATH, 'data/record')
SID_CID_FOLDER = os.path.join(ROOT_PATH, 'data/sid_cid')
SUBSPACE_FOLDER = os.path.join(ROOT_PATH, 'data/subspace')

sns_counter = 0


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

    def __get_record_by_subspace(self, name, sid):
        sid_cid_data = self.__get_sid_cid_by_name(name)
        record_data = self.__get_record_by_name(name)

        df = pd.merge(sid_cid_data, record_data, on=['cid'])
        df = df.loc[df['sid'] == sid]
        df = df.drop(['sid', 'cid'], axis=1)
        return df

    def get_data_by_name(self, name):
        edge_data = self.__get_edge_by_name(name)
        insight_data, insight_name, insight_type = self.__get_insight_by_name(name)
        record_data = self.__get_record_by_name(name)
        subspace_data, feature_data = self.__get_subspace_by_name(name)
        # print(subspace_data)
        # print("---------------------")
        # print(subspace_data.to_dict('index'))
        return {
            'record': record_data.to_dict('records'),
            'insight': insight_data.to_dict('records'),
            'edge': edge_data.to_dict('records'),
            'feature': feature_data,
            'insight_name': insight_name,
            'insight_type': insight_type,
            'subspace': subspace_data.to_dict('index')
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
        record = self.__get_record_by_subspace(name, insight['sid'].iloc[0])

        insight_name = insight['insight'].iloc[0]
        breakdown = insight['breakdown'].iloc[0]
        breakdown_value = insight['breakdown_value'].iloc[0]
        if breakdown_value.isdigit():
            breakdown_value = int(breakdown_value)
        measure = insight['measure'].iloc[0]

        if insight_name == 'top1':
            record = record.groupby(breakdown, as_index=False).agg({measure: 'sum'})
            record = record.sort_values(by=measure, ascending=False).iloc[0:10]
            return {
                'insight_name': insight_name,
                'measure': measure,
                'measure_value': record[measure].tolist()
            }
        elif insight_name == 'trend':
            record = record.groupby(breakdown, as_index=False).agg(
                {breakdown: 'first', measure: 'sum'})
            return {
                'insight_name': insight_name,
                'breakdown': breakdown,
                'measure': measure,
                'breakdown_value': record[breakdown].tolist(),
                'measure_value': record[measure].tolist()
            }
        elif insight_name == 'correlation':
            # todo
            return 0
        elif insight_name == 'change point' or insight_name == 'outlier':
            record = record.groupby(breakdown, as_index=False).agg(
                {breakdown: 'first', measure: 'sum'})
            # todo: int value might be read as string
            y = record.loc[record[breakdown] == breakdown_value][measure].iloc[0]

            return {
                'insight_name': insight_name,
                'breakdown': breakdown,
                'measure': measure,
                'breakdown_value': record[breakdown].tolist(),
                'measure_value': record[measure].tolist(),
                'x': str(breakdown_value),
                'y': str(y)
            }
        elif insight_name == 'attribution':
            record = record.groupby(breakdown, as_index=False).agg(
                {breakdown: 'first', measure: 'sum'})
            record = record.sort_values(by=measure)
            return {
                'insight_name': insight_name,
                'breakdown_value': record[breakdown].tolist(),
                'measure_value': record[measure].tolist()
            }
        else:
            return 0

    def get_insight_count_for_subspace_by_name(self, name):
        insight_data, _, _ = self.__get_insight_by_name(name)
        iid_sid_df = insight_data[['iid', 'sid']].groupby('sid')['iid'].apply(list).reset_index(name='iids')
        iid_sid_df['iid_count'] = [len(id_list) for id_list in iid_sid_df['iids']]

        subspace_data, _ = self.__get_subspace_by_name(name)
        subspace_data['star_count'] = ""
        for index, row in subspace_data.iterrows():
            if '*' in row.tolist():
                count = row.tolist().count('*')
                subspace_data['star_count'][index] = count
            else:
                subspace_data['star_count'][index] = 0

        iid_sid_df = subspace_data[['sid', 'star_count']].merge(iid_sid_df, on='sid', how='inner')
        iid_sid_df.sort_values(by=['iid_count', 'star_count'], inplace=True, ascending=False)
        subspace_data, _ = self.__get_subspace_by_name(name)
        iid_sid_df = pd.merge(iid_sid_df, subspace_data, on='sid', how='inner')
        iid_sid_df.reset_index(inplace=True, drop=True)
        print(iid_sid_df)
        res = iid_sid_df.to_dict('index')
        # print("get_insight_count_for_subspace_by_name!!!!")
        # print(iid_sid_df)
        return res

    def get_subspace_count_for_record_by_name(self, name):
        sid_cid_df = self.__get_sid_cid_by_name(name)
        record_data = self.__get_record_by_name(name)
        df = pd.merge(record_data, sid_cid_df, on='cid', how='inner')
        df = df.groupby('cid')['sid'].apply(list).reset_index(name='sid')
        df['sid_count'] = [len(id_list) for id_list in df['sid']]
        df.sort_values(by='sid_count', inplace=True, ascending=False)
        df.reset_index(inplace=True, drop=True)
        res = df.to_dict('index')
        return res

    @cache.memoize(timeout=50)
    def get_data_info_by_name(self, name):
        global sns_counter
        record_data = self.__get_record_by_name(name)
        insight_data, _, _ = self.__get_insight_by_name(name)
        record_data = record_data.drop(columns=['cid'])

        data_info = {
            'dataName': name,
            'dataDescription': '',
            'rowCnt': record_data.shape[0],
            'colCnt': record_data.shape[1],
            'colName': [],
            'colType': [],
            'colValueType': [],
            'colValue': []
        }

        for value_type in record_data.dtypes.tolist():
            value_type = str(value_type)
            if value_type != 'int64' and value_type != 'float64':
                data_info['colValueType'].append('categorical')
            else:
                data_info['colValueType'].append(value_type.rstrip('64'))

        if name == 'carSales1':
            data_info['dataDescription'] = 'Describe vehicle sales.'
        elif name == 'carSales2':
            data_info['dataDescription'] = 'Describe vehicle sales in more detail.'
        elif name == 'Census':
            data_info['dataDescription'] = 'Describe demographic information.'

        data_info['colName'] = record_data.columns.values.tolist()
        measure_list = insight_data['measure'].unique()
        for i in range(len(data_info['colName'])):
            col = data_info['colName'][i]
            if col in measure_list:
                data_info['colType'].append('measure')
            else:
                data_info['colType'].append('attribute')

            if data_info['colValueType'][i] == 'int' \
                    or data_info['colValueType'][i] == 'float':
                p = sns.kdeplot(record_data[col].values)
                lines = [obj for obj in p.findobj() if str(type(obj)) == "<class 'matplotlib.lines.Line2D'>"]
                x, y = lines[sns_counter].get_data()[0].tolist(), lines[sns_counter].get_data()[1].tolist()
                data_info['colValue'].append([x, y,
                                              round(min(x), 2), round(max(x), 2),
                                              min(y), max(y)])
                sns_counter += 1
            else:
                cnt_dict = record_data[col].value_counts().to_dict()
                value_list = list(cnt_dict.values())
                upper_bound = value_list[0]
                while upper_bound % 5 != 0 or upper_bound % 2 != 0:
                    upper_bound += 1
                data_info['colValue'].append([list(cnt_dict), value_list, upper_bound])

        return data_info

    def get_data_attr_map_by_name(self, name):
        record_data = self.__get_record_by_name(name)
        _, feature_data = self.__get_subspace_by_name(name)
        attr_map = dict()
        for feature in feature_data:
            print(record_data[feature])
            print(record_data[feature].unique())
            feature_list = record_data[feature].unique().tolist()
            attr_map[feature] = dict((k, i) for (i, k) in enumerate(feature_list))
        print(attr_map.__class__)
        return attr_map
