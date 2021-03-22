import datetime
import os
import pandas as pd
from flask_caching import Cache
import seaborn as sns
from scipy.stats import pearsonr

import numpy as np
import math

from sklearn.cluster import DBSCAN
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler

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
        sid_cid_path = os.path.join(SID_CID_FOLDER, 'sid_cid_{}.csv'.format(name))
        df = pd.read_csv(sid_cid_path)
        return df

    @cache.memoize(timeout=50)
    def __get_subspace_by_name(self, name):
        subspace_path = os.path.join(SUBSPACE_FOLDER, 'subspace_{}.csv'.format(name))
        df = pd.read_csv(subspace_path)
        return df, df.columns.values.tolist()[0:-1]

    def __get_record_by_subspace(self, name, sid):
        sid_cid_data = self.__get_sid_cid_by_name(name)
        record_data = self.__get_record_by_name(name)

        df = pd.merge(sid_cid_data, record_data, on=['cid'])
        df = df.loc[df['sid'] == sid]
        df = df.drop(['sid', 'cid'], axis=1)
        return df

    def __get_subspace_str(self, subspace_col, row):
        subspace = ''
        for i in range(len(subspace_col)):
            col = subspace_col[i]
            if row[col].tolist()[0] != '*':
                subspace += col + ' is ' + row[col].tolist()[0] + ', '
        if subspace[-2:] == ', ':
            subspace = subspace[0:-2]
        return subspace

    def get_data_by_name(self, name):
        edge_data = self.__get_edge_by_name(name)
        insight_data, insight_name, insight_type = self.__get_insight_by_name(name)
        # record_data = self.__get_record_by_name(name)
        subspace_data, feature_data = self.__get_subspace_by_name(name)
        insight_cnt = insight_data['insight'].value_counts().to_dict()
        return {
            # 'record': record_data.to_dict('records'),
            'insight': insight_data.to_dict('records'),
            'edge': edge_data.to_dict('records'),
            'feature': feature_data,
            'insight_name': insight_name,
            'insight_count': [insight_cnt[x] for x in insight_name],
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
        subspace_data, feature_data = self.__get_subspace_by_name(name)
        insight = insight_data.loc[insight_data['iid'] == iid]
        insight = pd.merge(insight, subspace_data, on='sid', how='inner')
        record = self.__get_record_by_subspace(name, insight['sid'].iloc[0])

        insight_name = insight['insight'].iloc[0]
        breakdown = insight['breakdown'].iloc[0]
        breakdown_value = insight['breakdown_value'].iloc[0]
        if breakdown_value.isdigit():
            breakdown_value = int(breakdown_value)
        measure = insight['measure'].iloc[0]
        subspace = self.__get_subspace_str(feature_data, insight)

        if insight_name == 'top1':
            record = record.groupby(breakdown, as_index=False).agg({measure: 'sum'})
            record = record.sort_values(by=measure, ascending=False).iloc[0:10]
            measure_value = record[measure].tolist()
            sentence = 'Given the statistics is {}, ' \
                       'the maximum value of the {} data is {} ' \
                       'when {}{}{} is {}.' \
                .format(measure, 'total', measure_value[0],
                        subspace, (' and ' if subspace != '' else ''),
                        breakdown, breakdown_value)
            return {
                'insight_name': insight_name,
                'breakdown': breakdown,
                'breakdown_value': record[breakdown].tolist(),
                'measure': measure,
                'measure_value': measure_value,
                'sentence': sentence
            }
        elif insight_name == 'trend':
            record = record.groupby(breakdown, as_index=False).agg(
                {breakdown: 'first', measure: 'sum'})

            breakdown_value = record[breakdown]

            if breakdown == 'date' or breakdown == 'Date':
                record[breakdown] = pd.to_datetime(record[breakdown])
            try:
                x = record[breakdown].map(datetime.datetime.toordinal).values
            except:
                x = record[breakdown].values

            x = np.array(x).reshape(-1, 1)
            y = np.array(record[measure]).reshape(-1, 1)
            reg = LinearRegression().fit(x, y)
            slope = reg.coef_[0][0]

            sentence = 'The trend of the {} {} over {}s' \
                       '{}{} is {}.' \
                .format('total', measure, breakdown,
                        (' when ' if subspace != '' else ' in all data'),
                        ' and '.join(subspace.rsplit(', ', 1)),
                        ('increasing' if slope >= 0 else 'decreasing'))
            return {
                'insight_name': insight_name,
                'breakdown': breakdown,
                'measure': measure,
                'breakdown_value': breakdown_value.tolist(),
                'measure_value': record[measure].tolist(),
                'sentence': sentence
            }
        elif insight_name == 'correlation':
            time_col = ""
            if name == 'carSales1':
                time_col = 'Year'
            breakdown_value = insight['breakdown_value'].values[0].split(';')
            col_list = record.columns.values.tolist()
            col_list.remove(insight['measure'].values[0])
            record = self.__get_record_by_name(name)
            record.drop(['cid'], axis=1, inplace=True)

            for i in range(len(col_list)):
                value = insight[col_list[i]].values[0]
                if value != '*':
                    record = record.loc[record[col_list[i]] == value]
            if breakdown_value[1] != '*':
                corr_record = record.loc[record[insight['breakdown'].values[0]] == breakdown_value[1]]
            else:
                corr_record = record.copy()
            if breakdown_value[0] != '*':
                record = record.loc[record[insight['breakdown'].values[0]] == breakdown_value[0]]

            corr_record = corr_record.groupby(time_col, as_index=False).agg(
                {breakdown: 'first', measure: 'sum'})
            record = record.groupby(time_col, as_index=False).agg(
                {breakdown: 'first', measure: 'sum'})
            y1 = record[measure].values
            y2 = corr_record[measure].values
            corr, _ = pearsonr(y1, y2)

            sentence = "The {} of {} and {} are correlated {}." \
                .format(measure, breakdown_value[0],
                        (breakdown_value[1] if breakdown_value[1] != '*' else 'all data'),
                        ('in the dataset' if subspace == '' else 'when ' + subspace))

            y1 = y1.tolist()
            y2 = y2.tolist()

            return {
                'insight_name': insight_name,
                'time_col': time_col,
                'time_col_value': record[time_col].tolist(),
                'measure': measure,
                'measure_value': [y1, y2],
                'max_min': [max(max(y1), max(y2)), min(min(y1), min(y2))],
                'sentence': sentence
            }
        elif insight_name == 'change point' or insight_name == 'outlier':
            record = record.groupby(breakdown, as_index=False).agg(
                {breakdown: 'first', measure: 'sum'})
            # todo: int value might be read as string
            y = record.loc[record[breakdown] == breakdown_value][measure].iloc[0]

            if insight_name == 'change point':
                sentence = 'Among {}s{}{}, ' \
                           'change occurs in {} ' \
                           'and its {} {} is {}.' \
                    .format(breakdown, (' when ' if subspace != '' else ' in all data'),
                            ' and '.join(subspace.rsplit(', ', 1)),
                            breakdown_value, 'total', measure, y)

            else:
                sentence = 'Among {}s{}{}, ' \
                           'the {} {} of {} in {} is an anomaly.' \
                    .format(breakdown, (' when ' if subspace != '' else ' in all data'),
                            ' and '.join(subspace.rsplit(', ', 1)),
                            'total', measure, y, breakdown_value)

            return {
                'insight_name': insight_name,
                'breakdown': breakdown,
                'measure': measure,
                'breakdown_value': record[breakdown].tolist(),
                'measure_value': record[measure].tolist(),
                'x': str(breakdown_value),
                'y': str(y),
                'sentence': sentence
            }
        elif insight_name == 'attribution':
            record = record.groupby(breakdown, as_index=False).agg(
                {breakdown: 'first', measure: 'sum'})
            record = record.sort_values(by=measure)

            breakdown_value = record[breakdown].tolist()
            percentage = (record[measure] / record[measure].sum()).tolist()

            breakdown_value_list = ''
            percentage_list = ''
            for i in range(len(breakdown_value)):
                breakdown_value_list += breakdown_value[i] + ', '
                percentage[i] = round(percentage[i], 2)
                percentage_list += str(percentage[i]) + ', '
            if breakdown_value_list[-2:] == ', ':
                breakdown_value_list = breakdown_value_list[0:-2]
                percentage_list = percentage_list[0:-2]
            sentence = '{} makes up {} ' \
                       'of the {} {}{}{}.' \
                .format(' and '.join(breakdown_value_list.rsplit(', ', 1)),
                        ' and '.join(percentage_list.rsplit(', ', 1)),
                        'total', measure, (' when ' if subspace != '' else ' in all data'),
                        ' and '.join(subspace.rsplit(', ', 1)))

            return {
                'insight_name': insight_name,
                'breakdown_value': breakdown_value,
                'measure_value': record[measure].tolist(),
                'sentence': sentence,
                'percentage': percentage
            }
        elif insight_name == 'cross measure correlation':
            measures = measure.split(';')
            record = record.groupby(breakdown, as_index=False).agg(
                {breakdown: 'first', measures[0]: 'sum', measures[1]: 'sum'})
            record = record.sort_values(by=measures[0])

            x_value = record[measures[0]].values
            y_value = record[measures[1]].values
            reg = LinearRegression().fit(x_value.reshape(-1, 1), y_value.reshape(-1, 1))

            sentence = '{} and {} are linear correlated' \
                       '{}{}{}being grouped by {}.' \
                .format(measures[0], measures[1],
                        (' when ' if subspace != '' else ' in all data'),
                        ' and '.join(subspace.rsplit(', ', 1)),
                        (' and ' if subspace != '' else ' when '),
                        breakdown)

            return {
                'insight_name': insight_name,
                'x_value': x_value.tolist(),
                'y_value': y_value.tolist(),
                'line_y_value': [reg.predict(x_value[0].reshape(-1, 1))[0][0],
                                 reg.predict(x_value[-1].reshape(-1, 1))[0][0]],
                'sentence': sentence
            }
        elif insight_name == 'clustering':
            measures = measure.split(';')
            record = record.groupby(breakdown, as_index=False).agg(
                {breakdown: 'first', measures[0]: 'sum', measures[1]: 'sum'})
            record = record.sort_values(by=measures[0])

            x_value = record[measures[0]].values
            y_value = record[measures[1]].values
            X = np.vstack((x_value, y_value)).T
            X_scale = StandardScaler().fit_transform(X)
            db = DBSCAN(eps=0.3, min_samples=5).fit(X_scale)
            core_samples_mask = np.zeros_like(db.labels_, dtype=bool)
            core_samples_mask[db.core_sample_indices_] = True
            labels = db.labels_

            noise = ''
            if -1 in labels:
                breakdown_value = record[breakdown].values
                cnt = 2
                for i, label in enumerate(labels):
                    if label == -1 and cnt > 0:
                        noise += str(breakdown_value[i]) + ', '
                        cnt -= 1
                noise += 'etc'

            sentence = '{} and {} form clusters' \
                       '{}{}{}being grouped by {}{}{}.' \
                .format(measures[0], measures[1],
                        (' when ' if subspace != '' else ' in all data'),
                        ' and '.join(subspace.rsplit(', ', 1)),
                        (' and ' if subspace != '' else ' when '),
                        breakdown,
                        (', except for ' if noise != '' else ''),
                        noise)

            return {
                'insight_name': insight_name,
                'x_value': x_value.tolist(),
                'y_value': y_value.tolist(),
                'label': labels.tolist(),
                'sentence': sentence
            }
        else:
            return 0

    def get_insight_count_for_subspace_by_name(self, name):
        insight_data, _, _ = self.__get_insight_by_name(name)
        iid_sid_df = insight_data[['iid', 'sid']].groupby('sid')['iid'].apply(list).reset_index(name='iids')
        iid_sid_df['iid_count'] = [len(id_list) for id_list in iid_sid_df['iids']]
        subspace_data, _ = self.__get_subspace_by_name(name)
        iid_sid_df = pd.merge(iid_sid_df, subspace_data, on='sid', how='inner')
        iid_sid_df.sort_values(by=['iid_count'], inplace=True, ascending=False)
        iid_sid_df.reset_index(inplace=True, drop=True)
        res = iid_sid_df.to_dict('index')
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
        measure_list = insight_data['measure'].unique().tolist()
        for measure in measure_list:
            if ';' in measure:
                measure = measure.split(';')
                measure_list.append(measure[0])
                measure_list.append(measure[1])

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
            feature_list = record_data[feature].unique().tolist()
            attr_map[feature] = dict((k, i) for (i, k) in enumerate(feature_list))
        # {'Year': {2007: 0, 2008: 1, 2009: 2, 2010: 3, 2011: 4},
        return attr_map

    def get_subspace_range_by_name(self, name):
        insight, _, _ = self.__get_insight_by_name(name)
        sid_list = np.unique(insight['sid'].tolist())
        subspace, feature_data = self.__get_subspace_by_name(name)
        subspace = subspace.loc[subspace['sid'].isin(sid_list)]
        subspace_range = dict()
        for feature in feature_data:
            feature_list = subspace[feature].unique().tolist()
            subspace_range[feature] = feature_list
        return subspace_range

    def get_data_feature_attribution_by_name(self, name):
        record_data = self.__get_record_by_name(name)
        # result = {'feature_name': {'value_name' : [start_angle, end_angle]}}
        _, feature_data = self.__get_subspace_by_name(name)
        result = {}
        for feature in feature_data:
            value_count = record_data[feature].value_counts().sort_values(ascending=False)
            value_angle = (value_count / value_count.sum() * 2 * math.pi).tolist()
            start_angle = np.concatenate(([0.0], np.cumsum(value_angle)))
            end_angle = np.cumsum(value_angle)
            feature_res = {str(val): [start_angle[idx], end_angle[idx]] for (idx, val) in enumerate(value_count.keys())}
            result[feature] = feature_res
        # feature_cid_count = {feature: record_data[feature].value_counts().to_dict() for feature in feature_data}
        return result

    def get_similar_insight(self, feature, sid, name, breakdown, breakdown_value):
        insight_data, insight_name, insight_type = self.__get_insight_by_name(name)
        subspace_data, feature_data = self.__get_subspace_by_name(name)
        subspace_data = pd.merge(subspace_data, insight_data, on='sid', how='inner')

        subspace = subspace_data.loc[subspace_data['sid'] == sid]
        feature_value = subspace[feature].tolist()[0]
        if feature_value == '*':
            if breakdown == feature:
                feature_value = breakdown_value
            else:
                return {
                    'similar_iid': [],
                    'similar_sid': [],
                    'similar_insight_name': []
                }

        if ';' in feature_value:
            feature_value = breakdown_value.split(';')
        else:
            feature_value = [feature_value]

        similar_iid = []
        similar_sid = []
        similar_insight_name = []
        for value in feature_value:
            if value == '*':
                continue
            similar_subspace = subspace_data.loc[subspace_data[feature] == value]

            for index, sub in similar_subspace.iterrows():
                insights = insight_data.loc[insight_data['sid'] == sub['sid']]
                similar_iid.extend(insights['iid'].tolist())
                similar_sid.extend(insights['sid'].tolist())
                similar_insight_name.extend(insights['insight'].tolist())

        return {
            'similar_iid': similar_iid,
            'similar_sid': similar_sid,
            'similar_insight_name': similar_insight_name
        }
