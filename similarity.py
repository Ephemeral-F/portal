import networkx as nx
import matplotlib.pyplot as plt
import re
import pandas as pd
import numpy as np
import pymysql

SIMILAR_CSV_PATH = 'data/similar.csv'


class Normalization:
    def __init__(self):
        pass

    @staticmethod
    def normalize_title(title):
        # 去除分隔符/特殊符号/空格，转成全小写
        if title is None:
            return ''
        cop = re.compile('[’!"#$%&\'()*+,-./:;<=>?@，。?★†§、…【】《》？“”‘’！[\\]^_`{|}~\s]+')  # 匹配特殊字符
        return cop.sub('', title).lower()  # 将匹配到的字符替换成空字符

    @staticmethod
    def normalize_description(description):
        # 去除HTML印记/分隔符/特殊符号/空格，转成全小写
        if description is None:
            return ''
        cop = re.compile('<[^>]*>')  # 匹配HTML标记
        temp = cop.sub('', description)
        cop = re.compile('[’!"#$%&\'()*+,-./:;<=>?@，。?★†§、…【】《》？“”‘’！[\\]^_`{|}~\s]+')  # 匹配特殊字符
        return cop.sub('', temp).lower()  # 将匹配到的字符替换成空字符

    @staticmethod
    def normalize_url(url):
        # url去除https://
        if url is None:
            return ''
        cop = re.compile('(http|https)(://)')
        return cop.sub('', url).lower()

    @staticmethod
    def remove_date(text):
        # 去除数字
        if text is None:
            return ''
        cop = re.compile('\d{4}[-/]\d{2}[-/]\d{2}')  # 如2016-12-24与2016/12/24的日期格式
        temp = cop.sub('', text).lower()
        cop = re.compile('(20|19)\d{2}')  # 20xx 或 19xx
        return cop.sub('', temp).lower()


class SimilarDataset:
    def __init__(self):
        self.connection = pymysql.connect(host='localhost',  # host属性
                                          port=3306,
                                          user='root',  # 用户名
                                          password='mysql',  # 此处填登录数据库的密码
                                          db='rdf_metadata',  # 数据库名
                                          )
        self.cur = self.connection.cursor()
        self.nor = Normalization()
        self.df = pd.DataFrame([])
        self.replicas_graph = nx.Graph()
        self.version_graph = nx.Graph()
        self.parent_graph = nx.Graph()

    def find_date_version(self, col_n_d, col_n):
        # [A, B], A=[a2020, a2021], a2020=[col2020 datasets]
        n_d_gb = self.df.groupby(col_n_d).groups
        n_gb = self.df.groupby(col_n).groups
        version = {}
        for n_d_key, n_d_list in n_d_gb.items():
            version_list = []
            if len(n_d_list) > 1 and len(n_d_key) > 0:
                difference = n_d_list
                while len(difference) > 0:
                    # print(difference)
                    n_key = self.df.loc[difference[0], col_n]
                    n_list = n_gb.get(n_key, [])
                    intersection = list(set(difference) & set(n_list))
                    difference = list(set(difference) - set(n_list))
                    version_list.append(intersection)
                version[n_d_key] = version_list
        return self.convert_dict_to_list(version)

    @staticmethod
    def find_same(df, gb_col):  # 按gb_col来groupby，返回数量>=2的子字典
        gb = df.groupby(gb_col).groups
        ret_dict = {}
        for k, v in gb.items():
            skip = len(v) < 2
            if isinstance(k, str):
                skip = len(k) <= 0
            else:
                for i in k:
                    if len(i) <= 0:
                        skip = True
            if skip:
                continue
            else:
                ret_dict[k] = v
        return ret_dict

    @staticmethod
    def convert_dict_to_list(gb_dict):
        gb_list = []
        for k, v in gb_dict.items():
            gb_list.append(v.to_list())
        return gb_list

    def find_duplicates(self, gb_col, s_col, w_col):
        duplicates_list = []
        gb_dict = self.find_same(self.df, gb_col)
        for k, v in gb_dict.items():
            sub_df = self.df.loc[v]
            for col in s_col:
                duplicates_list += self.convert_dict_to_list(self.find_same(sub_df, col))
                # print(col, duplicates_list)
            all_primary_id = set(sub_df.index.values)
            sub_df['inter'] = sub_df.apply(lambda x: all_primary_id, axis=1)
            for col in w_col:
                w_dict = sub_df.groupby(col).groups
                # print(col, w_dict)
                sub_df['_'] = sub_df[col].map(lambda x: w_dict[x] if x != '' else all_primary_id)
                sub_df['inter'] = sub_df.apply(lambda x: set.intersection(x['inter'], set.union(set(x['_']), set(w_dict.get('', [])))), axis=1)
                # print(sub_df['_'].values)
                # print(sub_df['inter'].values)
                # print()
            temp = sub_df['inter'].values
            duplicates_list += [list(x) for x in temp if len(x) > 1]
        return duplicates_list

    def find_replicas(self):
        # 描述性（title/description）+链接（url/download），3/4相同，空不算相同
        replicas_list = []
        w_col = ['name', 'author', 'license', 'notes', 'parent_fxf']
        gb_col, s_col = ['title_n', 'description_n'], ['url_n', 'download_n']
        replicas_list += self.find_duplicates(gb_col, s_col, w_col)
        gb_col, s_col = ['title_n', 'url_n'], ['download_n']
        replicas_list += self.find_duplicates(gb_col, s_col, w_col)
        gb_col, s_col = ['title_n', 'download_n'], []
        replicas_list += self.find_duplicates(gb_col, s_col, w_col)
        gb_col, s_col = ['url_n', 'description_n'], []
        replicas_list += self.find_duplicates(gb_col, s_col, w_col)
        gb_col, s_col = ['url_n', 'download_n'], []
        replicas_list += self.find_duplicates(gb_col, s_col, w_col)
        gb_col, s_col = ['description_n', 'download_n'], []
        replicas_list += self.find_duplicates(gb_col, s_col, w_col)
        return replicas_list

    def construct_replicas_garph(self, replicas_list):
        for replicas in replicas_list:
            for idx in range(len(replicas)-1):
                self.replicas_graph.add_edge(replicas[idx], replicas[idx+1])
        cc_list = list(nx.connected_components(self.replicas_graph))
        for i in cc_list:
            print(i)
        print(len(cc_list), max(cc_list, key=len))

    def find_similar(self):
        sql = 'SELECT * FROM summary_dataset;'
        self.df = pd.read_sql(sql, self.connection, index_col='primary_id')
        self.df.replace([None], '', inplace=True)
        self.df.fillna('')  # 用空字符串填充null，方便处理

        self.df['title_n'] = self.df['title'].map(self.nor.normalize_title)
        self.df['title_n_d'] = self.df['title_n'].map(self.nor.remove_date)

        self.df['description_n'] = self.df['description'].map(self.nor.normalize_description)

        self.df['url_n'] = self.df['url'].map(self.nor.normalize_url)
        self.df['url_n_d'] = self.df['url_n'].map(self.nor.remove_date)

        self.df['download_n'] = self.df['download'].map(self.nor.normalize_url)
        self.df['download_n_d'] = self.df['download_n'].map(self.nor.remove_date)

        replicas_list = self.find_replicas()
        self.construct_replicas_garph(replicas_list)


def test():
    sim = SimilarDataset()
    sim.find_similar()

    # nor = Normalization()
    # print(nor.remove_date('NYCgov Poverty Measure Data (2005)'))


if __name__ == '__main__':
    test()
