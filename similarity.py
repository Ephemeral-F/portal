import networkx as nx
import matplotlib.pyplot as plt
import re
import pandas as pd
import numpy as np
import pymysql
import time

from itertools import combinations

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
        # 去除日期
        if text is None:
            return ''
        cop = re.compile('\d{4}[-/]\d{2}[-/]\d{2}')  # 如2016-12-24与2016/12/24的日期格式
        temp = cop.sub('', text).lower()
        cop = re.compile('(20|19)\d{2}')  # 20xx or 19xx
        return cop.sub('', temp).lower()

    @staticmethod
    def remove_date_strictly(text):
        if text is None:
            return ''
        cop = re.compile('\d{4}[-/]\d{2}[-/]\d{2}')  # 如2016-12-24与2016/12/24的日期格式
        temp = cop.sub('', text).lower()
        cop = re.compile('(20)(0|1|2)\d{1}')  # 20xx or 19xx
        return cop.sub('', temp).lower()


class RelatedDataset:
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
        self.replica_graph = nx.Graph()
        self.version_graph = nx.Graph()
        self.full_connected_version_graph = nx.Graph()
        self.parent_graph = nx.DiGraph()
        self.similar_graph = nx.Graph()
        self.outerlink_graph = nx.DiGraph()
        self.general_graph = nx.MultiGraph()
        self.max_version_num = 0
        self.cnt = 0
        self.init_df()
        self.gi = GraphInformation(self.df)

    def init_df(self):
        sql = 'SELECT * FROM summary_dataset;'
        # sql = 'SELECT * FROM summary_ckan_dataset WHERE primary_id in (SELECT primary_id FROM outlink);'
        self.df = pd.read_sql(sql, self.connection, index_col='primary_id')
        self.df.replace([None], '', inplace=True)
        self.df.fillna('')  # 用空字符串填充null，方便处理

        self.df['title_n'] = self.df['title'].map(self.nor.normalize_title)
        self.df['title_n_d'] = self.df['title'].map(self.nor.remove_date)
        self.df['title_n_d'] = self.df['title_n_d'].map(self.nor.normalize_title)

        self.df['description_n'] = self.df['description'].map(self.nor.normalize_description)
        self.df['description_n_d'] = self.df['description'].map(self.nor.remove_date)
        self.df['description_n_d'] = self.df['description_n_d'].map(self.nor.normalize_description)

        self.df['url_n'] = self.df['url'].map(self.nor.normalize_url)
        self.df['url_n_d'] = self.df['url'].map(self.nor.remove_date_strictly)
        self.df['url_n_d'] = self.df['url_n_d'].map(self.nor.normalize_url)

        self.df['download_n'] = self.df['download'].map(self.nor.normalize_url)
        self.df['download_n_d'] = self.df['download'].map(self.nor.remove_date_strictly)
        self.df['download_n_d'] = self.df['download_n_d'].map(self.nor.normalize_url)

    @staticmethod
    def find_date_version(df, key):
        # [A, B, C, ...], A=[a1, a2, ...], a1=[pid, ...]=maybe dataset a1 replicas
        col_n_d, col_n = key + '_n_d', key + '_n'
        n_d_gb = df.groupby(col_n_d).groups
        n_gb = df.groupby(col_n).groups
        version = {}
        for n_d_key, n_d_list in n_d_gb.items():
            version_list = []
            if len(n_d_list) > 1 and len(n_d_key) > 0:
                difference = n_d_list
                while len(difference) > 0:
                    # print(difference)
                    n_key = df.loc[difference[0], col_n]
                    n_list = n_gb.get(n_key, [])
                    intersection = list(set(difference) & set(n_list))
                    difference = list(set(difference) - set(n_list))
                    intersection.sort()
                    version_list.append(intersection)
                version[n_d_key] = version_list
        return [v for k, v in version.items()]

    @staticmethod
    def find_strongly_same(df, gb_col):  # 按gb_col来groupby（不计空串），返回数量>=2的子字典
        gb = df.groupby(gb_col).groups
        # print(gb, gb_col)
        ret_dict = {}
        for k, v in gb.items():
            skip = len(v) < 2
            if skip:
                continue
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
        # print("ret_dict:", ret_dict)
        return ret_dict

    @staticmethod
    def convert_dict_to_list(gb_dict):
        gb_list = []
        for k, v in gb_dict.items():
            gb_list.append(v.to_list())
        return gb_list

    def find_duplicates(self, gb_col, s_col, w_col):
        duplicates_list = []
        gb_dict = self.find_strongly_same(self.df, gb_col)
        for k, v in gb_dict.items():
            sub_df = self.df.loc[v]
            for col in s_col:
                # duplicates_list += self.convert_dict_to_list(self.find_strongly_same(sub_df, col))
                temp = self.convert_dict_to_list(self.find_strongly_same(sub_df, col))
                # for i in temp:
                #     if len(i) < 2:
                #         print(gb_col, s_col, col)
                #         print(temp)
                duplicates_list += temp
                # print(col, duplicates_list)
            all_primary_id = set(sub_df.index.values)
            sub_df['inter'] = sub_df.apply(lambda x: all_primary_id, axis=1)
            for col in w_col:
                w_dict = sub_df.groupby(col).groups
                # print(col, w_dict)
                sub_df['_'] = sub_df[col].map(lambda x: w_dict[x] if x != '' else all_primary_id)
                sub_df['inter'] = sub_df.apply(
                    lambda x: set.intersection(x['inter'], set.union(set(x['_']), set(w_dict.get('', [])))), axis=1)
                # print(sub_df['_'].values)
                # print(sub_df['inter'].values)
                # print()
            temp = sub_df['inter'].values
            duplicates_list += [list(x) for x in temp if len(x) > 1]
        return duplicates_list

    def find_replicas(self):
        # 描述性（title/description）+链接（url/download），3/4相同，空不算相同
        replicas_list = []
        w_col = ['name', 'author', 'license', 'notes', 'parent_fxf', 'size']
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

    def construct_replicas_graph(self, replicas_list):
        for replicas in replicas_list:
            nx.add_path(self.replica_graph, replicas)
        # cc_list = list(nx.connected_components(self.replica_graph))
        # cc_num, max_cc = len(cc_list), max(cc_list, key=len)
        # max_cc_len = len(max_cc)
        # print(cc_num, max_cc_len, max_cc)

    def construct_similar_graph(self, similar_list):
        for similar in similar_list:
            nx.add_path(self.similar_graph, similar)
        # cc_list = list(nx.connected_components(self.similar_graph))
        # cc_num, max_cc = len(cc_list), max(cc_list, key=len)
        # max_cc_len = len(max_cc)
        # print(cc_num, max_cc_len, max_cc)

    def construct_parent_graph(self, parent_list):
        if len(parent_list) == 0:
            return
        self.parent_graph.add_edges_from(parent_list)
        # cc_list = list(nx.weakly_connected_components(self.parent_graph))
        # cc_num, max_cc = len(cc_list), max(cc_list, key=len)
        # max_cc_len = len(max_cc)
        # print(cc_num, max_cc_len, max_cc)

    def construct_version_graph(self, version_list):
        for version in version_list:
            if len(version) < 2:
                continue
            path = [x[0] for x in version]
            nx.add_path(self.version_graph, path)
            diff = set([item for sublist in version for item in sublist])
            for same in version:
                self.cnt = max(len(same), self.cnt)
                diff = diff - set(same)
                edges = [(x, y) for x in same for y in diff]
                self.full_connected_version_graph.add_edges_from(edges)
        # max_cc = max(nx.connected_components(self.version_graph), key=len)
        # print(len(max_cc), max_cc)
        # for version in version_list:
        #     diff = set([item for sublist in version for item in sublist])
        #     for same in version:
        #         diff = diff - set(same)
        #         edges = [(x, y) for x in same for y in diff]
        #         self.version_graph.add_edges_from(edges)
        # max_ = max(version_list, key=len)
        # self.max_version_num = max(self.max_version_num, len(max_cc))
        # print(len(version_list), len(max_), max_)
        # print(self.max_version_num)

    def find_parent_fxf(self):
        parent_list = []
        sql = 'SELECT id, primary_id FROM summary_socrata;'
        self.cur.execute(sql)
        results = self.cur.fetchall()
        id_pid_dict = {res[0]: res[1] for res in results}
        sub_df = self.df.loc[self.df['parent_fxf'] != '']
        for primary_id, row in sub_df.iterrows():
            parents = row['parent_fxf'].split(';')
            for parent in parents:
                if parent in id_pid_dict.keys():
                    parent_list.append([id_pid_dict[parent], primary_id])
        return parent_list

    def find_replicas_version(self, connected_replicas_list):
        version_list = []  # [A, B, C, ...], A=[a1, a2, ...], a1=[pid, ...]=dataset a1 replicas
        for replicas in connected_replicas_list:
            sub_df = self.df.loc[replicas]
            ver_dict = sub_df.groupby('version').groups
            ver_dict2 = {k: v for k, v in ver_dict.items() if len(k) > 0}
            if len(ver_dict2) > 1:
                version_list.append(self.convert_dict_to_list(ver_dict2))
        return version_list

    def find_similar(self):
        cols = ['title_n', 'description_n', 'url_n', 'download_n']
        gb_cols = combinations(cols, 2)
        similar_list = []
        for gb_col in gb_cols:
            gb_dict = self.find_strongly_same(self.df, list(gb_col))
            similar_list += self.convert_dict_to_list(gb_dict)
        return similar_list

    def construct_outerlink_graph(self):
        sql = 'SELECT lzy_id, primary_id FROM outlink;'
        self.cur.execute(sql)
        results = self.cur.fetchall()
        map_dict = {x[0]: x[1] for x in results}
        connection = pymysql.connect(host='114.212.82.63',  # host属性
                                     port=3306,
                                     user='root',  # 用户名
                                     password='lzy199661',  # 此处填登录数据库的密码
                                     db='dataset',  # 数据库名
                                     )
        cur = connection.cursor()
        sql = 'SELECT sub_ds, obj_ds, count FROM outerlink3'
        cur.execute(sql)
        results = cur.fetchall()
        for res in results:
            pid_sub, pid_obj, count = map_dict[res[0]], map_dict[res[1]], res[2]
            self.outerlink_graph.add_edge(pid_sub, pid_obj, count=count)
        connection.close()

    def construct_general_graph(self, add_outerlink=False):
        self.general_graph.add_nodes_from(self.df.index.to_list())
        self.general_graph.add_edges_from(self.similar_graph.edges(), relationship='similar')
        self.general_graph.add_edges_from(self.replica_graph.edges(), relationship='replica')
        self.general_graph.add_edges_from(self.version_graph.edges(), relationship='version')
        self.general_graph.add_edges_from(self.parent_graph.edges(), relationship='parent')
        if add_outerlink:
            self.general_graph.add_edges_from(self.outerlink_graph.edges(data=True), relationship='outerlink')

    def construct_full_connected_general_graph(self, add_outerlink=False):
        self.general_graph.add_nodes_from(self.df.index.to_list())
        cc = nx.connected_components(self.similar_graph)
        for c in cc:
            G = nx.complete_graph(c)
            self.general_graph.add_edges_from(G.edges(), relationship='similar')
        cc = nx.connected_components(self.replica_graph)
        for c in cc:
            G = nx.complete_graph(c)
            self.general_graph.add_edges_from(G.edges(), relationship='replica')
        self.general_graph.add_edges_from(self.parent_graph.edges(), relationship='parent')
        self.general_graph.add_edges_from(self.full_connected_version_graph.edges(), relationship='version')
        if add_outerlink:
            self.general_graph.add_edges_from(self.outerlink_graph.edges(data=True), relationship='outerlink')

    def analyse(self):
        print('====similar====')
        lst = self.find_similar()
        self.construct_similar_graph(lst)

        G = self.similar_graph
        self.gi.similar_graph_statistic(G)

        print()
        print()
        print()

        print('====replicas====')
        lst = self.find_replicas()
        self.construct_replicas_graph(lst)

        G = self.replica_graph
        cc_list = nx.connected_components(G)
        lst = self.find_replicas_version(cc_list)
        self.construct_version_graph(lst)
        self.gi.similar_graph_statistic(G)

        print()
        print()
        print()

        print('====date version====')
        lst = self.find_date_version(self.df, 'title')
        self.construct_version_graph(lst)

        title_same_list = self.convert_dict_to_list(self.find_strongly_same(self.df, 'title_n'))
        for title_same in title_same_list:
            sub_df = self.df.loc[title_same]
            lst = self.find_date_version(sub_df, 'description')
            self.construct_version_graph(lst)
            lst = self.find_date_version(sub_df, 'download')
            self.construct_version_graph(lst)
        G = self.version_graph
        self.gi.version_graph_statistic(G)

        print(self.cnt)
        print()
        print()
        print()

        print('====parent====')
        lst = self.find_parent_fxf()
        self.construct_parent_graph(lst)

        G = self.parent_graph
        self.gi.parent_graph_statistic(G)

        print()
        print()
        print()

        print('====general====')
        self.construct_outerlink_graph()
        self.construct_full_connected_general_graph(add_outerlink=True)
        G = self.general_graph
        self.gi.general_graph_statistic(G)

        # self.construct_outerlink_graph()
        # G = self.outerlink_graph
        # self.gi.outerlink_graph_statistic(G)


class GraphInformation:
    def __init__(self, df):
        self.df = df

    @staticmethod
    def log_fit(x, y):
        """line fitting on log-log scale"""
        xx = np.nonzero(y)[0][1:]
        yy = np.array(y)[xx]
        k, m = np.polyfit(np.log(xx), np.log(yy), 1)
        print('k, m: ', k, m)
        x = np.array(x)
        return np.exp(m) * x ** (k)

    def degree_statistic(self, G):
        # d30, d369 = [], []
        # degree = nx.degree(G)
        # for d in degree:
        #     if d[1] == 30:
        #         d30.append(d[0])
        #     elif d[1] == 369:
        #         d369.append(d[0])
        # print('degree = 30: ', d30)
        # print('degree = 369: ', d369)
        degree = nx.degree_histogram(G)  # 返回图中所有节点的度分布序列
        print(degree)
        print('max degree:', len(degree))
        x = range(len(degree))  # 生成x轴序列，从1到最大度
        y = [z / float(sum(degree)) for z in degree]
        print(y)
        ys = self.log_fit(x[:200], y[:30])
        plt.loglog(x, y, '.')  # 在双对坐标轴上绘制度分布曲线
        plt.plot(x[:200], ys)
        plt.show()  # 显示图表

    @staticmethod
    def connected_components_statistic(cc_list, truncation=6):
        print('number_connected_components: ', len(cc_list))
        cc_list = [x for x in cc_list if len(x) > 1]
        print('number_connected_components(>=2): ', len(cc_list))
        cc_len_dict = {}
        num_nodes = 0
        for i in cc_list:
            cc_len_dict[len(i)] = cc_len_dict.get(len(i), 0) + 1
            num_nodes += len(i)
        print(cc_len_dict)
        print('number_connected_components_nodes: ', num_nodes)
        print('max_connected_components_length: ', max(cc_len_dict.keys()))
        truncation_cc_len_dict = {}
        num_larger = 0
        for k, v in cc_len_dict.items():
            if k < truncation:
                truncation_cc_len_dict[str(k)] = v
            else:
                num_larger += v
        truncation_cc_len_dict['{} and larger'.format(str(truncation))] = num_larger
        print(truncation_cc_len_dict)
        x = list(truncation_cc_len_dict.keys())
        x.sort()
        x_value = [truncation_cc_len_dict[i] for i in x]
        num_cc = sum(x_value)
        y = [z / num_cc for z in x_value]
        print(x)
        print(y)
        plt.bar(x, y)
        plt.show()

    @staticmethod
    def top_k_dict(in_dict, top=5, reverse=True):
        sort_list = sorted(in_dict.items(), key=lambda item: item[1], reverse=reverse)
        return sort_list[:top]

    def cc_list_source_statistic(self, cc_list, is_pair=False, top=5):
        db_dict, db_pair_dict = {}, {}
        for cc in cc_list:
            sub_df = self.df.loc[cc, ['db_name']]
            db_name_list = sub_df['db_name'].to_list()
            db_name_dict = {}
            for db_name in db_name_list:
                db_dict[db_name] = db_dict.get(db_name, 0) + 1
                if is_pair:
                    db_name_dict[db_name] = db_name_dict.get(db_name, 0) + 1
                    db_name_pairs = combinations(db_name_dict.keys(), 2)
                    for pair in db_name_pairs:
                        pair = pair if pair[0] < pair[1] else (pair[1], pair[0])
                        db_pair_dict[pair] = db_pair_dict.get(pair, 0) + 1
        db_sort_list = sorted(db_dict.items(), key=lambda item: item[1], reverse=True)
        x = [i[0] for i in db_sort_list]
        y = [i[1] for i in db_sort_list]
        print('number_portals:', len(db_sort_list))
        print('top {} portal: '.format(top), db_sort_list[:top])
        if is_pair:
            print('top {} portal pairs: '.format(top), self.top_k_dict(db_pair_dict, top))
        print(y)

    def similar_graph_statistic(self, G, top=5):
        cc_list = list(nx.connected_components(G))
        self.connected_components_statistic(cc_list)
        self.cc_list_source_statistic(cc_list, is_pair=True, top=top)

    def parent_graph_statistic(self, G):
        if len(G.edges()) == 0:
            return
        in_degree, out_degree = G.in_degree, G.out_degree
        max_in_degree, max_out_degree = max(in_degree, key=lambda x: x[1]), max(out_degree, key=lambda x: x[1])
        print('max_in_degree: ', max_in_degree)
        print('max_out_degree: ', max_out_degree)
        n_nodes, n_edges = G.number_of_nodes(), G.number_of_edges()
        print('number_of_nodes: ', n_nodes)
        print('number_of_edges: ', n_edges)
        print('avg_degree: ', 2 * n_edges / n_nodes)
        print('dag_longest_path_length: ', nx.dag_longest_path_length(G))
        cc_list = list(nx.weakly_connected_components(G))
        self.connected_components_statistic(cc_list)
        self.cc_list_source_statistic(cc_list)

    def version_graph_statistic(self, G):
        cc_list = list(nx.connected_components(G))
        cc_list = [x for x in cc_list if len(x) > 1]
        self.connected_components_statistic(cc_list)
        self.cc_list_source_statistic(cc_list)

    def outerlink_graph_statistic(self, G):
        print('degree')
        in_degree, out_degree, inout_degree, weight_degree = G.in_degree, G.out_degree, G.degree, nx.degree(G,
                                                                                                            weight='count')
        max_in_degree, max_out_degree = max(in_degree, key=lambda x: x[1]), max(out_degree, key=lambda x: x[1])
        max_degree, max_weight_degree = max(inout_degree, key=lambda x: x[1]), max(weight_degree, key=lambda x: x[1])
        print('max_in_degree: ', max_in_degree)
        print('max_out_degree: ', max_out_degree)
        print('max_degree: ', max_degree)
        print('max_weight_degree: ', max_weight_degree)
        cc_list = list(nx.strongly_connected_components(G))
        print('strongly_connected_components')
        self.connected_components_statistic(cc_list)
        cc_list = list(nx.weakly_connected_components(G))
        print('weakly_connected_components')
        self.connected_components_statistic(cc_list)
        degree = nx.degree_histogram(G)  # 返回图中所有节点的度分布序列
        x = range(len(degree))  # 生成x轴序列，从1到最大度
        y = [z / float(sum(degree)) for z in degree]
        plt.bar(x, y)
        plt.show()

    def general_graph_statistic(self, G, top=5):
        self.degree_statistic(G)
        H = nx.Graph(G)
        cc_list = list(nx.connected_components(H))
        self.connected_components_statistic(cc_list)
        self.cc_list_source_statistic(cc_list)
        print('average_clustering: ', nx.average_clustering(H))  # 群聚系数
        # print('diameter: ', nx.diameter(G))  # 直径
        print('degree_assortativity_coefficient: ', nx.degree_assortativity_coefficient(H))  # 同配性

        dc_dict = nx.degree_centrality(H)  # 度中心性，{node: degree_centrality}
        print('degree_centrality top {}: '.format(top), self.top_k_dict(dc_dict, top))
        dc_dict = nx.eigenvector_centrality(H)  # 特征向量中心性
        print('eigenvector_centrality top {}: '.format(top), self.top_k_dict(dc_dict, top))
        dc_dict = nx.closeness_centrality(H)  # 接近中心性
        print('closeness_centrality top {}: '.format(top), self.top_k_dict(dc_dict, top))
        # dc_dict = nx.betweenness_centrality(H)  # 中介中心性
        # print('betweenness_centrality top {}: '.format(top), self.top_k_dict(dc_dict, top))


def log_fit(x, y):
    """line fitting on log-log scale"""
    xx = np.nonzero(y[:30])[0][1:]
    yy = np.array(y)[xx]
    k, m = np.polyfit(np.log(xx), np.log(yy), 1)
    print(k, m)
    x = np.array(x)
    return np.exp(m) * x ** k


def test():
    sim = RelatedDataset()
    sim.analyse()

    # degree = [25353, 50192, 13673, 6571, 3983, 1803, 1511, 1216, 882, 477, 541, 497, 572, 247, 236, 195, 301, 168, 186, 138, 189, 65, 149, 44, 90, 89, 23, 71, 35, 135, 1420, 42, 16, 8, 12, 4, 25, 55, 19, 8, 48, 39, 4, 44, 6, 5, 7, 4, 6, 5, 5, 3, 5, 3, 1, 34, 17, 15, 1, 33, 4, 2, 0, 0, 2, 1, 9, 5, 3, 1, 0, 2, 2, 1, 1, 3, 1, 0, 2, 1, 0, 1, 37, 2, 4, 10, 1, 13, 1, 0, 1, 0, 0, 0, 1, 1, 1, 1, 1, 0, 0, 0, 22, 1, 1, 0, 0, 1, 1, 1, 1, 0, 2, 5, 6, 3, 4, 1, 1, 2, 1, 1, 0, 0, 3, 2, 2, 0, 0, 2, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 0, 2, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 734, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
    # x = range(len(degree))  # 生成x轴序列，从1到最大度
    # y = [z / float(sum(degree)) for z in degree]
    # plt.loglog(x, y, '.')  # 在双对坐标轴上绘制度分布曲线
    # xx = np.nonzero(y[:30])[0][1:]
    # yy = np.array(y)[xx]
    # xxx, yyy = np.log(xx), np.log(yy)
    # k, m = np.polyfit(xxx, yyy, 1)
    # yyyy = np.exp(m) * np.array(x[:200]) ** k
    # print(k, m)
    # print(xxx)
    # print(yyy)
    # plt.plot(x[:200], yyyy)
    # plt.show()  # 显示图表


if __name__ == '__main__':
    test()
