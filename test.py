import requests
import pymysql
import datetime
import re
import networkx as nx
import matplotlib.pyplot as plt


def test_sql():
    connection = pymysql.connect(host='localhost',  # host属性
                                 port=3306,
                                 user='root',  # 用户名
                                 password='mysql',  # 此处填登录数据库的密码
                                 db='a.c.f',  # 数据库名
                                 )
    cur = connection.cursor()
    # 关闭连接对象，否则会导致连接泄漏，消耗数据库资源
    sql = 'INSERT INTO `a-s-x`(`runoob_id`, `runoob_title`, `runoob_author`, `submission_date`) VALUES (%s,%s,%s,%s)'
    create_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    val = [(6, 777, '', create_time),
           ]
    try:
        # 执行sql语句
        row = cur.executemany(sql, val)
        # 提交到数据库执行
        connection.commit()
        print(row)
    except Exception as e:
        # 如果发生错误则回滚
        print(e)
        connection.rollback()
    cur.execute('SELECT * FROM `a-s-x`')
    print(cur.description)
    results = cur.fetchall()
    for res in results:
        print(res)
    connection.close()
    # 关闭光标
    cur.close()


def test_sql2():
    # connection = pymysql.connect(host='114.212.82.63',  # host属性
    #                              port=3306,
    #                              user='root',  # 用户名
    #                              password='lzy199661',  # 此处填登录数据库的密码
    #                              db='dataset',  # 数据库名
    #                              )
    # cur = connection.cursor()
    # sql = 'SELECT id, dataset_id FROM metadata WHERE dataset_id in (SELECT DISTINCT(dataset_id) FROM triple);'
    # cur.execute(sql)
    # results = cur.fetchall()
    # id_did = {x[0]: x[1] for x in results}
    # connection.close()
    connection = pymysql.connect(host='localhost',  # host属性
                                 port=3306,
                                 user='root',  # 用户名
                                 password='mysql',  # 此处填登录数据库的密码
                                 db='rdf_metadata',  # 数据库名
                                 )
    cur = connection.cursor()
    sql = 'SELECT lzy_id, primary_id FROM outlink LIMIT 10;'
    cur.execute(sql)
    results = cur.fetchall()
    map_dict = {x[0]: x[1] for x in results}
    print(map_dict)
    try:
        connection.commit()
        connection.close()
    except Exception as e:
        # 如果发生错误则回滚
        print(e)
        connection.rollback()

def test_re():
    cop = re.compile('[0-9’!"#$%&\'()*+,-./:;<=>?@，。?★†§、…【】《》？“”‘’！[\\]^_`{|}~\s]+')  # 匹配特殊字符
    string1 = 'Órgãos do estado de São Paulo em RDF123'
    string2 = cop.sub('', string1)  # 将string1中匹配到的字符替换成空字符
    print(string2)
    print(string2.lower())
    cop = re.compile('<[^>]*>')
    string1 = 'see <a href="https://wwwn.cdc.gov/nndss/document/Users_guide_WONDER_tables_cleared_final.pdf">https://wwwn.cdc.gov/nndss/document/Users_guide_WONDER_tables_cleared_fi...</a>.<br />'
    string2 = cop.sub('', string1)  # 将string1中匹配到的字符替换成空字符
    print(string2)

    cop = re.compile('(20)(0|1|2)\d{1}')  # 匹配特殊字符
    string1 = 'Órgãos do estado de São Paulo em RDF2012 ffff2555 dddd1989'
    string2 = cop.sub('', string1)  # 将string1中匹配到的字符替换成空字符
    print(string2)
    print(string2.lower())


def test():
    l = [[1, 5], [4, 7, 9, 8]]
    G = nx.Graph()
    # for i in l:
    #     for j in range(len(i) - 1):
    #         G.add_edge(i[j], i[j + 1], test='ss')
    # G.add_node(11)
    # G.add_node(12)
    # G.add_edge(11, 12, col={'a': 'red'})
    # G.add_edge(11, 12, col='red')
    # G.nodes[11][0] = 1
    # G.add_node(20)
    G1 = nx.complete_graph(l[1])
    G2 = nx.Graph()
    G2.add_edges_from([(4,5),(1,5),(4,7)], count=2)
    G1.nodes[4]['a'] = 0
    G2.nodes[4]['a'] = 1
    G = nx.compose(G1, G2)
    G = nx.DiGraph()
    G.add_edges_from(list(G1.edges()), source='G1')
    G.add_edges_from(G2.edges(data=True))
    # degree = nx.degree_histogram(G)
    # x = range(len(degree))  # 生成x轴序列，从1到最大度
    # y = [z / float(sum(degree)) for z in degree]
    # plt.bar(x, y)
    # plt.show()
    # version_list = [[[1,2,3],[4,5],[6]], [[7], [9]]]
    # for version in version_list:
    #     diff = set([item for sublist in version for item in sublist])
    #     for same in version:
    #         diff = diff - set(same)
    #         edges = [(x, y) for x in same for y in diff]
    #         G.add_edges_from(edges)
    # G.add_node(15)
    # print('diameter: ', nx.diameter(G))  # 直径
    print(nx.closeness_centrality(G))
    print(list(G.edges(data=True)))
    print(list(G.nodes(data=True)))
    print(G.degree)
    print(nx.degree(G, weight='count'))
    # print(G.in_degree)
    # print(G.out_degree)
    # print(list(nx.connected_components(G)))
    print(G.number_of_nodes())
    print(G.number_of_edges())
    nx.draw(G, with_labels=True, font_weight='bold')
    plt.show()


if __name__ == '__main__':
    test()
