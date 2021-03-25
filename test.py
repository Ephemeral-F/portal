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
    # string = 'http://www.adere.org.ar/123'
    # ind1 = string.find('//')
    # ind2 = string.find('/', ind1+2)
    # print(ind1, ind2)
    # s2 = string[:0]
    # print(s2)
    connection = pymysql.connect(host='localhost',  # host属性
                                 port=3306,
                                 user='root',  # 用户名
                                 password='mysql',  # 此处填登录数据库的密码
                                 db='portals',  # 数据库名
                                 )
    cur = connection.cursor()
    cur.execute('select id,url from ckan')
    results = cur.fetchall()
    for res in results:
        ind1 = res[1].find('//')
        ind2 = res[1].find('/', ind1 + 2)
        if ind2 > 0:
            name = res[1][ind1 + 2:ind2]
        else:
            name = res[1][ind1 + 2:]
        sql = 'update ckan set name=\'{}\' where id={}'.format(name, res[0])
        cur.execute(sql)
    try:
        connection.commit()
        connection.close()
        cur.close()
        print('success')
    except Exception as e:
        print(e)


def test_re():
    cop = re.compile('[0-9’!"#$%&\'()*+,-./:;<=>?@，。?★†§、…【】《》？“”‘’！[\\]^_`{|}~\s]+')  # 匹配特殊字符
    string1 = 'Órgãos do estado de São Paulo em RDF123'
    string2 = cop.sub('', string1)  # 将string1中匹配到的字符替换成空字符
    print(string2)
    print(hash(string2))
    print(string2.lower())
    print(hash(string2.lower()))
    cop = re.compile('<[^>]*>')
    string1 = 'see <a href="https://wwwn.cdc.gov/nndss/document/Users_guide_WONDER_tables_cleared_final.pdf">https://wwwn.cdc.gov/nndss/document/Users_guide_WONDER_tables_cleared_fi...</a>.<br />'
    string2 = cop.sub('', string1)  # 将string1中匹配到的字符替换成空字符
    print(string2)
    print(hash(string2))



def test():
    l = [[1, 5], [4, 7, 9]]
    G = nx.Graph()
    for i in l:
        for j in range(len(i)-1):
            G.add_edge(i[j], i[j+1])
    G.add_node(11)
    nx.draw(G, with_labels=True, font_weight='bold')
    plt.show()
    for i in nx.connected_components(G):
        print(i)
    cc = list(nx.connected_components(G))
    print(cc)
    print(len(cc), max(cc, key=len))


if __name__ == '__main__':
    test()
