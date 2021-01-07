import requests
import pymysql
import datetime


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


def test():
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
            name = res[1][ind1+2:ind2]
        else:
            name = res[1][ind1+2:]
        sql = 'update ckan set name=\'{}\' where id={}'.format(name, res[0])
        cur.execute(sql)
    try:
        connection.commit()
        connection.close()
        cur.close()
        print('success')
    except Exception as e:
        print(e)


if __name__ == '__main__':
    test_sql()
