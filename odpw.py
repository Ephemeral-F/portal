import pymysql
import requests
import csv
import json

MAYBE_CSV_PATH = 'data/maybe_ckan.csv'

def normalize_name():
    connection = pymysql.connect(host='localhost',  # host属性
                                 port=3306,
                                 user='root',  # 用户名
                                 password='mysql',  # 此处填登录数据库的密码
                                 db='portals',  # 数据库名
                                 )
    cur = connection.cursor()
    cur.execute('select id,url from odpw')
    results = cur.fetchall()
    for res in results:
        # 提取url【http://.../】的...部分作为name
        ind1 = res[1].find('//')
        ind2 = res[1].find('/', ind1 + 2)
        if ind2 > 0:
            name = res[1][ind1+2:ind2]
        else:
            name = res[1][ind1+2:]
        sql = 'update odpw set name=\'{}\' where id={}'.format(name, res[0])
        cur.execute(sql)
    try:
        connection.commit()
        connection.close()
        cur.close()
        print('success')
    except Exception as e:
        print(e)


def classify_portal():
    # 'name', 'url'
    connection = pymysql.connect(host='localhost',  # host属性
                                 port=3306,
                                 user='root',  # 用户名
                                 password='mysql',  # 此处填登录数据库的密码
                                 db='portals',  # 数据库名
                                 )
    cur = connection.cursor()
    # api为ckan的，并且没爬过
    # cur.execute('select `name`,`url` from odpwckan WHERE `name` not in (SELECT `name` FROM crawled  UNION SELECT '
    #             '`name` FROM failed)')
    # results = cur.fetchall()
    # print(len(results))
    # cur.execute('select `name`,`url` from odpw where `generator` is null and `api_endpoint` is null and `api_type` is null')
    # results = cur.fetchall()
    # print(len(results))
    cur.execute(
        'select `name`,`url`, `generator`, `api_endpoint`, `api_type` from odpwnot')
    results = cur.fetchall()
    csv_path = MAYBE_CSV_PATH
    with open(csv_path, 'w', encoding='utf-8', newline='') as fout:
        csv_write = csv.writer(fout)
        csv_write.writerow(['name', 'url', 'api_type'])
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'}
        proxies = {"http": None, "https": None}
        for res in results:
            name, url, api_url = res[0], res[1], res[1]+'/api/3/action/package_search?rows=1&start=0'
            print(name, url, end=' ')
            try:
                response = requests.get(api_url, headers=headers, proxies=proxies)
                status_code = response.status_code
                print(status_code)
                if status_code == 200:
                    api_type = 'CKAN'
                else:
                    generator, api_endpoint, api_type = res[2], res[3], res[4]
                    if api_type is not None:
                        continue
                    elif generator is not None:
                        api_type = generator
                    elif api_endpoint is not None:
                        api_type = api_endpoint
            except Exception as e:
                print(e)
                api_type = None
            csv_write.writerow([name, url, api_type])


if __name__ == '__main__':
    classify_portal()

