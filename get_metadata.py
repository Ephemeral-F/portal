import requests
import pymysql


portal_list = [
    # ("annuario.comune.fi.it", "https://data.comune.fi.it/datastore/api/package_search"),
    # ("opendata.aragon.es", "https://opendata.aragon.es/api/3/action/package_search"),
    # ("data.gov.hk", "https://data.gov.hk/tc-data/api/3/action/package_search"),
    # ("open.nrw", "https://open.nrw/api/3/action/package_search"),
    # # ("data.london.gov.uk", "http://data.london.gov.uk/api/3/action/package_search"),
    # ("data.tainan.gov.tw", "http://data.tainan.gov.tw/api/3/action/package_search"),
    # ("datamx.io", "http://datamx.io/api/3/action/package_search"),
    # ("daten.buergernetz.bz.it", "http://daten.buergernetz.bz.it/de/api/3/action/package_search"),
    # ("dati.trentino.it", "http://dati.trentino.it/api/3/action/package_search"),
    # ("dati.veneto.it", "https://dati.veneto.it/SpodCkanApi/api/3/action/package_search"),
    # ("datosabiertos.malaga.eu", "http://datosabiertos.malaga.eu/api/3/action/package_search"),
    # ("www.datos.misiones.gov.ar", "http://www.datos.misiones.gov.ar/api/3/action/package_search"),
    # ("www.hri.fi", "https://hri.fi/data/api/3/action/package_search"),
    # ("data.yorkopendata.org", "https://data.yorkopendata.org/api/3/action/package_search"),
    # ("Avoindata.fi", "https://www.avoindata.fi/data/api/3/action/package_search"),
    # ("data.cityofboston.gov", "https://data.boston.gov/api/3/action/package_search"),
    # ("datosabiertos.rivasciudad.es", "https://datosabiertos.rivasciudad.es/api/3/action/package_search"),
    # ("data.illinois.gov", "https://data.illinois.gov/api/3/action/package_search"),
    ]


def get_json(url, row, start, retry=7):
    payload = {'rows': row, 'start': start}
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}
    proxies = {"http": None, "https": None}
    print("now get: " + url + "?rows=", row, "&start=", start, sep='')
    try:
        r = requests.get(url, timeout=1000, verify=False, params=payload, headers=headers)
    except Exception as e:
        if retry <= 0:
            print("error in get_json")
            print(e)
            return
        print("retry: ", retry)
        return get_json(url, row, start, retry-1)
    else:
        return r.text


def get_metadata(portal_name, portal_url):
    payload = {'rows': 1, 'start': 0}
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}
    proxies = {"http": None, "https": None}
    print("portal: ", portal_name, portal_url)
    try:
        r = requests.get(portal_url, timeout=1000, verify=False, params=payload, headers=headers)
    except Exception as e:
        print(e)
        print("error in get_metadata: ", portal_name, portal_url)
        return False
    else:
        dic = r.json()
        count = dic['result']['count']
        page_num = 100
        with open('json/' + portal_name, 'w', encoding='utf-8') as f:
            for i in range(count // page_num):
                text = get_json(portal_url, page_num, i * page_num)
                f.write(text)
                f.write('\n')
            last = count // page_num * page_num
            text = get_json(portal_url, count - last, last)
            f.write(text)
        print('get success')
        return True


def get_rest(portal_name, portal_url, start):
    payload = {'rows': 1, 'start': 0}
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}
    proxies = {"http": None, "https": None}
    try:
        r = requests.get(portal_url, timeout=1000, verify=False, params=payload, headers=headers, proxies=proxies)
    except Exception as e:
        print(e)
        print("error in get_metadata")
        return False
    else:
        dic = r.json()
        count = dic['result']['count']
        page_num = 100
        with open('json/' + portal_name, 'a+', encoding='utf-8') as f:
            for i in range(start, count // page_num):
                text = get_json(portal_url, page_num, i * page_num)
                f.write(text)
                f.write('\n')
            last = count // page_num * page_num
            text = get_json(portal_url, count - last, last)
            f.write(text)
        print('get success')
        return True


if __name__ == '__main__':
    requests.packages.urllib3.disable_warnings()
    requests.adapters.DEFAULT_RETRIES = 10

    connection = pymysql.connect(host='localhost',  # host属性
                                 port=3306,
                                 user='root',  # 用户名
                                 password='mysql',  # 此处填登录数据库的密码
                                 db='portals',  # 数据库名
                                 )
    cur = connection.cursor()
    cur.execute('select `name`, url, api_url FROM failed WHERE save_file=0')
    results = cur.fetchall()
    for res in results:
        is_successful = get_metadata(res[0], res[2]+'api/3/action/package_search')
        if is_successful:
            cur.execute("update failed set save_file=1 where `name` = '{}'".format(res[0]))
            connection.commit()

    # for portal in portal_list:
    #     get_metadata(portal[0], portal[1])
    # get_rest("daten.hamburg.de", "https://suche.transparenz.hamburg.de/api/3/action/package_search", 1097)


