import requests
import csv
import json
import pandas as pd  # data processing, CSV file I/O (e.g. pd.read_csv)
from bs4 import BeautifulSoup  # 从bs4引入BeautifulSoup

URL = "https://ckan.github.io/ckan-instances/"  # URL不变
HTML_PATH = "data/ckan.html"
CSV_PATH = "data/ckan.csv"
PORTAL_NUM = 199


def get_html(url, filename):
    # 新增伪装成浏览器的header
    fake_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36'
    }
    proxies = {"http": None, "https": None}
    response = requests.get(url, headers=fake_headers, proxies=proxies)  # 请求参数里面把假的请求header加上
    # 保存网页到本地
    # file_obj = open(filename, 'w')  # 以写模式打开文件
    # file_obj.write(response.content.decode('utf-8'))  # 把响应的html内容
    # file_obj.close()  # 关闭文件，结束写入
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(response.content.decode('utf-8'))
    return


def get_portal(html_path, save_path):
    # 读取文件内容到html变量里面
    file_obj = open(html_path, 'r', encoding='utf-8')  # 以读方式打开文件
    html = file_obj.read()  # 把文件的内容全部读取出来并赋值给html变量
    soup = BeautifulSoup(html, 'lxml')  # 初始化BeautifulSoup
    save_obj = open(save_path, 'w', encoding='utf-8', newline='')
    csv_write = csv.writer(save_obj)
    # 先写入columns_name
    csv_write.writerow(['title', 'description', 'place', 'url'])
    valid_cnt = 0
    requests.packages.urllib3.disable_warnings()
    for i in range(1, PORTAL_NUM + 1):
        print('now get ' + str(i))
        selector = '#instances > article:nth-child(' + str(i) + ') > '
        # 获取'title', 'description', 'place', 'url'
        portal_name = soup.select(selector + 'h2')[0].text
        portal_url = soup.select(selector + 'p.description')[0].text
        portal_api = soup.select(selector + 'p.meta')[0].text
        portal_searchable = soup.select(selector + 'a')[0].get('href')
        # print(portal_url)
        # # 判断Homepage能否访问
        # try:
        #     r = requests.get(portal_url, timeout=100, verify=False)
        # # except requests.exceptions.ProxyError:
        # except Exception:
        #     print(portal_url, ' get error')
        #     status_code = 0
        # else:
        #     status_code = r.status_code
        #     if status_code == 200:
        #         valid_cnt += 1
        #         print('valid: ', valid_cnt, '/', i)
        row = [portal_name, portal_url, portal_api, portal_searchable]
        csv_write.writerow(row)
    file_obj.close()  # 关闭文件对象
    save_obj.close()
    return


def get_portal_from_json():
    json_path = 'data/socrata.html'
    csv_path = 'data/socrata2.csv'
    with open(json_path, 'r', encoding='utf-8') as fin, open(csv_path, 'w', encoding='utf-8', newline='') as fout:
        dic = json.load(fin)
        csv_write = csv.writer(fout)
        csv_write.writerow(['domain', 'count', 'url', 'status_code'])
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'}
        proxies = {"http": None, "https": None}
        for item in dic['results']:
            url = 'http://' + item['domain']
            status_code = 0
            print(url)
            try:
                response = requests.get(url, headers=headers, proxies=proxies)
                status_code = response.status_code
                if status_code == 200:
                    url = response.url
            except Exception as e:
                print(e)
            csv_write.writerow([item['domain'], item['count'], url, status_code])


def test():
    get_portal_from_json()


if __name__ == '__main__':
    # get_html(URL, HTML_PATH)
    # get_portal(HTML_PATH, CSV_PATH)
    test()
