import requests
import pymysql
import datetime

SOCRATA_API_URL = 'http://api.us.socrata.com/api/catalog/v1'
SOCRATA_PARA_KEY = '&limit=1&offset=0'
TABLE_SQL_PATH = 'resource/socrata.sql'
LIMIT = 100


class Socrata:
    def __init__(self, domain, count, url, status_code):
        self.domain = domain
        self.count = count
        self.url = url
        self.status_code = status_code
        self.connection = pymysql.connect(host='localhost',  # host属性
                                          port=3306,
                                          user='root',  # 用户名
                                          password='mysql',  # 此处填登录数据库的密码
                                          db='metadata',  # 数据库名
                                          )
        self.cur = self.connection.cursor()

    @staticmethod
    def get_single(dic, key):
        return dic.get(key, None)

    @staticmethod
    def get_array(dic, key):
        l1 = dic.get(key, [])
        s1 = ';'.join([str(x) for x in l1])
        if len(s1):  # s1!=''
            return s1
        else:
            return None

    @staticmethod
    def get_json_dic(params):
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}
        proxies = {"http": None, "https": None}
        try:
            r = requests.get(SOCRATA_API_URL, timeout=1000, verify=False, params=params, headers=headers)
            # print(r.url)
        except Exception as e:
            print(e)
            return None
        else:
            return r.json()

    @staticmethod
    def get_results(json_dic):
        return json_dic.get('results', None)

    def save_metadata(self, json_dic):
        results = self.get_results(json_dic)
        if results is None or len(results)==0:
            print('results is None or len(results)==0')
            return True
        sql = 'INSERT IGNORE INTO `{}`(`name`, `id`, `parent_fxf`, `description`, `attribution`, `attribution_link`, ' \
              '`contact_email`, `type`, `updatedAt`, `createdAt`, `metadata_updated_at`, `data_updated_at`, ' \
              '`page_views_last_week`, `page_views_last_month`, `page_views_total`, `page_views_last_week_log`, ' \
              '`page_views_last_month_log`, `page_views_total_log`, `columns_name`, `columns_field_name`, ' \
              '`columns_datatype`, `columns_description`, `columns_format`, `download_count`, `provenance`, ' \
              '`lens_view_type`, `blob_mime_type`, `hide_from_data_json`, `publication_date`, `categories`, `tags`, ' \
              '`domain_tags`, `domain_metadata`, `domain`, `license`, `permalink`, `link`, `owner_id`, `user_type`, ' \
              '`display_name`, `data_source`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,' \
              ' %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'\
            .format(self.domain)
        values = []
        for result in results:
            resource = self.get_resource(result)
            classification = self.get_classification(result)
            metadata = self.get_metadata(result)
            permalink = self.get_single(result, 'permalink')
            link = self.get_single(result, 'link')
            owner = self.get_owner(result)
            value = resource + classification + metadata + [permalink] + [link] + owner + [self.url]
            values.append(tuple(value))
        try:
            # 执行sql语句
            rows = self.cur.executemany(sql, values)
            # 提交到数据库执行
            self.connection.commit()
            print('insert {} rows'.format(rows))
            return True
        except Exception as e:
            # 如果发生错误则回滚
            print(e)
            self.connection.rollback()
            return False

    def get_resource(self, result_dic):
        dic = result_dic.get('resource', {})
        ret = []
        ret.append(self.get_single(dic, 'name'))
        ret.append(self.get_single(dic, 'id'))
        ret.append(self.get_array(dic, 'parent_fxf'))
        ret.append(self.get_single(dic, 'description'))
        ret.append(self.get_single(dic, 'attribution'))
        ret.append(self.get_single(dic, 'attribution_link'))
        ret.append(self.get_single(dic, 'contact_email'))
        ret.append(self.get_single(dic, 'type'))
        ret.append(self.get_single(dic, 'updatedAt'))
        ret.append(self.get_single(dic, 'createdAt'))
        ret.append(self.get_single(dic, 'metadata_updated_at'))
        ret.append(self.get_single(dic, 'data_updated_at'))
        page_views_dic = dic.get('page_views', {})
        ret.append(self.get_single(page_views_dic, 'page_views_last_week'))
        ret.append(self.get_single(page_views_dic, 'page_views_last_month'))
        ret.append(self.get_single(page_views_dic, 'page_views_total'))
        ret.append(self.get_single(page_views_dic, 'page_views_last_week_log'))
        ret.append(self.get_single(page_views_dic, 'page_views_last_month_log'))
        ret.append(self.get_single(page_views_dic, 'page_views_total_log'))
        ret.append(self.get_array(dic, 'columns_name'))
        ret.append(self.get_array(dic, 'columns_field_name'))
        ret.append(self.get_array(dic, 'columns_datatype'))
        ret.append(self.get_array(dic, 'columns_description'))
        ret.append(self.get_array(dic, 'columns_format'))
        ret.append(self.get_single(dic, 'download_count'))
        ret.append(self.get_single(dic, 'provenance'))
        ret.append(self.get_single(dic, 'lens_view_type'))
        ret.append(self.get_single(dic, 'blob_mime_type'))
        ret.append(self.get_single(dic, 'hide_from_data_json'))
        ret.append(self.get_single(dic, 'publication_date'))
        return ret

    def get_classification(self, result_dic):
        dic = result_dic.get('classification', {})
        ret = []
        ret.append(self.get_array(dic, 'categories'))
        ret.append(self.get_array(dic, 'tags'))
        ret.append(self.get_array(dic, 'domain_tags'))
        ret.append(self.get_array(dic, 'domain_metadata'))
        return ret

    def get_metadata(self, result_dic):
        dic = result_dic.get('metadata', {})
        ret = []
        ret.append(self.get_single(dic, 'domain'))
        ret.append(self.get_single(dic, 'license'))
        return ret

    def get_owner(self, result_dic):
        dic = result_dic.get('owner', {})
        ret = []
        ret.append(self.get_single(dic, 'id'))
        ret.append(self.get_single(dic, 'user_type'))
        ret.append(self.get_single(dic, 'display_name'))
        return ret

    def get_scroll_id(self, json_dic):
        results = self.get_results(json_dic)
        if len(results) == 0:
            return 0
        last_result = results[-1]
        last_resource = last_result.get('resource', {})
        return last_resource.get('id', None)

    def create_table(self):
        sql = 'CREATE TABLE IF NOT EXISTS `{}`'.format(self.domain)
        with open(TABLE_SQL_PATH, 'r', encoding='utf-8') as f:
            sql = sql + f.read()
        try:
            self.cur.execute(sql)
        except Exception as e:
            print(e)
        else:
            print('create table `{}` success'.format(self.domain))

    def close(self):
        self.connection.close()
        self.cur.close()

    # disused method, replaced by `def crawl_from_http_scroll(self)`
    def crawl_from_http(self):
        self.create_table()
        params = {'domains': self.domain, 'limit': LIMIT, 'offset': 0}
        for i in range(self.count // LIMIT + 1):
            offset = i * LIMIT
            params['offset'] = offset
            print('url: {}?domains={}&limit={}&offset={}'.format(SOCRATA_API_URL, params['domains'],
                                                                 params['limit'], params['offset']))
            json_dic = self.get_json_dic(params)
            retry = 5
            while json_dic is None and retry > 0:
                print('json_dic is None, retry')
                json_dic = self.get_json_dic(params)
                retry -= 1
            if json_dic is None:
                print('get fail')
                return False
            is_save_success = self.save_metadata(json_dic)
            if not is_save_success:
                print('save metadata failed!')
                return False
        self.close()
        return True

    def crawl_from_http_scroll(self):
        self.create_table()
        params = {'domains': self.domain, 'limit': LIMIT, 'scroll_id': ''}
        for i in range(self.count // LIMIT + 1):
            print('url: {}?domains={}&limit={}&scroll_id={}'.format(SOCRATA_API_URL, params['domains'],
                                                                    params['limit'], params['scroll_id']))
            json_dic = self.get_json_dic(params)
            retry = 5
            while json_dic is None and retry > 0:
                print('json_dic is None, retry')
                json_dic = self.get_json_dic(params)
                retry -= 1
            if json_dic is None:
                print('get fail')
                return False
            is_save_success = self.save_metadata(json_dic)
            if not is_save_success:
                print('save metadata failed!')
                return False
            scroll_id = self.get_scroll_id(json_dic)
            if scroll_id is None:
                print('get scroll_id failed')
                return False
            if scroll_id == 0:
                print('results is [], continue')
                continue
            params['scroll_id'] = scroll_id
        self.close()
        return True


def get_socrata_metadata():
    connection = pymysql.connect(host='localhost',  # host属性
                                 port=3306,
                                 user='root',  # 用户名
                                 password='mysql',  # 此处填登录数据库的密码
                                 db='portals',  # 数据库名
                                 )
    cur = connection.cursor()
    sql = 'SELECT domain, count, url, status_code FROM socrata ' \
          'WHERE domain not in (SELECT `name` FROM crawled UNION SELECT `name` FROM failed)  AND count>0'
    cur.execute(sql)
    portals = cur.fetchall()
    print('portals list: ', len(portals))
    for portal in portals:
        domain, count, url, status_code = portal[0], portal[1], portal[2], portal[3]
        api_url = SOCRATA_API_URL + '?domains=' + domain
        api_type = 'Socrata'
        print('now get: ', domain, count, url, status_code)
        so = Socrata(domain, count, url, status_code)
        # is_success = so.crawl_from_http()
        is_success = so.crawl_from_http_scroll()
        if is_success:
            print('save crawled table')
            crawled_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sql = 'INSERT INTO `crawled`(`name`, `url`, `api_type`, `api_url`, `parameter_key`, `return_count`, ' \
                  '`crawled_date`) VALUES (\'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\')' \
                .format(domain, url, api_type, api_url, SOCRATA_PARA_KEY, portal[1], crawled_date)
            cur.execute(sql)
        else:
            print('save failed table')
            sql = 'INSERT INTO `failed`(`name`, `url`, `api_url`, `api_type`) VALUES (\'{}\',\'{}\',\'{}\',\'{}\')' \
                .format(domain, url, api_url, api_type)
            cur.execute(sql)
        connection.commit()
        print()
    connection.close()
    cur.close()


def get_failed_10000():
    connection = pymysql.connect(host='localhost',  # host属性
                                 port=3306,
                                 user='root',  # 用户名
                                 password='mysql',  # 此处填登录数据库的密码
                                 db='portals',  # 数据库名
                                 )
    cur = connection.cursor()
    sql = 'SELECT domain, count, url, status_code FROM socrata ' \
          'WHERE domain in (SELECT `name` FROM failed) AND domain not in (SELECT `name` FROM crawled)'
    cur.execute(sql)
    portals = cur.fetchall()
    print('portals list: ', len(portals))
    for portal in portals:
        domain, count, url, status_code = portal[0], portal[1], portal[2], portal[3]
        api_url = SOCRATA_API_URL + '?domains=' + domain
        api_type = 'Socrata'
        print('now get: ', domain, count, url, status_code)
        so = Socrata(domain, count, url, status_code)
        is_success = so.crawl_from_http_scroll()
        if is_success:
            print('save crawled table')
            crawled_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sql = 'INSERT INTO `crawled`(`name`, `url`, `api_type`, `api_url`, `parameter_key`, `return_count`, ' \
                  '`crawled_date`) VALUES (\'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\')' \
                .format(domain, url, api_type, api_url, SOCRATA_PARA_KEY, portal[1], crawled_date)
            cur.execute(sql)
            connection.commit()
        else:
            print('still failed')
        print()
    connection.close()
    cur.close()


def test():
    # s = Socrata('data.nasa.gov', 27864,	'https://nasa.github.io/data-nasa-gov-frontpage/', 200)
    # params = {'domains': s.domain, 'limit': LIMIT, 'scroll_id': 'ywju-bk66'}
    pass


if __name__ == '__main__':
    # get_socrata_metadata()
    # get_failed_10000()
    test()

