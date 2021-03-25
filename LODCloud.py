import json
import pymysql

PATH = "E:\\dataset\\lod-data.json"


def test():
    with open(PATH, 'r') as f:
        data = json.load(f)
        print(len(data))
        print(type(data))
        print(data["universal-dependencies-treebank-english-lines"].keys())
        print(data["slod"]['owner'])


def load_google():
    # 希望电脑没事.jpg
    path = "E:\\dataset\\google_dataset_search\\dataset_metadata_2020_10_16.csv"
    with open(path, 'rb') as f:
        for i, l in enumerate(f):
            pass
        print(i)


class LODCloud:
    def __init__(self):
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
    def get_file_dic():
        with open(PATH, 'r') as f:
            data = json.load(f)
            return data

    def get_value(self, json_dic):
        value = []
        value.append(self.get_single(json_dic, '_id'))
        value.append(self.get_array(json_dic, 'sparql'))
        value.append(self.get_array(json_dic, 'example'))

        description_dic = self.get_single(json_dic, 'description')
        value.append(self.get_single(description_dic, 'en'))

        value.append(self.get_single(json_dic, 'image'))

        owner_dic = self.get_single(json_dic, 'owner')
        if isinstance(owner_dic, str):
            value.append(owner_dic)
            value.append(owner_dic)
        elif isinstance(owner_dic, dict):
            value.append(self.get_single(owner_dic, 'email'))
            value.append(self.get_single(owner_dic, 'name'))

        owner_dic = self.get_single(json_dic, 'contact_point')
        value.append(self.get_single(owner_dic, 'email'))
        value.append(self.get_single(owner_dic, 'name'))

        value.append(self.get_array(json_dic, 'full_download'))
        value.append(self.get_array(json_dic, 'keywords'))
        value.append(self.get_array(json_dic, 'other_download'))
        value.append(self.get_single(json_dic, 'namespace'))
        value.append(self.get_single(json_dic, 'identifier'))
        value.append(self.get_single(json_dic, 'license'))
        value.append(self.get_array(json_dic, 'links'))
        value.append(self.get_single(json_dic, 'title'))
        value.append(self.get_single(json_dic, 'website'))
        value.append(self.get_single(json_dic, 'doi'))
        value.append(self.get_single(json_dic, 'domain'))
        value.append(self.get_single(json_dic, 'triples'))

        return value

    def save_metadata(self):
        sql = 'INSERT INTO `lod-cloud.net`(`_id`, `sparql`, `example`, `description`, `image`, ' \
              '`owner_email`, `owner_name`, `contact_point_email`, `contact_point_name`, `full_download`, `keywords`,' \
              ' `other_download`, `namespace`, `identifier`, `license`, `links`, `title`, `website`, `doi`, `domain`,' \
              ' `triples`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ' \
              '%s, %s, %s, %s, %s, %s, %s, %s, %s);'
        values = []
        file_dic = self.get_file_dic()
        for id in file_dic.keys():
            json_dic = file_dic[id]
            value = self.get_value(json_dic)
            values.append(value)
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


if __name__ == '__main__':
    # test()
    lod = LODCloud()
    lod.save_metadata()
