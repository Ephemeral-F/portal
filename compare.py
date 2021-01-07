import pandas as pd
import numpy as np
import requests
import csv

CSV_PATH_1 = 'E:\\python\\dpo.csv'
CSV_PATH_2 = 'E:\\python\\wu.csv'

INTERSECTION_PATH = 'E:\\python\\portal\\intersection.csv'
DIFFERENCE_PATH_1 = 'E:\\python\\portal\\only_in_dpo.csv'
DIFFERENCE_PATH_2 = 'E:\\python\\portal\\only_in_wu.csv'


PORTALS_PATH = 'E:\\python\\portals.csv'


def verify_access(read_path, save_path):
    df1 = pd.read_csv(read_path)
    nRow, nCol = df1.shape
    valid_cnt = 0
    status_list = []
    requests.packages.urllib3.disable_warnings()
    for i in range(nRow):
        portal_url = df1.iloc[i]['url']
        try:
            r = requests.get(portal_url, timeout=100, verify=False)
        # except requests.exceptions.ProxyError:
        except Exception:
            print(portal_url, ' get error')
            status_code = 0
        else:
            status_code = r.status_code
            if status_code == 200:
                valid_cnt += 1
                print('valid: ', valid_cnt, '/', i + 1)
        status_list.append(status_code)
    df1.insert(nCol, 'status_code', np.array(status_list))
    df1.to_csv(save_path)
    return


def overview_csv(path1, path2):
    df1 = pd.read_csv(path1)
    df2 = pd.read_csv(path2)
    print(f'df1 shape: {df1.shape}; df2 shape: {df2.shape}')
    print(f'df1 columns: {df1.columns}')
    print(f'df2 columns: {df2.columns}')
    print('df1 head(1)')
    print(df1.loc[0, :])
    print('df2 head(1)')
    print(df2.loc[0, :])
    return


'''
df1 columns: Index(['Unnamed: 0', 'name', 'title', 'url', 'author', 'publisher', 'issued',
       'publisher_classification', 'description', 'tags', 'license_id',
       'license_url', 'place', 'location', 'country', 'language', 'status',
       'metadatacreated', 'generator', 'api_endpoint', 'api_type',
       'full_metadata_download', 'status_code'],
      dtype='object')
url：key。
status：3种取值，active/inactive/static，划分依据是？
api_endpoint：意义不明，似乎是给出了api前缀？
api_type: 给出了类似CKAN的值或者api文档的url，但是只有87/594有记录
full_metadata_download：意义不明，似乎是package_list？

df2 columns: Index(['name', 'url', 'api_url', 'api_type', 'status_code'], dtype='object')
url：key。
api_url：意义不明，一部分数据给的是rdf文件的下载链接，或许rdf文件就是网站的resource合集
api_type: 给出了类似CKAN的值
'''


def statistic_csv(path1, path2, same_path, diff_path1, diff_path2):
    df1 = pd.read_csv(path1, index_col=0)
    df2 = pd.read_csv(path2)
    # 取交集
    intersection = pd.merge(left=df1, right=df2, how='inner', on='url', left_on=None, right_on=None,
                            left_index=False, right_index=False, sort=True, suffixes=('_dpo', '_wu'), copy=True,
                            indicator=False)
    intersection.to_csv(same_path)
    # 取差集
    temp = df1.append(intersection)
    difference_df1 = temp.drop_duplicates(subset=['url'], keep=False)
    difference_df1.loc[:, df1.columns].to_csv(diff_path1)
    temp = df2.append(intersection)
    difference_df2 = temp.drop_duplicates(subset=['url'], keep=False)
    difference_df2.loc[:, df2.columns].to_csv(diff_path2)


'''
intersection: 32/40
only_in_dpo: 336/548
only_in_wu: 145/163
total = 32 + 336 + 145 = 513
'''


def drop_unnamed(path):
    df1 = pd.read_csv(path)
    newData = df1.loc[:, ~df1.columns.str.contains('^Unnamed')]
    newData.to_csv(path, index=False)


def test():
    df1 = pd.read_csv('E:\\python\\portals.csv')
    print(df1.shape)


if __name__ == '__main__':
    # verify_access(PORTALS_PATH, CSV_PATH_1)
    overview_csv(CSV_PATH_1, CSV_PATH_2)
    # statistic_csv(CSV_PATH_1, CSV_PATH_2, INTERSECTION_PATH, DIFFERENCE_PATH_1, DIFFERENCE_PATH_2)
    # test()

'''
查找相同的：merge

pd.merge(left, right, how='inner', on=None, left_on=None, right_on=None,
      left_index=False, right_index=False, sort=True,
      suffixes=('_x', '_y'), copy=True, indicator=False)
      left与right：两个不同的DataFrame
      
how：指的是合并(连接)的方式有inner(内连接),left(左外连接),right(右外连接),outer(全外连接);默认为inner
on : 指的是用于连接的列索引名称。必须存在右右两个DataFrame对象中，如果没有指定且其他参数也未指定则以两个DataFrame的列名交集做为连接键
left_on：左则DataFrame中用作连接键的列名;这个参数中左右列名不相同，但代表的含义相同时非常有用。
right_on：右则DataFrame中用作 连接键的列名
left_index：使用左则DataFrame中的行索引做为连接键
right_index：使用右则DataFrame中的行索引做为连接键
sort：默认为True，将合并的数据进行排序。在大多数情况下设置为False可以提高性能
suffixes：字符串值组成的元组，用于指定当左右DataFrame存在相同列名时在列名后面附加的后缀名称，默认为('_x','_y')
copy：默认为True,总是将数据复制到数据结构中；大多数情况下设置为False可以提高性能
indicator：在 0.17.0中还增加了一个显示合并数据中来源情况；如只来自己于左边(left_only)、两者(both)
      

假设相同的dataframe叫same
temp = df1.append(same)
df1_only = temp.drop_duplicates(subset=['url'],keep=False)

DataFrame.drop_duplicates(subset=None, keep='first', inplace=False)

subset : column label or sequence of labels, optional
用来指定特定的列，默认所有列
keep : {‘first’, ‘last’, False}, default ‘first’
删除重复项并保留第一次出现的项
inplace : boolean, default False
是直接在原来数据上修改还是保留一个副本

'''
