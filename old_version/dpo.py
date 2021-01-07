import pandas as pd  # data processing, CSV file I/O (e.g. pd.read_csv)
import requests

CSV_PATH = 'D:\\dataset\\dpo\\portals.csv'
SAVE_PATH = 'dpo.csv'


def verify_access(read_path, save_path):
    df1 = pd.read_csv(read_path)
    nRow, nCol = df1.shape
    valid_cnt = 0
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
        df1.iloc[i]['status_code'] = status_code
    df1.to_csv(save_path)
    return


def test():
    df1 = pd.read_csv('D:\\dataset\\dpo\\portals.csv', delimiter=',')
    df1.dataframeName = 'portals.csv'
    nRow, nCol = df1.shape
    print(f'There are {nRow} rows and {nCol} columns')
    print(df1.columns)
    print(df1.head(5))
    print(df1.iloc[0]['url'])
    # for indexs in df1.columns:
    #     print(indexs, df1.at[3, indexs])
    return


def extract_same(path1, path2):
    df1 = pd.read_csv(path1)
    df2 = pd.read_csv(path2)
    return


if __name__ == '__main__':
    # verify_access(CSV_PATH, SAVE_PATH)
    test()
