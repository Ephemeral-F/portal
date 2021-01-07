import json
import pandas as pd

PATH = "E:\\dataset\\lod-data.json"


def test():
    with open(PATH, 'r') as f:
        data = json.load(f)
        print(len(data))
        print(data["universal-dependencies-treebank-english-lines"].keys())


def load_google():
    # 希望电脑没事.jpg
    path = "E:\\dataset\\google_dataset_search\\dataset_metadata_2020_10_16.csv"
    with open(path, 'rb') as f:
        for i, l in enumerate(f):
            pass
        print(i)


if __name__ == '__main__':
    test()
    # load_google()