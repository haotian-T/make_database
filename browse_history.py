import pymysql
import json
import time
from tqdm import tqdm
import csv

DBhost = 'localhost'
DBuser = 'root'
DBname = 'petal'
DBpwd = 'shujuku2021'


class Datas:
    def __init__(self, item):
        self.item = item

    def __eq__(self, other):
        if self.item['id'] == other.item['id']:
            return True
        else:
            return False

    def __hash__(self) -> int:
        return self.item['id']

    def __str__(self):
        return str(self.item)

    def keys(self):
        return ('rank', 'cover_url', 'is_playable', 'id', 'types', 'regions', 'title', 'url', 'release_date',
                'actor_count', 'vote_count', 'score', 'actors', 'is_watched')

    def __getitem__(self, item):
        """内置方法, 当使用obj['name']的形式的时候, 将调用这个方法, 这里返回的结果就是值"""
        return getattr(self, item)


try:
    db = pymysql.connect(user=DBuser, password=DBpwd, host=DBhost, database=DBname)
    print("数据库链接成功")
except pymysql.Error as err:
    print(err)
    print("数据库链接失败")
try:
    cur = db.cursor()
    cur.execute('DROP TABLE IF EXISTS Browse_history')
    create = """
        CREATE TABLE `Browse_history`(
        `uid` VARCHAR(255) DEFAULT NULL,
        `mid` VARCHAR(255) DEFAULT NULL  
        )
    """  # PRIMARY KEY (`uid_domain`) USING BTREE
    cur.execute(create)
    print('Browse_history 表格创建成功')
except pymysql.Error as err:
    print(err)
    print("Browse_history 表格创建失败")

db.close()
