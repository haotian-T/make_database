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
    cur.execute('DROP TABLE IF EXISTS User')
    create = """
        CREATE TABLE `User`(
        `user_picture` VARCHAR(255) DEFAULT NULL,
        `uid_domain` VARCHAR(255) NOT NULL,
        `user_name` VARCHAR(255) DEFAULT '手机号',
        `is_watched` int DEFAULT 0 COMMENT '是否看过'        
        )
    """  # PRIMARY KEY (`uid_domain`) USING BTREE
    cur.execute(create)
    print('User 表格创建成功')
except pymysql.Error as err:
    print(err)
    print("User 表格创建失败")
try:
    with open('reviews.csv', 'r', encoding='utf8') as csv_reader:
        reader = csv.reader(csv_reader)
        datas = list(reader)
    print('共' + str(len(datas)) + '条评论')
    time.sleep(1)
    sql = "INSERT INTO User(user_picture, uid_domain, user_name, is_watched) VALUES (%s,%s,%s,%s)"

    value = []
    for data in tqdm(datas, desc='准备数据中'):
        value.append((data[1], data[1], data[2], 1))
    cur.executemany(sql, value)
    # 提交到数据库执行
    db.commit()
    print("User 插入数据成功")
except pymysql.Error as err:
    print(err)
    print("User 插入数据失败")
    db.rollback()
try:
    results = cur.fetchall()
    print("查询成功")
except pymysql.Error as err:
    print(err)
    print("查询失败")
db.close()
