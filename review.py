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
    cur.execute('DROP TABLE IF EXISTS Review')
    create = """
        CREATE TABLE `Review`(
        `score` int DEFAULT 3,
        `rid` int NOT NULL,
        `uid` VARCHAR(255) DEFAULT '手机号',
        `mid` int NOT NULL,
        `content` TEXT(500) DEFAULT NULL COMMENT '少于500字'        
        )
    """  # PRIMARY KEY (`rid`) USING BTREE
    cur.execute(create)
    print('Review 表格创建成功')
except pymysql.Error as err:
    print(err)
    print("Review 表格创建失败")
try:
    with open('reviews.csv', 'r', encoding='utf8') as csv_reader:
        reader = csv.reader(csv_reader)
        datas = list(reader)
    print('共' + str(len(datas)) + '条评论')
    time.sleep(1)
    sql = "INSERT INTO Review(score, rid, uid, mid, content) VALUES (%s,%s,%s,%s,%s)"
    value = []
    for i, data in tqdm(enumerate(datas), desc='准备数据中'):
        value.append((data[3] if data[3] != '' else 3, i, data[1], data[0], data[4]))
    cur.executemany(sql, value)
    # 提交到数据库执行
    db.commit()
    print("Review 插入数据成功")
except pymysql.Error as err:
    print(err)
    print("Review 插入数据失败")
    db.rollback()
try:
    results = cur.fetchall()
    print("查询成功")
except pymysql.Error as err:
    print(err)
    print("查询失败")
db.close()
