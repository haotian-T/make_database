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
    cur.execute('DROP TABLE IF EXISTS type_movie')
    create = """
        CREATE TABLE `type_movie`(
        `id` int NOT NULL,
        `type` VARCHAR (255) DEFAULT NULL
        )
    """
    cur.execute(create)
    print('type_movie 表格创建成功')
except pymysql.Error as err:
    print(err)
    print("type_movie 表格创建失败")
try:
    csv_reader = csv.reader(open(r"movie.csv", 'r', encoding='utf8'))
    datas = []
    keys = []
    reader = list(csv_reader)
    for i in range(len(reader)):
        if i == 0:
            keys = reader[i][1:]
            # print(keys)
        else:
            if len(reader[i]) != 15:
                line = reader[i][2:]
            else:
                line = reader[i][1:]
            line = dict(zip(keys, line))
            line['id'] = int(line['id'])
            line['types'] = list(eval(line['types']))
            for type in line['types']:
                datas.append([line['id'], type])

    print('共' + str(len(datas)))
    time.sleep(1)
    sql = "INSERT INTO type_movie(id,type) VALUES (%s,%s)"

    value = []
    for data in tqdm(datas, desc='准备数据中'):
        value.append((data[0], data[1]))
    cur.executemany(sql, value)
    # 提交到数据库执行
    db.commit()
    print("type_movie 插入数据成功")
except pymysql.Error as err:
    print(err)
    print("type_movie 插入数据失败")
    db.rollback()
try:
    results = cur.fetchall()
    print("查询成功")
except pymysql.Error as err:
    print(err)
    print("查询失败")
db.close()
