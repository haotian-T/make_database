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
    cur.execute('DROP TABLE IF EXISTS Movie')
    create = """
        CREATE TABLE `Movie`(
        `score` float DEFAULT NULL COMMENT '评分',
        `picture_url` VARCHAR(255) DEFAULT NULL,
        `is_playable` int DEFAULT NULL,
        `id` int NOT NULL,
        `types` VARCHAR(255) DEFAULT NULL,
        `regions` VARCHAR(255) DEFAULT NULL,
        `title` VARCHAR(255) NOT NULL,
        `url` VARCHAR(255) DEFAULT NULL,
        `release_date` VARCHAR(255) DEFAULT NULL COMMENT '上映时间',
        `actor_count` int DEFAULT NULL COMMENT '参演人数',
        `vote_count` int DEFAULT NULL COMMENT '评分人数',
        `actor` TEXT(65535) DEFAULT NULL,
        `is_watched` int DEFAULT NULL COMMENT '是否看过',
        PRIMARY KEY (`id`) USING BTREE
        )
    """
    cur.execute(create)
    print('Movie 表格创建成功')
except pymysql.Error as err:
    print(err)
    print("Movie 表格创建失败")
try:
    csv_reader = csv.reader(open(r"movie.csv", 'r', encoding='utf8'))
    datas = set()
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
            line['rank'] = int(line['rank'])
            line['is_playable'] = 1 if line['is_playable'] is True else 0
            line['actor_count'] = int(line['actor_count'])
            line['vote_count'] = int(line['vote_count'])
            line['is_watched'] = 1 if line['is_playable'] is True else 0
            line['score'] = float(line['score'])
            line['types'] = list(eval(line['types']))
            line['regions'] = list(eval(line['regions']))
            line['actors'] = list(eval(line['actors']))
            chars = line['types'][0]
            for j in range(1, len(line['types'])):
                chars += ',' + line['types'][j]
            line['types'] = chars
            if len(line['regions']) == 0:
                line['regions'] = ''
            else:
                chars = line['regions'][0]
                for j in range(1, len(line['regions'])):
                    chars += ',' + line['regions'][j]
                line['regions'] = chars
            if len(line['actors']) == 0:
                line['actors'] = ''
            else:
                chars = line['actors'][0]
                for j in range(1, len(line['actors'])):
                    chars += ',' + line['actors'][j]
                line['actors'] = chars
            line = Datas(line)
            datas.add(line)
    print('共' + str(len(datas)) + '部电影')
    time.sleep(1)
    sql = "INSERT INTO Movie(score, picture_url, is_playable, id, types,regions,title,url," \
          "release_date,actor_count,vote_count,actor,is_watched) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    datas = list(datas)
    value = []
    for data in tqdm(datas, desc='准备数据中'):
        data = data.item
        value.append((data['score'], data['cover_url'], data['is_playable'], data['id'], data['types'], data['regions'],
                      data['title'], data['url'], data['release_date'], data['actor_count'], data['vote_count'],
                      data['actors'], data['is_watched']))
    cur.executemany(sql, value)
    # 提交到数据库执行
    db.commit()
    print("Movie 插入数据成功")
except pymysql.Error as err:
    print(err)
    print("Movie 插入数据失败")
    db.rollback()
try:
    results = cur.fetchall()
    print("查询成功")
except pymysql.Error as err:
    print(err)
    print("查询失败")
db.close()
