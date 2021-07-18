"""
说明：此脚本用于执行redis增删改查
作者：huangjunhao
日期：2021-05-19
"""

import redis
from redis import ConnectionError


class RedisOperation(object):

    def __init__(self, redis_db, redis_host, redis_port, password=None):
        """
        :param redis_db: db号码(0-15)
        :param redis_host: ip
        :param redis_port: 端口
        :param password: 密码
        """
        if password:
            redis_pool = redis.ConnectionPool(db=redis_db, host=redis_host, port=redis_port, password=password)
            self._db = redis.StrictRedis(connection_pool=redis_pool)
        else:
            redis_pool = redis.ConnectionPool(db=redis_db, host=redis_host, port=redis_port)
            self._db = redis.StrictRedis(connection_pool=redis_pool)

    def set(self, name, value):
        """
        string:设置给定key的值.如果key已经存储其他值,SET就覆写旧值,无视该key的数据类型
        :param name: db的key值
        :param value: value
        :return: 返回True代表set成功
        """
        return self._db.set(name, value)

    def get(self, name):
        """
        string:用于获取指定key的值.如果key不存在,返回None
        :param name: db的key值
        :return: value
        """
        return self._db.get(name).decode()

    def sadd(self, name, value):
        """
        set:判定数据在set中是否重复
        :param name: db的key值
        :param value: value
        :return: 1代表数据不存在并加入了数据,0代表数据已存在
        """
        return self._db.sadd(name, value)

    def scard(self, name):
        """
        set:返回集合中元素的数量.
        :param name: db的key值
        :return: 集合的数量.当集合 key 不存在时，返回 0.
        """
        return self._db.scard(name)

    def srem(self, name, *value):
        """
        set:删除set中某值
        :param name: db的key值
        :param value: 想删除的value值,可以有多个(必须为list类型)
        :return: 1代表数据存在并已删除,0代表数据不存在
        """
        return self._db.srem(name, *value)

    def sscan_iter(self, name):
        """
        set:迭代获取
        :param name: db的key值
        :return: generator
        """
        return self._db.sscan_iter(name)

    def lpop(self, name):
        """
        list:左弹出
        :param name: db的key值
        :return: value
        """
        return self._db.lpop(name).decode()

    def lpush(self, name, value):
        """
        list:左插入
        :param name: db的key值
        :param data_str: value
        :return: 当前key中的value数量
        """
        return self._db.lpush(name, value)

    def lrange(self, name):
        """
        list:迭代获取
        :param name: db的key值
        :return:
        """
        start = 0
        end = 1000
        while True:
            data = self._db.lrange(name, start, end)
            if not data:
                break
            for im in data:
                yield im
            start += 1000
            end += 1000

    def lset(self, name, index, value):
        """
        通过索引来设置元素的值
        :param name: db的key值
        :param index: 位置
        :param value: value
        :return: 操作成功返回True,否则返回错误信息。
        """
        return self._db.lset(name, index, value)

    def llen(self, name):
        """
        list: 用于返回列表的长度.如果列表key不存在,则 key被解释为一个空列表,返回 0.如果 key不是列表类型,返回一个错误.
        :param name: db的key值
        :return: 列表的长度
        """
        return self._db.llen(name)

    def lrem(self, name, value, count=1):
        """
        list:删除某value值
        :param name: db的key值
        :param value: 想删除的value值
        :param count: 默认为1,正数表示从表头开始向表尾搜索,移除与VALUE相等的元素,数量为 COUNT
        :return: 被移除元素的数量,列表不存在该元素时返回0.
        """
        return self._db.lrem(name, count, value)

    def hset(self, name, key, field):
        """
        hash:插入hash值
        :param name: db的key值
        :param key: hash结构的key值
        :param field: hash结构的field值
        :return: hash结构的field值已存在则返回0,不存在返回1
        """
        return self._db.hset(name, key, field)

    def hget(self, name, field):
        """
        hash:获取hash值
        :param name: db的key值
        :param field: hash结构的field值
        :return: hash结构的value值,不存在则返回None
        """
        temp = self._db.hget(name, str(field))
        if temp is not None:
            return temp.decode()

    def hscan_iter(self, name):
        """
        hash:迭代获取数据
        :param name: db的key值
        :return: generator
        """
        return self._db.hscan_iter(name)

    def hlen(self, name):
        """
        hash:获取哈希表中字段的数量
        :param name: db的key值
        :return: 哈希表中字段的数量.当 key不存在时,返回 0.
        """
        return self._db.hlen(name)

    def hdel(self, name, *field):
        """
        :param name: db的key值
        :param key: 想删除的field值,可以有多个(必须为list类型)
        :return: 被成功删除field的数量
        """
        return self._db.hdel(name, *field)

    def zadd(self, name, mapping):
        """
        zset:插入数据
        :param name: db的key值
        :param mapping: dict {value:score}
        :return: 新增value的个数
        """
        return self._db.zadd(name, mapping)

    def zrank(self, name, value):
        """
        zset:查找数据排名,由小到大排序
        :param name: db的key值
        :param value: 该value的名称
        :return: 该value的排名
        """
        return self._db.zrank(name, value)

    def zscore(self, name, value):
        """
        zscore:查找数据分数
        :param name: db的key值
        :param value: 该value的名称
        :return: 该value的分数
        """
        return self._db.zscore(name, value)

    def zcard(self, name):
        """
        用于计算集合中元素的数量
        :param name: db的key值
        :return: 当 key存在且是有序集类型时,返回有序集的基数.当 key不存在,返回 0.
        """
        return self._db.zcard(name)

    def zscan_iter(self, name):
        """
        zset:迭代获取数据
        :param name: db的key值
        :return: generator
        """
        return self._db.zscan_iter(name)

    def zrem(self, name, *value):
        """
        zset: 删除一个或多个key
        :param name: db的key值
        :param value: 移除成员的名称,可以有多个(必须为list类型)
        :return: 被成功移除的成员的数量
        """
        return self._db.zrem(name, *value)

    def scan_all_keys(self):
        """
        遍历所有的KEY,统计数量分布
        :return: {key:number}
        """
        key_number_dict = dict()  # 不同业务类型的key的数量
        scan_cursor = 0
        # 统计遍历key的数量
        scan_num = 0
        while True:
            key_list = self._db.scan(scan_cursor, count=10000)  # 分片为10000条数据
            scan_cursor = key_list[0]
            scan_num = scan_num + len(key_list[1])
            for key_name in key_list[1]:
                key_name = transform_key_name_to_simple(key_name.decode())
                key_number_dict[key_name] = key_number_dict.get(key_name, 0)+1
            if scan_cursor == 0:
                return key_number_dict

    def get_type(self, name):
        """
        获得key的数据结构
        :param name: db的key值
        :return: key的数据类型,key不存在返回None
        """
        key_type = self._db.type(name).decode()
        if key_type == 'none':
            return None
        return key_type

    def ping(self):
        """
        测试是否可以连通redis
        :return:
        """
        self._db.ping()

    def delete(self, name):
        """
        删除某key
        :param name: db的key值
        :return: 返回1代表成功删除,返回0代表key不存在
        """
        return self._db.delete(name)


def test_connection(ip, port, password, db=0):
    try:
        RedisOperation(db, ip, port, password).ping()
        return True
    except ConnectionError:
        return False


def transform_key_name_to_simple(key_name):
    """
    key命名规则和统计规则之间的转换
    :return: 统计命名
    """
    return key_name.split(":")[0]


if __name__ == '__main__':
    x = RedisOperation(redis_db='2', redis_host='localhost', redis_port=6379, password='')
    for i in range(10):
        x.lpush('list', str(i))
        x.sadd('set', str(i))
        x.zadd('zset', {str(i): i})
        x.hset('hash', str(i), str(i))
