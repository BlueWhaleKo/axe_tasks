import ast
from collections.abc import Iterable
import json

import redis
from redis.exceptions import DataError, ResponseError


class Redis:
    def __init__(self, host, port):
        self.host = host
        self.port = port

        self._conn = None

    @property
    def conn(self):
        if self._conn is None:
            self._conn = redis.Redis(host=self.host, port=self.port, db=0)
        return self._conn

    def execute_command(self, cmd):
        self.conn.execute_command(cmd)

    # set all in one
    def set_all(self, key, value, overwrite=True, ex=None, **kwargs):
        if overwrite:
            self.conn.delete(key)

        try:
            if isinstance(value, (str, int, float, bytes)):
                self.conn.set(key, value, **kwargs)
            elif isinstance(value, list):
                self.conn.rpush(key, *value, **kwargs)
            elif isinstance(value, set):
                self.conn.sadd(key, *value, **kwargs)
            elif isinstance(value, dict):
                self.conn.hmset(key, value, **kwargs)
        except Exception as e:
            try:  # serialize as json and dump
                self.conn.set(key, json.dumps(value), **kwargs)
            except TypeError as e:
                print(e)
        finally:
            if not ex is None:
                self.conn.expire(key, ex)

    def get_all(self, key, decode=True):
        dtype = self.conn.type(key).decode()
        if dtype == "none":
            value = None
        elif dtype == "list":
            value = self.conn.lrange(key, start=0, end=-1)
        elif dtype == "set":
            value = self.conn.smembers(key)
        elif dtype == "hash":
            value = self.conn.hgetall(key)
        elif dtype == "string":
            value = self.conn.get(key)
        try:
            value = self._decode(value)
        except:
            pass

        return value

    def _decode(self, value):
        if isinstance(value, bytes):
            value = value.decode()
        elif isinstance(value, (list, set)):
            value = [v.decode() for v in value]
        elif isinstance(value, dict):
            value = {k.decode(): v.decode() for k, v in value.items()}
        return value

    def keys(self, pattern="*"):
        return self.conn.keys(pattern)

    def scan_iter(self, pattern="*", count=100):
        keys = self.conn.scan_iter(match=pattern, count=count)
        return [k.decode() for k in keys]

    def rpush(self, key, *value):
        self.conn.rpush(key, *value)

    def lrange(self, key, start=0, end=-1):
        value = self.conn.lrange(key, start=start, end=end)
        return [v.decode() for v in value]

    def flushall(self):
        self.conn.flushall()
