"""PytSite Redis Cache Driver
"""
import pickle as _pickle
from typing import Any as _Any, Mapping as _Mapping, List as _List
from redis import StrictRedis as _StrictRedis, exceptions as _redis_error
from pytsite import cache as _cache, reg as _reg, logger as _logger, router as _router

__author__ = 'Alexander Shepetko'
__email__ = 'a@shepetko.com'
__license__ = 'MIT'

_server_name = _router.server_name()


class Redis(_cache.driver.Abstract):
    """Redis Cache Driver
    """

    def __init__(self, pool_uid: str):
        """Init
        """
        super().__init__(pool_uid)

        self._host = _reg.get('redis.host', 'localhost')
        self._port = _reg.get('redis.port', 6379)
        self._client = _StrictRedis(self._host, self._port)

    def _get_fq_key(self, key: str) -> str:
        """Get fully qualified key name
        """
        return _server_name + ':' + self._pool_uid + ':' + key

    def has(self, key: str) -> bool:
        """Check whether an item exists in the pool
        """
        return self._client.exists(self._get_fq_key(key))

    def put(self, key: str, value: _Any, ttl: int = None) -> _Any:
        """Put an item into the pool
        """
        self._client.set(self._get_fq_key(key), _pickle.dumps(value), ttl)

        return value

    def get(self, key: str) -> _Any:
        """Get an item from the pool
        """
        v = self._client.get(self._get_fq_key(key))
        if v is None:
            raise _cache.error.KeyNotExist(self._pool_uid, key)

        return _pickle.loads(v)

    def put_hash(self, key: str, value: _Mapping, ttl: int = None) -> _Mapping:
        """Put a hash
        """
        key = self._get_fq_key(key)

        self._client.hmset(key, {k: _pickle.dumps(v) for k, v in value.items()})

        if ttl:
            self._client.expire(key, ttl)

        return value

    def put_hash_item(self, key: str, item_key: str, value: _Any) -> _Any:
        """Put a value into a hash
        """
        self._client.hset(self._get_fq_key(key), item_key, _pickle.dumps(value))

        return value

    def get_hash(self, key: str, hash_keys: _List[str] = None) -> _Mapping:
        """Get a hash
        """
        key = self._get_fq_key(key)
        values = self._client.hmget(key, hash_keys) if hash_keys else self._client.hvals(key)

        if not hash_keys:
            hash_keys = self._client.hkeys(key)

        r = {}
        for k in range(len(values)):
            v = values[k]

            if v is None:
                raise _cache.error.HashKeyNotExists(self._pool_uid, key, hash_keys[k])

            r[hash_keys[k]] = _pickle.loads(v)

        return r

    def get_hash_item(self, key: str, item_key: str) -> _Any:
        """Get a single value from a hash
        """
        key = self._get_fq_key(key)

        r = self._client.hget(key, item_key)
        if r is None:
            raise _cache.error.HashKeyNotExists(self._pool_uid, key, item_key)

        return _pickle.loads(r)

    def rm_hash_item(self, key: str, item_key: str):
        """Remove a value from a hash
        """
        self._client.hdel(key, item_key)

    def l_push(self, key: str, value: _Any) -> int:
        """Push a value into beginning of a list
        """
        return self._client.lpush(key, _pickle.dumps(value))

    def r_pop(self, key: str) -> _Any:
        """Pop an item from the end of a list
        """
        v = self._client.rpop(key)
        if not v:
            raise _cache.error.KeyNotExist(self._pool_uid, key)

        return _pickle.loads(v)

    def ttl(self, key: str) -> int:
        """Get key's expiration time
        """
        key = self._get_fq_key(key)

        r = self._client.ttl(key)
        if r == -1:
            raise _cache.error.KeyNeverExpires(self._pool_uid, key)
        if r == -2:
            raise _cache.error.KeyNotExist(self._pool_uid, key)

        return r

    def rnm(self, key: str, new_key: str):
        """Rename a key
        """
        try:
            self._client.rename(key, new_key)
        except _redis_error.ResponseError as e:
            if str(e) == 'no such key':
                raise _cache.error.KeyNotExist(self._pool_uid, key)
            else:
                raise e

    def rm(self, key: str):
        """Remove a single item from the pool
        """
        self._client.delete(self._get_fq_key(key))

    def clear(self):
        """Clear entire pool
        """
        for key in self._client.keys(_server_name + ':' + self._pool_uid + ':*'):
            self._client.delete(key)

        if _reg.get('cache.debug'):
            _logger.debug("Pool '{}' CLEARED.".format(self._pool_uid))

    def cleanup(self):
        """Cleanup outdated items from the cache
        """
        # Do nothing. Redis maintains garbage collection by itself
        pass
