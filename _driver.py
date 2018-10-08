"""PytSite Redis Cache Driver
"""
__author__ = 'Oleksandr Shepetko'
__email__ = 'a@shepetko.com'
__license__ = 'MIT'

import pickle as _pickle
from typing import Any as _Any, Mapping as _Mapping, List as _List, Generator as _Generator, Optional as _Optional
from redis import StrictRedis as _StrictRedis, exceptions as _redis_error
from pytsite import cache as _cache, reg as _reg, router as _router

_server_name = _router.server_name()


class Redis(_cache.driver.Abstract):
    """Redis Cache Driver
    """

    def __init__(self):
        """Init
        """
        self._host = _reg.get('redis.host', 'localhost')
        self._port = _reg.get('redis.port', 6379)
        self._client = _StrictRedis(self._host, self._port)

    @staticmethod
    def _fqkn(pool: str, key: str) -> str:
        """Get fully qualified key name
        """
        return _server_name + ':' + pool + ':' + key

    def keys(self, pool: str) -> _Generator[str, None, None]:
        """Get all keys of the pool
        """
        key_prefix = _server_name + ':' + pool + ':'
        for k in self._client.keys(key_prefix + '*'):
            yield k.decode('utf-8').replace(key_prefix, '')

    def has(self, pool: str, key: str) -> bool:
        """Check whether an item exists in the pool
        """
        return self._client.exists(self._fqkn(pool, key))

    def put(self, pool: str, key: str, value: _Any, ttl: int = None) -> _Any:
        """Put an item into the pool
        """
        self._client.set(self._fqkn(pool, key), _pickle.dumps(value), ttl)

        return value

    def get(self, pool: str, key: str) -> _Any:
        """Get an item from the pool
        """
        v = self._client.get(self._fqkn(pool, key))
        if v is None:
            raise _cache.error.KeyNotExist(pool, key)

        return _pickle.loads(v)

    def put_hash(self, pool: str, key: str, value: _Mapping, ttl: int = None) -> _Mapping:
        """Put a hash
        """
        # Redis does not store empty hashes, so we need to mark empty hashes from our side
        if not value:
            value = {'__pytsite_empty_hash_marker': True}

        key = self._fqkn(pool, key)

        self._client.hmset(key, {k: _pickle.dumps(v) for k, v in value.items()})

        if ttl:
            self._client.expire(key, ttl)

        return value

    def put_hash_item(self, pool: str, key: str, item_key: str, value: _Any) -> _Any:
        """Put a value into a hash
        """
        self._client.hset(self._fqkn(pool, key), item_key, _pickle.dumps(value))

        return value

    def get_hash(self, pool: str, key: str, hash_keys: _List[str] = None) -> _Mapping:
        """Get a hash
        """
        key = self._fqkn(pool, key)
        values = self._client.hmget(key, hash_keys) if hash_keys else self._client.hvals(key)

        if not hash_keys:
            hash_keys = self._client.hkeys(key)

        r = {}
        for i in range(len(values)):
            v = values[i]

            if v is None:
                raise _cache.error.HashKeyNotExists(pool, key, hash_keys[i])

            k = hash_keys[i].decode('utf-8')

            # Redis does not store empty hashes, so we need to mark empty hashes from our side and then remove them
            if k != '__pytsite_empty_hash_marker':
                r[k] = _pickle.loads(v)

        return r

    def get_hash_item(self, pool: str, key: str, item_key: str, default=None) -> _Any:
        """Get a single value from a hash
        """
        r = self._client.hget(self._fqkn(pool, key), item_key)

        return _pickle.loads(r) if r else default

    def rm_hash_item(self, pool: str, key: str, item_key: str):
        """Remove a value from a hash
        """
        self._client.hdel(key, item_key)

    def l_push(self, pool: str, key: str, value: _Any) -> int:
        """Push a value into beginning of a list
        """
        return self._client.lpush(key, _pickle.dumps(value))

    def r_pop(self, pool: str, key: str) -> _Any:
        """Pop an item from the end of a list
        """
        v = self._client.rpop(key)
        if not v:
            raise _cache.error.KeyNotExist(pool, key)

        return _pickle.loads(v)

    def ttl(self, pool: str, key: str) -> _Optional[int]:
        """Get key's expiration time
        """
        key = self._fqkn(pool, key)

        r = self._client.ttl(key)

        if r == -2:
            raise _cache.error.KeyNotExist(pool, key)

        return r if r >= 0 else None

    def rnm(self, pool: str, key: str, new_key: str):
        """Rename a key
        """
        try:
            self._client.rename(self._fqkn(pool, key), self._fqkn(pool, new_key))

        except _redis_error.ResponseError as e:
            if str(e) == 'no such key':
                raise _cache.error.KeyNotExist(pool, key)
            else:
                raise e

    def rm(self, pool: str, key: str):
        """Remove a single item from the pool
        """
        self._client.delete(self._fqkn(pool, key))

    def cleanup(self, pool: str):
        """Cleanup outdated items from the pool
        """
        # Do nothing. Redis maintains garbage collection by itself
        pass

    def clear(self, pool: str):
        """Clear entire pool
        """
        for key in self._client.keys(_server_name + ':' + pool + ':*'):
            self._client.delete(key)
