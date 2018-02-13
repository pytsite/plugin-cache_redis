"""PytSite Cache Redis Driver Plugin
"""

__author__ = 'Alexander Shepetko'
__email__ = 'a@shepetko.com'
__license__ = 'MIT'


def plugin_load():
    from pytsite import reg, cache
    from . import _driver

    if reg.get('cache_redis.enabled', True):
        cache.set_driver(_driver.Redis())
