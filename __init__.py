"""PytSite Cache Redis Driver Plugin
"""

__author__ = 'Alexander Shepetko'
__email__ = 'a@shepetko.com'
__license__ = 'MIT'


def plugin_load():
    from pytsite import cache
    from . import _cache_driver

    cache.set_driver(_cache_driver.Redis)
