u''' A simple wrapper for pylibmc. It can be overwritten with simple hashing if necessary '''
from __future__ import absolute_import
import lib.settings as settings
import lib.logger
log = lib.logger.get_logger(u'Cache')

import pylibmc


class Cache(object):

    def __init__(self):
        # Open a new connection
        self.mc = pylibmc.Client(
            [settings.MEMCACHE_HOST + u":" + unicode(settings.MEMCACHE_PORT)], binary=True)
        log.info(u"Caching initialized")

    def set(self, key, value, time=settings.MEMCACHE_TIMEOUT):
        return self.mc.set(settings.MEMCACHE_PREFIX + unicode(key), value, time)

    def get(self, key):
        return self.mc.get(settings.MEMCACHE_PREFIX + unicode(key))

    def delete(self, key):
        return self.mc.delete(settings.MEMCACHE_PREFIX + unicode(key))

    def exists(self, key):
        return unicode(key) in self.mc.get(settings.MEMCACHE_PREFIX + unicode(key))
