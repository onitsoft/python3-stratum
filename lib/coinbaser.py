from __future__ import absolute_import
from . import util
from twisted.internet import defer

from . import settings

import lib.logger
log = lib.logger.get_logger(u'coinbaser')

# TODO: Add on_* hooks in the app


class SimpleCoinbaser(object):
    u'''This very simple coinbaser uses constant bitcoin address
    for all generated blocks.'''

    def __init__(self, bitcoin_rpc, address):
        log.debug(u"Got to coinbaser")
        # Fire Callback when the coinbaser is ready
        self.on_load = defer.Deferred()

        self.address = address
        self.is_valid = False

        self.bitcoin_rpc = bitcoin_rpc
        self._validate()

    def _validate(self):
        d = self.bitcoin_rpc.validateaddress(self.address)
        d.addCallback(self.address_check)
        d.addErrback(self._failure)

    def address_check(self, result):
        if result[u'isvalid'] and result[u'ismine']:
            self.is_valid = True
            log.info(u"Coinbase address '%s' is valid" % self.address)
            if u'address' in result:
                log.debug(u"Address = %s " % result[u'address'])
                self.address = result[u'address']
            if u'pubkey' in result:
                log.debug(u"PubKey = %s " % result[u'pubkey'])
                self.pubkey = result[u'pubkey']
            if u'iscompressed' in result:
                log.debug(u"Is Compressed = %s " % result[u'iscompressed'])
            if u'account' in result:
                log.debug(u"Account = %s " % result[u'account'])
            if not self.on_load.called:
                self.address = result[u'address']
                self.on_load.callback(True)

        elif result[u'isvalid'] and settings.ALLOW_NONLOCAL_WALLET:
            self.is_valid = True
            log.warning(
                u"!!! Coinbase address '%s' is valid BUT it is not local" %
                self.address)
            if u'pubkey' in result:
                log.debug(u"PubKey = %s " % result[u'pubkey'])
                self.pubkey = result[u'pubkey']
            if u'account' in result:
                log.debug(u"Account = %s " % result[u'account'])
            if not self.on_load.called:
                self.on_load.callback(True)

        else:
            self.is_valid = False
            log.error(u"Coinbase address '%s' is NOT valid!" % self.address)

        # def on_new_block(self):
    #    pass

    # def on_new_template(self):
    #    pass
    def _failure(self, failure):
        log.exception(u"Cannot validate Wallet address '%s'" % self.address)
        raise

    def get_script_pubkey(self):
        if settings.COINDAEMON_Reward == u'POW':
            self._validate()
            return util.script_to_address(self.address)
        else:
            return util.script_to_pubkey(self.pubkey)

    def get_coinbase_data(self):
        return u''
