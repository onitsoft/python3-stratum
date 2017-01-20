u'''
    Implements simple interface to a coin daemon's RPC.
'''

from __future__ import absolute_import
import simplejson as json
import base64
from twisted.internet import defer
from twisted.web import client
import time

import lib.logger
log = lib.logger.get_logger(u'bitcoin_rpc')


class BitcoinRPC(object):

    def __init__(self, host, port, username, password):
        log.debug(u"Got to Bitcoin RPC")
        self.bitcoin_url = u'http://%s:%d' % (host, port)
        self.credentials = base64.b64encode(u"%s:%s" % (username, password))
        self.headers = {
            u'Content-Type': u'text/json',
            u'Authorization': u'Basic %s' % self.credentials,
        }
        client.HTTPClientFactory.noisy = False
        self.has_submitblock = False

    def _call_raw(self, data):
        client.Headers
        return client.getPage(
            url=self.bitcoin_url,
            method=u'POST',
            headers=self.headers,
            postdata=data,
        )

    def _call(self, method, params):
        return self._call_raw(json.dumps({
            u'jsonrpc': u'2.0',
            u'method': method,
            u'params': params,
            u'id': u'1',
        }))

    @defer.inlineCallbacks
    def check_submitblock(self):
        try:
            log.info(u"Checking for submitblock")
            resp = (yield self._call(u'submitblock', []))
            self.has_submitblock = True
        except Exception, e:
            if (unicode(e) == u"404 Not Found"):
                log.debug(u"No submitblock detected.")
                self.has_submitblock = False
            elif (unicode(e) == u"500 Internal Server Error"):
                log.debug(u"submitblock detected.")
                self.has_submitblock = True
            else:
                log.debug(u"unknown submitblock check result.")
                self.has_submitblock = True
        finally:
            defer.returnValue(self.has_submitblock)

    @defer.inlineCallbacks
    def submitblock(self, block_hex, hash_hex, scrypt_hex):
      # try 5 times? 500 Internal Server Error could mean random error or that
      # TX messages setting is wrong
        attempts = 0
        while True:
            attempts += 1
            if self.has_submitblock:
                try:
                    log.debug(
                        u"Submitting Block with submitblock: attempt #" +
                        unicode(attempts))
                    log.debug([block_hex, ])
                    resp = (yield self._call(u'submitblock', [block_hex, ]))
                    log.debug(u"SUBMITBLOCK RESULT: %s", resp)
                    break
                except Exception, e:
                    if attempts > 4:
                        log.exception(
                            u"submitblock failed. Problem Submitting block %s" %
                            unicode(e))
                        log.exception(u"Try Enabling TX Messages in config.py!")
                        raise
                    else:
                        continue
            elif self.has_submitblock == False:
                try:
                    log.debug(
                        u"Submitting Block with getblocktemplate submit: attempt #" +
                        unicode(attempts))
                    log.debug([block_hex, ])
                    resp = (yield self._call(u'getblocktemplate', [{u'mode': u'submit', u'data': block_hex}]))
                    break
                except Exception, e:
                    if attempts > 4:
                        log.exception(
                            u"getblocktemplate submit failed. Problem Submitting block %s" %
                            unicode(e))
                        log.exception(u"Try Enabling TX Messages in config.py!")
                        raise
                    else:
                        continue
            else:  # self.has_submitblock = None; unable to detect submitblock, try both
                try:
                    log.debug(u"Submitting Block with submitblock")
                    log.debug([block_hex, ])
                    resp = (yield self._call(u'submitblock', [block_hex, ]))
                    break
                except Exception, e:
                    try:
                        log.exception(
                            u"submitblock Failed, does the coind have submitblock?")
                        log.exception(u"Trying GetBlockTemplate")
                        resp = (yield self._call(u'getblocktemplate', [{u'mode': u'submit', u'data': block_hex}]))
                        break
                    except Exception, e:
                        if attempts > 4:
                            log.exception(
                                u"submitblock failed. Problem Submitting block %s" %
                                unicode(e))
                            log.exception(
                                u"Try Enabling TX Messages in config.py!")
                            raise
                        else:
                            continue

        if json.loads(resp)[u'result'] is None:
            # make sure the block was created.
            log.info(u"CHECKING FOR BLOCK AFTER SUBMITBLOCK")
            defer.returnValue((yield self.blockexists(hash_hex, scrypt_hex)))
        else:
            defer.returnValue(False)

    @defer.inlineCallbacks
    def getinfo(self):
        resp = (yield self._call(u'getinfo', []))
        defer.returnValue(json.loads(resp)[u'result'])

    @defer.inlineCallbacks
    def getblocktemplate(self):
        try:
            resp = (yield self._call(u'getblocktemplate', [{}]))
            defer.returnValue(json.loads(resp)[u'result'])
        # if internal server error try getblocktemplate without empty {} #
        # ppcoin
        except Exception, e:
            if (unicode(e) == u"500 Internal Server Error"):
                resp = (yield self._call(u'getblocktemplate', []))
                defer.returnValue(json.loads(resp)[u'result'])
            else:
                raise

    @defer.inlineCallbacks
    def prevhash(self):
        resp = (yield self._call(u'getwork', []))
        try:
            defer.returnValue(json.loads(resp)[u'result'][u'data'][8:72])
        except Exception, e:
            log.exception(u"Cannot decode prevhash %s" % unicode(e))
            raise

    @defer.inlineCallbacks
    def validateaddress(self, address):
        resp = (yield self._call(u'validateaddress', [address, ]))
        defer.returnValue(json.loads(resp)[u'result'])

    @defer.inlineCallbacks
    def getdifficulty(self):
        resp = (yield self._call(u'getdifficulty', []))
        defer.returnValue(json.loads(resp)[u'result'])

    @defer.inlineCallbacks
    def blockexists(self, hash_hex, scrypt_hex):
        valid_hash = None
        blockheight = None
        # try both hash_hex and scrypt_hex to find block
        try:
            resp = (yield self._call(u'getblock', [hash_hex, ]))
            result = json.loads(resp)[u'result']
            if u"hash" in result and result[u'hash'] == hash_hex:
                log.debug(u"Block found: %s" % hash_hex)
                valid_hash = hash_hex
                if u"height" in result:
                    blockheight = result[u'height']
                else:
                    defer.returnValue(True)
            else:
                log.info(u"Cannot find block for %s" % hash_hex)
                defer.returnValue(False)

        except Exception, e:
            try:
                resp = (yield self._call(u'getblock', [scrypt_hex, ]))
                result = json.loads(resp)[u'result']
                if u"hash" in result and result[u'hash'] == scrypt_hex:
                    valid_hash = scrypt_hex
                    log.debug(u"Block found: %s" % scrypt_hex)
                    if u"height" in result:
                        blockheight = result[u'height']
                    else:
                        defer.returnValue(True)
                else:
                    log.info(u"Cannot find block for %s" % scrypt_hex)
                    defer.returnValue(False)

            except Exception, e:
                log.info(
                    u"Cannot find block for hash_hex %s or scrypt_hex %s" %
                    hash_hex, scrypt_hex)
                defer.returnValue(False)

        # after we've found the block, check the block with that height in the
        # blockchain to see if hashes match
        try:
            log.debug(
                u"checking block hash against hash of block height: %s",
                blockheight)
            resp = (yield self._call(u'getblockhash', [blockheight, ]))
            hash = json.loads(resp)[u'result']
            log.debug(u"hash of block of height %s: %s", blockheight, hash)
            if hash == valid_hash:
                log.debug(
                    u"Block confirmed: hash of block matches hash of blockheight")
                defer.returnValue(True)
            else:
                log.debug(
                    u"Block invisible: hash of block does not match hash of blockheight")
                defer.returnValue(False)

        except Exception, e:
            # cannot get blockhash from height; block was created, so return
            # true
            defer.returnValue(True)
        else:
            log.info(u"Cannot find block for %s" % hash_hex)
            defer.returnValue(False)
