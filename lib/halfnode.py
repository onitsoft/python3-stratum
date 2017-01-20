#!/usr/bin/python
# Public Domain
# Original author: ArtForz
# Twisted integration: slush

from __future__ import absolute_import
import struct
import socket
import binascii
import time
import sys
import random
import io
from Crypto.Hash import SHA256

from twisted.internet.protocol import Protocol
from .util import *
from . import settings

import lib.logger
log = lib.logger.get_logger(u'halfnode')
log.debug(u"Got to Halfnode")

if settings.COINDAEMON_ALGO == u'scrypt':
    log.debug(u"########################################### Loading LTC Scrypt #########################################################")
    import ltc_scrypt
elif settings.COINDAEMON_ALGO == u'quark':
    log.debug(u"########################################### Loading Quark Support #########################################################")
    import quark_hash
else:
    log.debug(u"########################################### Loading SHA256 Support ######################################################")

# if settings.COINDAEMON_Reward == 'POS':
#        log.debug("########################################### Loading POS Support #########################################################")
#        pass
# else:
#        log.debug("########################################### Loading POW Support ######################################################")
#        pass

if settings.COINDAEMON_TX == u'yes':
    log.debug(u"########################################### Loading SHA256 Transaction Message Support #########################################################")
    pass
else:
    log.debug(u"########################################### NOT Loading SHA256 Transaction Message Support ######################################################")
    pass


MY_VERSION = 31402
MY_SUBVERSION = u".4"


class CAddress(object):

    def __init__(self):
        self.nTime = 0
        self.nServices = 1
        self.pchReserved = u"\x00" * 10 + u"\xff" * 2
        self.ip = u"0.0.0.0"
        self.port = 0

    def deserialize(self, f):
        #self.nTime = struct.unpack("<I", f.read(4))[0]
        self.nServices = struct.unpack(u"<Q", f.read(8))[0]
        self.pchReserved = f.read(12)
        self.ip = socket.inet_ntoa(f.read(4))
        self.port = struct.unpack(u">H", f.read(2))[0]

    def serialize(self):
        r = u""
        #r += struct.pack("<I", self.nTime)
        r += struct.pack(u"<Q", self.nServices)
        r += self.pchReserved
        r += socket.inet_aton(self.ip)
        r += struct.pack(u">H", self.port)
        return r

    def __repr__(self):
        return u"CAddress(nServices=%i ip=%s port=%i)" % (
            self.nServices, self.ip, self.port)


class CInv(object):
    typemap = {
        0: u"Error",
        1: u"TX",
        2: u"Block"}

    def __init__(self):
        self.type = 0
        self.hash = 0

    def deserialize(self, f):
        self.type = struct.unpack(u"<i", f.read(4))[0]
        self.hash = deser_uint256(f)

    def serialize(self):
        r = u""
        r += struct.pack(u"<i", self.type)
        r += ser_uint256(self.hash)
        return r

    def __repr__(self):
        return u"CInv(type=%s hash=%064x)" % (
            self.typemap[self.type], self.hash)


class CBlockLocator(object):

    def __init__(self):
        self.nVersion = MY_VERSION
        self.vHave = []

    def deserialize(self, f):
        self.nVersion = struct.unpack(u"<i", f.read(4))[0]
        self.vHave = deser_uint256_vector(f)

    def serialize(self):
        r = u""
        r += struct.pack(u"<i", self.nVersion)
        r += ser_uint256_vector(self.vHave)
        return r

    def __repr__(self):
        return u"CBlockLocator(nVersion=%i vHave=%s)" % (
            self.nVersion, repr(self.vHave))


class COutPoint(object):

    def __init__(self):
        self.hash = 0
        self.n = 0

    def deserialize(self, f):
        self.hash = deser_uint256(f)
        self.n = struct.unpack(u"<I", f.read(4))[0]

    def serialize(self):
        r = u""
        r += ser_uint256(self.hash)
        r += struct.pack(u"<I", self.n)
        return r

    def __repr__(self):
        return u"COutPoint(hash=%064x n=%i)" % (self.hash, self.n)


class CTxIn(object):

    def __init__(self):
        self.prevout = COutPoint()
        self.scriptSig = u""
        self.nSequence = 0

    def deserialize(self, f):
        self.prevout = COutPoint()
        self.prevout.deserialize(f)
        self.scriptSig = deser_string(f)
        self.nSequence = struct.unpack(u"<I", f.read(4))[0]

    def serialize(self):
        r = u""
        r += self.prevout.serialize()
        r += ser_string(self.scriptSig)
        r += struct.pack(u"<I", self.nSequence)
        return r

    def __repr__(self):
        return u"CTxIn(prevout=%s scriptSig=%s nSequence=%i)" % (
            repr(self.prevout), binascii.hexlify(self.scriptSig), self.nSequence)


class CTxOut(object):

    def __init__(self):
        self.nValue = 0
        self.scriptPubKey = u""

    def deserialize(self, f):
        self.nValue = struct.unpack(u"<q", f.read(8))[0]
        self.scriptPubKey = deser_string(f)

    def serialize(self):
        r = u""
        r += struct.pack(u"<q", self.nValue)
        r += ser_string(self.scriptPubKey)
        return r

    def __repr__(self):
        return u"CTxOut(nValue=%i.%08i scriptPubKey=%s)" % (
            self.nValue // 100000000, self.nValue % 100000000, binascii.hexlify(self.scriptPubKey))


class CTransaction(object):

    def __init__(self):
        if settings.COINDAEMON_Reward == u'POW':
            self.nVersion = 1
            if settings.COINDAEMON_TX == u'yes':
                self.nVersion = 2
            self.vin = []
            self.vout = []
            self.nLockTime = 0
            self.sha256 = None
        elif settings.COINDAEMON_Reward == u'POS':
            self.nVersion = 1
            if settings.COINDAEMON_TX == u'yes':
                self.nVersion = 2
            self.nTime = 0
            self.vin = []
            self.vout = []
            self.nLockTime = 0
            self.sha256 = None
        if settings.COINDAEMON_TX == u'yes':
            self.strTxComment = u""

    def deserialize(self, f):
        if settings.COINDAEMON_Reward == u'POW':
            self.nVersion = struct.unpack(u"<i", f.read(4))[0]
            self.vin = deser_vector(f, CTxIn)
            self.vout = deser_vector(f, CTxOut)
            self.nLockTime = struct.unpack(u"<I", f.read(4))[0]
            self.sha256 = None
        elif settings.COINDAEMON_Reward == u'POS':
            self.nVersion = struct.unpack(u"<i", f.read(4))[0]
            self.nTime = struct.unpack(u"<i", f.read(4))[0]
            self.vin = deser_vector(f, CTxIn)
            self.vout = deser_vector(f, CTxOut)
            self.nLockTime = struct.unpack(u"<I", f.read(4))[0]
            self.sha256 = None
        if settings.COINDAEMON_TX == u'yes':
            self.strTxComment = deser_string(f)

    def serialize(self):
        if settings.COINDAEMON_Reward == u'POW':
            r = u""
            r += struct.pack(u"<i", self.nVersion)
            r += ser_vector(self.vin)
            r += ser_vector(self.vout)
            r += struct.pack(u"<I", self.nLockTime)
        elif settings.COINDAEMON_Reward == u'POS':
            r = u""
            r += struct.pack(u"<i", self.nVersion)
            r += struct.pack(u"<i", self.nTime)
            r += ser_vector(self.vin)
            r += ser_vector(self.vout)
            r += struct.pack(u"<I", self.nLockTime)
        if settings.COINDAEMON_TX == u'yes':
            r += ser_string(self.strTxComment)
        return r

    def calc_sha256(self):
        if self.sha256 is None:
            self.sha256 = uint256_from_str(SHA256.new(
                SHA256.new(self.serialize()).digest()).digest())
        return self.sha256

    def is_valid(self):
        self.calc_sha256()
        for tout in self.vout:
            if tout.nValue < 0 or tout.nValue > 21000000 * 100000000:
                return False
        return True

    def __repr__(self):
        return u"CTransaction(nVersion=%i vin=%s vout=%s nLockTime=%i)" % (
            self.nVersion, repr(self.vin), repr(self.vout), self.nLockTime)


class CBlock(object):

    def __init__(self):
        self.nVersion = 1
        self.hashPrevBlock = 0
        self.hashMerkleRoot = 0
        self.nTime = 0
        self.nBits = 0
        self.nNonce = 0
        self.vtx = []
        self.sha256 = None
        if settings.COINDAEMON_ALGO == u'scrypt':
            self.scrypt = None
        elif settings.COINDAEMON_ALGO == u'quark':
            self.quark = None
        elif settings.COINDAEMON_ALGO == u'riecoin':
            self.riecoin = None
        else:
            pass
        if settings.COINDAEMON_Reward == u'POS':
            self.signature = u""
        else:
            pass

    def deserialize(self, f):
        self.nVersion = struct.unpack(u"<i", f.read(4))[0]
        self.hashPrevBlock = deser_uint256(f)
        self.hashMerkleRoot = deser_uint256(f)
        if settings.COINDAEMON_ALGO == u'riecoin':
            self.nBits = struct.unpack(u"<I", f.read(4))[0]
            self.nTime = struct.unpack(u"<II", f.read(8))[0]
            self.nNonce = struct.unpack(u"<IIIIIIII", f.read(32))[0]
        else:
            self.nTime = struct.unpack(u"<I", f.read(4))[0]
            self.nBits = struct.unpack(u"<I", f.read(4))[0]
            self.nNonce = struct.unpack(u"<I", f.read(4))[0]
        self.vtx = deser_vector(f, CTransaction)
        if settings.COINDAEMON_Reward == u'POS':
            self.signature = deser_string(f)
        else:
            pass

    def serialize(self):
        r = []
        r.append(struct.pack(u"<i", self.nVersion))
        r.append(ser_uint256(self.hashPrevBlock))
        r.append(ser_uint256(self.hashMerkleRoot))
        if settings.COINDAEMON_ALGO == u'riecoin':
            r.append(struct.pack(u"<I", self.nBits))
            r.append(struct.pack(u"<Q", self.nTime))
            r.append(ser_uint256(self.nNonce))
        else:
            r.append(struct.pack(u"<I", self.nTime))
            r.append(struct.pack(u"<I", self.nBits))
            r.append(struct.pack(u"<I", self.nNonce))
        r.append(ser_vector(self.vtx))
        if settings.COINDAEMON_Reward == u'POS':
            r.append(ser_string(self.signature))
        else:
            pass
        return u''.join(r)

    if settings.COINDAEMON_ALGO == u'scrypt':
        def calc_scrypt(self):
            if self.scrypt is None:
                r = []
                r.append(struct.pack(u"<i", self.nVersion))
                r.append(ser_uint256(self.hashPrevBlock))
                r.append(ser_uint256(self.hashMerkleRoot))
                r.append(struct.pack(u"<I", self.nTime))
                r.append(struct.pack(u"<I", self.nBits))
                r.append(struct.pack(u"<I", self.nNonce))
                self.scrypt = uint256_from_str(
                    ltc_scrypt.getPoWHash(u''.join(r)))
            return self.scrypt
    elif settings.COINDAEMON_ALGO == u'quark':
        def calc_quark(self):
            if self.quark is None:
                r = []
                r.append(struct.pack(u"<i", self.nVersion))
                r.append(ser_uint256(self.hashPrevBlock))
                r.append(ser_uint256(self.hashMerkleRoot))
                r.append(struct.pack(u"<I", self.nTime))
                r.append(struct.pack(u"<I", self.nBits))
                r.append(struct.pack(u"<I", self.nNonce))
                self.quark = uint256_from_str(
                    quark_hash.getPoWHash(u''.join(r)))
            return self.quark
    elif settings.COINDAEMON_ALGO == u'riecoin':
        def calc_riecoin(self):
            if self.riecoin is None:
                r = []
                r.append(struct.pack(u"<i", self.nVersion))
                r.append(ser_uint256(self.hashPrevBlock))
                r.append(ser_uint256(self.hashMerkleRoot))
                r.append(struct.pack(u"<I", self.nBits))
                r.append(struct.pack(u"<Q", self.nTime))
                sha256 = uint256_from_str(
                    SHA256.new(
                        SHA256.new(
                            u''.join(r)).digest()).digest())
                self.riecoin = riecoinPoW(
                    sha256, uint256_from_compact(
                        self.nBits), self.nNonce)
            return self.riecoin
    else:
        def calc_sha256(self):
            if self.sha256 is None:
                r = []
                r.append(struct.pack(u"<i", self.nVersion))
                r.append(ser_uint256(self.hashPrevBlock))
                r.append(ser_uint256(self.hashMerkleRoot))
                r.append(struct.pack(u"<I", self.nTime))
                r.append(struct.pack(u"<I", self.nBits))
                r.append(struct.pack(u"<I", self.nNonce))
                self.sha256 = uint256_from_str(SHA256.new(
                    SHA256.new(u''.join(r)).digest()).digest())
            return self.sha256

    def is_valid(self):
        if settings.COINDAEMON_ALGO == u'riecoin':
            self.calc_riecoin()
        elif settings.COINDAEMON_ALGO == u'scrypt':
            self.calc_scrypt()
        elif settings.COINDAEMON_ALGO == u'quark':
            self.calc_quark()
        else:
            self.calc_sha256()

        if settings.COINDAEMON_ALGO == u'riecoin':
            target = settings.POOL_TARGET
        else:
            target = uint256_from_compact(self.nBits)

        if settings.COINDAEMON_ALGO == u'riecoin':
            if self.riecoin < target:
                return False
        if settings.COINDAEMON_ALGO == u'scrypt':
            if self.scrypt > target:
                return False
        elif settings.COINDAEMON_ALGO == u'quark':
            if self.quark > target:
                return False
        else:
            if self.sha256 > target:
                return False

        hashes = []
        for tx in self.vtx:
            tx.sha256 = None
            if not tx.is_valid():
                return False
            tx.calc_sha256()
            hashes.append(ser_uint256(tx.sha256))

        while len(hashes) > 1:
            newhashes = []
            for i in xrange(0, len(hashes), 2):
                i2 = min(i + 1, len(hashes) - 1)
                newhashes.append(
                    SHA256.new(
                        SHA256.new(
                            hashes[i] +
                            hashes[i2]).digest()).digest())
            hashes = newhashes

        if uint256_from_str(hashes[0]) != self.hashMerkleRoot:
            return False
        return True

    def __repr__(self):
        return u"CBlock(nVersion=%i hashPrevBlock=%064x hashMerkleRoot=%064x nTime=%s nBits=%08x nNonce=%08x vtx=%s)" % (
            self.nVersion, self.hashPrevBlock, self.hashMerkleRoot, time.ctime(self.nTime), self.nBits, self.nNonce, repr(self.vtx))


class msg_version(object):
    command = u"version"

    def __init__(self):
        self.nVersion = MY_VERSION
        self.nServices = 0
        self.nTime = time.time()
        self.addrTo = CAddress()
        self.addrFrom = CAddress()
        self.nNonce = random.getrandbits(64)
        self.strSubVer = MY_SUBVERSION
        self.nStartingHeight = 0

    def deserialize(self, f):
        self.nVersion = struct.unpack(u"<i", f.read(4))[0]
        if self.nVersion == 10300:
            self.nVersion = 300
        self.nServices = struct.unpack(u"<Q", f.read(8))[0]
        self.nTime = struct.unpack(u"<q", f.read(8))[0]
        self.addrTo = CAddress()
        self.addrTo.deserialize(f)
        self.addrFrom = CAddress()
        self.addrFrom.deserialize(f)
        self.nNonce = struct.unpack(u"<Q", f.read(8))[0]
        self.strSubVer = deser_string(f)
        self.nStartingHeight = struct.unpack(u"<i", f.read(4))[0]

    def serialize(self):
        r = []
        r.append(struct.pack(u"<i", self.nVersion))
        r.append(struct.pack(u"<Q", self.nServices))
        r.append(struct.pack(u"<q", self.nTime))
        r.append(self.addrTo.serialize())
        r.append(self.addrFrom.serialize())
        r.append(struct.pack(u"<Q", self.nNonce))
        r.append(ser_string(self.strSubVer))
        r.append(struct.pack(u"<i", self.nStartingHeight))
        return u''.join(r)

    def __repr__(self):
        return u"msg_version(nVersion=%i nServices=%i nTime=%s addrTo=%s addrFrom=%s nNonce=0x%016X strSubVer=%s nStartingHeight=%i)" % (
            self.nVersion, self.nServices, time.ctime(self.nTime), repr(self.addrTo), repr(self.addrFrom), self.nNonce, self.strSubVer, self.nStartingHeight)


class msg_verack(object):
    command = u"verack"

    def __init__(self):
        pass

    def deserialize(self, f):
        pass

    def serialize(self):
        return u""

    def __repr__(self):
        return u"msg_verack()"


class msg_addr(object):
    command = u"addr"

    def __init__(self):
        self.addrs = []

    def deserialize(self, f):
        self.addrs = deser_vector(f, CAddress)

    def serialize(self):
        return ser_vector(self.addrs)

    def __repr__(self):
        return u"msg_addr(addrs=%s)" % (repr(self.addrs))


class msg_inv(object):
    command = u"inv"

    def __init__(self):
        self.inv = []

    def deserialize(self, f):
        self.inv = deser_vector(f, CInv)

    def serialize(self):
        return ser_vector(self.inv)

    def __repr__(self):
        return u"msg_inv(inv=%s)" % (repr(self.inv))


class msg_getdata(object):
    command = u"getdata"

    def __init__(self):
        self.inv = []

    def deserialize(self, f):
        self.inv = deser_vector(f, CInv)

    def serialize(self):
        return ser_vector(self.inv)

    def __repr__(self):
        return u"msg_getdata(inv=%s)" % (repr(self.inv))


class msg_getblocks(object):
    command = u"getblocks"

    def __init__(self):
        self.locator = CBlockLocator()
        self.hashstop = 0

    def deserialize(self, f):
        self.locator = CBlockLocator()
        self.locator.deserialize(f)
        self.hashstop = deser_uint256(f)

    def serialize(self):
        r = []
        r.append(self.locator.serialize())
        r.append(ser_uint256(self.hashstop))
        return u''.join(r)

    def __repr__(self):
        return u"msg_getblocks(locator=%s hashstop=%064x)" % (
            repr(self.locator), self.hashstop)


class msg_tx(object):
    command = u"tx"

    def __init__(self):
        self.tx = CTransaction()

    def deserialize(self, f):
        self.tx.deserialize(f)

    def serialize(self):
        return self.tx.serialize()

    def __repr__(self):
        return u"msg_tx(tx=%s)" % (repr(self.tx))


class msg_block(object):
    command = u"block"

    def __init__(self):
        self.block = CBlock()

    def deserialize(self, f):
        self.block.deserialize(f)

    def serialize(self):
        return self.block.serialize()

    def __repr__(self):
        return u"msg_block(block=%s)" % (repr(self.block))


class msg_getaddr(object):
    command = u"getaddr"

    def __init__(self):
        pass

    def deserialize(self, f):
        pass

    def serialize(self):
        return u""

    def __repr__(self):
        return u"msg_getaddr()"


class msg_ping(object):
    command = u"ping"

    def __init__(self):
        pass

    def deserialize(self, f):
        pass

    def serialize(self):
        return u""

    def __repr__(self):
        return u"msg_ping()"


class msg_alert(object):
    command = u"alert"

    def __init__(self):
        pass

    def deserialize(self, f):
        pass

    def serialize(self):
        return u""

    def __repr__(self):
        return u"msg_alert()"


class BitcoinP2PProtocol(Protocol):
    messagemap = {
        u"version": msg_version,
        u"verack": msg_verack,
        u"addr": msg_addr,
        u"inv": msg_inv,
        u"getdata": msg_getdata,
        u"getblocks": msg_getblocks,
        u"tx": msg_tx,
        u"block": msg_block,
        u"getaddr": msg_getaddr,
        u"ping": msg_ping,
        u"alert": msg_alert,
    }

    def connectionMade(self):
        peer = self.transport.getPeer()
        self.dstaddr = peer.host
        self.dstport = peer.port
        self.recvbuf = u""
        self.last_sent = 0

        t = msg_version()
        t.nStartingHeight = getattr(self, u'nStartingHeight', 0)
        t.addrTo.ip = self.dstaddr
        t.addrTo.port = self.dstport
        t.addrTo.nTime = time.time()
        t.addrFrom.ip = u"0.0.0.0"
        t.addrFrom.port = 0
        t.addrFrom.nTime = time.time()
        self.send_message(t)

    def dataReceived(self, data):
        self.recvbuf += data
        self.got_data()

    def got_data(self):
        while True:
            if len(self.recvbuf) < 4:
                return
            if self.recvbuf[:4] != u"\xf9\xbe\xb4\xd9":
                raise ValueError(u"got garbage %s" % repr(self.recvbuf))

            if len(self.recvbuf) < 4 + 12 + 4 + 4:
                return
            command = self.recvbuf[4:4 + 12].split(u"\x00", 1)[0]
            msglen = struct.unpack(u"<i", self.recvbuf[4 + 12:4 + 12 + 4])[0]
            checksum = self.recvbuf[4 + 12 + 4:4 + 12 + 4 + 4]
            if len(self.recvbuf) < 4 + 12 + 4 + 4 + msglen:
                return
            msg = self.recvbuf[4 + 12 + 4 + 4:4 + 12 + 4 + 4 + msglen]
            th = SHA256.new(msg).digest()
            h = SHA256.new(th).digest()
            if checksum != h[:4]:
                raise ValueError(u"got bad checksum %s" % repr(self.recvbuf))
            self.recvbuf = self.recvbuf[4 + 12 + 4 + 4 + msglen:]

            if command in self.messagemap:
                f = io.StringIO(msg)
                t = self.messagemap[command]()
                t.deserialize(f)
                self.got_message(t)
            else:
                print u"UNKNOWN COMMAND", command, repr(msg)

    def prepare_message(self, message):
        command = message.command
        data = message.serialize()
        tmsg = u"\xf9\xbe\xb4\xd9"
        tmsg += command
        tmsg += u"\x00" * (12 - len(command))
        tmsg += struct.pack(u"<I", len(data))
        th = SHA256.new(data).digest()
        h = SHA256.new(th).digest()
        tmsg += h[:4]
        tmsg += data
        return tmsg

    def send_serialized_message(self, tmsg):
        if not self.connected:
            return

        self.transport.write(tmsg)
        self.last_sent = time.time()

    def send_message(self, message):
        if not self.connected:
            return

        # print message.command

        # print "send %s" % repr(message)
        command = message.command
        data = message.serialize()
        tmsg = u"\xf9\xbe\xb4\xd9"
        tmsg += command
        tmsg += u"\x00" * (12 - len(command))
        tmsg += struct.pack(u"<I", len(data))
        th = SHA256.new(data).digest()
        h = SHA256.new(th).digest()
        tmsg += h[:4]
        tmsg += data

        # print tmsg, len(tmsg)
        self.transport.write(tmsg)
        self.last_sent = time.time()

    def got_message(self, message):
        if self.last_sent + 30 * 60 < time.time():
            self.send_message(msg_ping())

        mname = u'do_' + message.command
        # print mname
        if not hasattr(self, mname):
            return

        method = getattr(self, mname)
        method(message)

#        if message.command == "tx":
#            message.tx.calc_sha256()
#            sha256 = message.tx.sha256
#            pubkey = binascii.hexlify(message.tx.vout[0].scriptPubKey)
#            txlock.acquire()
#            tx.append([str(sha256), str(time.time()), str(self.dstaddr), pubkey])
#            txlock.release()

    def do_version(self, message):
        # print message
        self.send_message(msg_verack())

    def do_inv(self, message):
        want = msg_getdata()
        for i in message.inv:
            if i.type == 1:
                want.inv.append(i)
            if i.type == 2:
                want.inv.append(i)
        if len(want.inv):
            self.send_message(want)
