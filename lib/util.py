u'''Various helper methods. It probably needs some cleanup.'''

from __future__ import absolute_import
import struct
import io
import binascii
from . import settings
from . import bitcoin_rpc
from hashlib import sha256


def deser_string(f):
    nit = struct.unpack(u"<B", f.read(1))[0]
    if nit == 253:
        nit = struct.unpack(u"<H", f.read(2))[0]
    elif nit == 254:
        nit = struct.unpack(u"<I", f.read(4))[0]
    elif nit == 255:
        nit = struct.unpack(u"<Q", f.read(8))[0]
    return f.read(nit)


def ser_string(s):
    if len(s) < 253:
        return unichr(len(s)) + s
    elif len(s) < 0x10000:
        return unichr(253) + struct.pack(u"<H", len(s)) + s
    elif len(s) < 0x100000000:
        return unichr(254) + struct.pack(u"<I", len(s)) + s
    return unichr(255) + struct.pack(u"<Q", len(s)) + s


def deser_uint256(f):
    r = 0
    for i in xrange(8):
        t = struct.unpack(u"<I", f.read(4))[0]
        r += t << (i * 32)
    return r


def ser_uint256(u):
    rs = u""
    for i in xrange(8):
        rs += struct.pack(u"<I", u & 0xFFFFFFFF)
        u >>= 32
    return rs


def uint256_from_str(s):
    r = 0
    t = struct.unpack(u"<IIIIIIII", s[:32])
    for i in xrange(8):
        r += t[i] << (i * 32)
    return r


def uint256_from_str_be(s):
    r = 0
    t = struct.unpack(u">IIIIIIII", s[:32])
    for i in xrange(8):
        r += t[i] << (i * 32)
    return r


def uint256_from_compact(c):
    nbytes = (c >> 24) & 0xFF
    if nbytes <= 3:
        v = (c & 0xFFFFFF) >> (8 * (3 - nbytes))
    else:
        v = (c & 0xFFFFFF) << (8 * (nbytes - 3))
    return v


def deser_vector(f, c):
    nit = struct.unpack(u"<B", f.read(1))[0]
    if nit == 253:
        nit = struct.unpack(u"<H", f.read(2))[0]
    elif nit == 254:
        nit = struct.unpack(u"<I", f.read(4))[0]
    elif nit == 255:
        nit = struct.unpack(u"<Q", f.read(8))[0]
    r = []
    for i in xrange(nit):
        t = c()
        t.deserialize(f)
        r.append(t)
    return r


def ser_vector(l):
    r = u""
    if len(l) < 253:
        r = unichr(len(l))
    elif len(l) < 0x10000:
        r = unichr(253) + struct.pack(u"<H", len(l))
    elif len(l) < 0x100000000:
        r = unichr(254) + struct.pack(u"<I", len(l))
    else:
        r = unichr(255) + struct.pack(u"<Q", len(l))
    for i in l:
        r += i.serialize()
    return r


def deser_uint256_vector(f):
    nit = struct.unpack(u"<B", f.read(1))[0]
    if nit == 253:
        nit = struct.unpack(u"<H", f.read(2))[0]
    elif nit == 254:
        nit = struct.unpack(u"<I", f.read(4))[0]
    elif nit == 255:
        nit = struct.unpack(u"<Q", f.read(8))[0]
    r = []
    for i in xrange(nit):
        t = deser_uint256(f)
        r.append(t)
    return r


def ser_uint256_vector(l):
    r = u""
    if len(l) < 253:
        r = unichr(len(l))
    elif len(l) < 0x10000:
        r = unichr(253) + struct.pack(u"<H", len(l))
    elif len(l) < 0x100000000:
        r = unichr(254) + struct.pack(u"<I", len(l))
    else:
        r = unichr(255) + struct.pack(u"<Q", len(l))
    for i in l:
        r += ser_uint256(i)
    return r

__b58chars = u'123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
__b58base = len(__b58chars)


def b58decode(v, length):
    u""" decode v into a string of len bytes
    """
    long_value = 0
    for (i, c) in enumerate(v[::-1]):
        long_value += __b58chars.find(c) * (__b58base**i)

    result = u''
    while long_value >= 256:
        div, mod = divmod(long_value, 256)
        result = unichr(mod) + result
        long_value = div
    result = unichr(long_value) + result

    nPad = 0
    for c in v:
        if c == __b58chars[0]:
            nPad += 1
        else:
            break

    result = unichr(0) * nPad + result
    if length is not None and len(result) != length:
        return None

    return result


def b58encode(value):
    u""" encode integer 'value' as a base58 string; returns string
    """
    encoded = u''
    while value >= __b58base:
        div, mod = divmod(value, __b58base)
        encoded = __b58chars[mod] + encoded  # add to left
        value = div
    encoded = __b58chars[value] + encoded  # most significant remainder
    return encoded


def reverse_hash(h):
    # This only revert byte order, nothing more
    if len(h) != 64:
        raise Exception(u'hash must have 64 hexa chars')

    return u''.join([h[56 - i:64 - i] for i in xrange(0, 64, 8)])


def doublesha(b):
    return sha256(sha256(b).digest()).digest()


def bits_to_target(bits):
    return struct.unpack(u'<L', bits[:3] + u'\0')[0] * \
        2**(8 * (int(bits[3], 16) - 3))


def address_to_pubkeyhash(addr):
    try:
        addr = b58decode(addr, 25)
    except:
        return None

    if addr is None:
        return None

    ver = addr[0]
    cksumA = addr[-4:]
    cksumB = doublesha(addr[:-4])[:4]

    if cksumA != cksumB:
        return None

    return (ver, addr[1:-4])


def ser_uint256_be(u):
    u'''ser_uint256 to big endian'''
    rs = u""
    for i in xrange(8):
        rs += struct.pack(u">I", u & 0xFFFFFFFF)
        u >>= 32
    return rs


def deser_uint256_be(f):
    r = 0
    for i in xrange(8):
        t = struct.unpack(u">I", f.read(4))[0]
        r += t << (i * 32)
    return r


def ser_number(n):
    # For encoding nHeight into coinbase
    s = bytearray(u'\1')
    while n > 127:
        s[0] += 1
        s.append(n % 256)
        n //= 256
    s.append(n)
    return unicode(s)


def isPrime(n):
    if pow(2, n - 1, n) == 1:
        return True
    return False


def riecoinPoW(hash_int, diff, nNonce):
    base = 1 << 8
    for i in xrange(256):
        base = base << 1
        base = base | (hash_int & 1)
        hash_int = hash_int >> 1
    trailingZeros = diff - 1 - 8 - 256
    if trailingZeros < 16 or trailingZeros > 20000:
        return 0
    base = base << trailingZeros

    base += nNonce

    if (base % 210) != 97:
        return 0

    if not isPrime(base):
        return 0
    primes = 1

    base += 4
    if isPrime(base):
        primes += 1

    base += 2
    if isPrime(base):
        primes += 1

    base += 4
    if isPrime(base):
        primes += 1

    base += 2
    if isPrime(base):
        primes += 1

    base += 4
    if isPrime(base):
        primes += 1

    return primes

# if settings.COINDAEMON_Reward == 'POW':


def script_to_address(addr):
    d = address_to_pubkeyhash(addr)
    if not d:
        raise ValueError(u'invalid address')
    (ver, pubkeyhash) = d
    return u'\x76\xa9\x14' + pubkeyhash + u'\x88\xac'
# else:


def script_to_pubkey(key):
    if len(key) == 66:
        key = binascii.unhexlify(key)
    if len(key) != 33:
        raise Exception(u'Invalid Address')
    return u'\x21' + key + u'\xac'
