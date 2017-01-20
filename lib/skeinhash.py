from __future__ import absolute_import
import hashlib
import struct
from . import skein


def skeinhash(msg):
    return hashlib.sha256(skein.Skein512(msg[:80]).digest()).digest()


def skeinhashmid(msg):
    s = skein.Skein512(msg[:64] + u'\x00')  # hack to force Skein512.update()
    return struct.pack(u'<8Q', *s.tf.key.tolist())

if __name__ == u'__main__':
    mesg = u"dissociative1234dissociative4567dissociative1234dissociative4567dissociative1234"
    h = skeinhashmid(mesg)
    print h.encode(u'hex')
    print u'ad0d423b18b47f57724e519c42c9d5623308feac3df37aca964f2aa869f170bdf23e97f644e81511df49c59c5962887d17e277e7e8513345137638334c8e59a4' == h.encode(u'hex')

    h = skeinhash(mesg)
    print h.encode(u'hex')
    print u'764da2e768811e91c6c0c649b052b7109a9bc786bce136a59c8d5a0547cddc54' == h.encode(u'hex')
