
import unittest
import redis_keys_format

class KeysSplit(unittest.TestCase):
    def testSuperkey(self):
        rkf = redis_keys_format.RedisKeysFormat({ 'delimiter': ':' })

        # simple cases
        self.assertEqual(rkf.superkeys('foo:bar:baz'), ['foo:', 'foo:bar:'])
        self.assertEqual(rkf.superkeys('foo'), [])

        # superkey for superkey
        self.assertEqual(rkf.superkeys('foo:bar:baz:'), ['foo:', 'foo:bar:'])

if __name__ == '__main__':
  unittest.main()
