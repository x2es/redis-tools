
import unittest
import redis_keys_index

def let_redis_keys_index(opts={}):
    default_opts = { 'delimiter': ':' }
    default_opts.update(opts)
    return redis_keys_index.RedisKeysIndex(default_opts)

class TreatKey(unittest.TestCase):
    def testFirstOccurance(self):
        rki = let_redis_keys_index()
        rki.treat('foo:bar:baz')
        self.assertEqual(rki.stat, {})
        self.assertEqual(rki.firstSeen, ['foo:', 'foo:bar:'])

    def testSecondOccurance(self):
        rki = let_redis_keys_index()
        count = rki.countSymbol
        rki.treat('foo:bar:baz')
        rki.treat('foo:bar:buz')
        self.assertEqual(rki.stat, { 
            'foo:': {
                count: 2
            },
            'foo:bar:': {
                count: 2
            }
        })
        self.assertEqual(filter(None, rki.firstSeen), [])

    def testCleanup(self):
        rki = let_redis_keys_index({ 'firstSeenThresholdLength': 10, 'firstSeenCutBucketSize': 4 })
        for i in xrange(12): rki.treat('K:{0}:x'.format(i+1))

        # like [NONE, K:1, K:2, K:3, ... K:12]
        self.assertEqual(len(rki.firstSeen), 13)

        # next item in firstSeen should trigger cleanup
        rki.treat('K:13:x')
        expected = []
        for i in range(4, 14): expected.append('K:{0}:'.format(i)) # upto K:13:x
        self.assertEqual(rki.firstSeen, expected)


    # def testLimit(self):
    #     rki = let_redis_keys_index()

    #     # This should limit
    #     #   K:* (2 parts)
    #     #   K:SK:* (3 parts)
    #     #   K:SI:* (3 parts)
    #     # but should pass
    #     #   K:NN:*
    #     rki.limit = set(['K:', 'SK:', 'SI:'])
        
class Misc(unittest.TestCase):
    def testStatCount(self):
        rki = let_redis_keys_index()
        rki.treat('foo:bar')
        rki.treat('foo:baz')
        self.assertEqual(rki.statCount('foo:'), 2)

if __name__ == '__main__':
  unittest.main()
