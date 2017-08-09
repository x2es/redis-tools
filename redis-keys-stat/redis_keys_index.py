
import redis_keys_format

class RedisKeysIndex:
    "builds stat index of passed reids keys"

    countSymbol = 1

    def __init__(self, opts):
        """ 
        @param {Dictionary(
                     delimiter, 
                     firstSeenThresholdLength=10000, 
                     firstSeenCutBucketSize=1000
                     verbose=False
               )} opts
        """

        self.delimiter = opts['delimiter']

        self.__redisKeysFormat = None
        self.__treatedKeysCount = 0
        self.__cleanedFirstSeenCount = 0

        self.stat = {}
        self.firstSeen = []
        # self.limit = set([])

        self.__redisKeysFormat = redis_keys_format.RedisKeysFormat({ 'delimiter': opts['delimiter'] })
        self.__firstSeenThresholdLength = opts.get('firstSeenThresholdLength', 10000)
        self.__firstSeenCutBucketSize = opts.get('firstSeenCutBucketSize', 1000)
        self.__verbose = opts.get('verbose', False)


    def treat(self, key):
        """
        Treat key
        @param {String} key
        """

        superkeys = self.__redisKeysFormat.superkeys(key)
        for superkey in superkeys: self.__treatSuperkey(superkey)

        self.__treatedKeysCount += 1
        # TODO: only verbose
        if self.__treatedKeysCount % 100 == 0: print ' # Treated keys:', self.__treatedKeysCount

        self.__cleanupFirstSeen()


    def statCount(self, key):
        return self.stat[key][self.countSymbol]


    def __treatSuperkey(self, superkey):
        # check/update stat
        if self.stat.get(superkey, None) is not None:
            self.stat[superkey][self.countSymbol] += 1
            return

        # check first seen / move to stat
        for i in reversed(xrange(len(self.firstSeen))):
            if self.firstSeen[i] == superkey:
                # TODO: only verbose
                print ' > Found superkey: "{0}"'.format(superkey)
                # we see this key second time
                self.stat[superkey] = { self.countSymbol: 2 }
                self.__cleanFirstSeenItem(i)
                return

        self.firstSeen.append(superkey)


    def __cleanFirstSeenItem(self, index):
        """
        set specified item as None, for future clenup
        @param {int} index
        """

        self.firstSeen[index] = None
        self.__cleanedFirstSeenCount += 1
    

    def __cleanupFirstSeen(self):
        "clenup according :firstSeenThresholdLength and :firstSeenCutBucketSize and compact"

        if len(self.firstSeen) < self.__firstSeenThresholdLength + self.__firstSeenCutBucketSize: return

        # TODO: only verbose
        print ' # Cleanup top {0} first seen superkeys'.format(self.__firstSeenCutBucketSize)

        for i in xrange(self.__firstSeenCutBucketSize):
            self.__cleanFirstSeenItem(i)
            self.__cleanedFirstSeenCount = 0

        print ' # Compact empty'
        self.firstSeen = filter(None, self.firstSeen)
        print ' # Cleanup done'
