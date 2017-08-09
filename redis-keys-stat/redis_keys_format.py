
class RedisKeysFormat:
    "Split and test redis keys"

    delimiter = None

    def __init__(self, opts):
        "@param {Dictionary(delimiter)} opts"
        
        self.delimiter = opts['delimiter']

    def superkeys(self, key):
        """ 
        @param {String} key
        @return {List}
        """
        
        parts = filter(None, key.split(self.delimiter))

        superkeys = []
        path = ''
        
        # we not care last part
        # TODO: xrange
        for i in range(0, len(parts) - 1):
            path += parts[i] + self.delimiter
            superkeys.append(path)

        return superkeys
