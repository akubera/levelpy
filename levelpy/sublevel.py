#
# levelpy/sublevel.py
#

from .leveldb import LevelDB


class Sublevel(LevelDB):
    """

    A Sublevel can be thought of as a table or collection in a leveldb
    databaase. They are a group of keys which all start with the same prefix,
    allowing for an organized collection of similar entries.

    The class is implemented as a subclass of the database, exposing the same
    interface for getting, setting, and iterating values.

    """

    def __init__(self, db, prefix, delim='!'):
        self.db = db
        self.prefix = prefix
        self.delim = delim

    def __getitem__(self, key):
        if isinstance(key, slice):
            if key.step is not None:
                raise ValueError("Step values are not available for "
                                 "slices in levelpy")
            start, stop = self.subkey(key.start), self.subkey(key.stop)
            return self.db[start:stop]
        elif isinstance(key, (tuple, list, set)):
            t = type(key)
            return self.db[t(self.subkey(k) for k in key)]
        else:
            return self.db[self.subkey(key)]

    def __setitem__(self, key, value):
        self.db[self.subkey(key)] = value

    def __delitem__(self, key):
        del self.db[self.subkey(key)]

    def __copy__(self):
        """
        Simple copy of sublevel - same db, prefix, and delimeter
        """
        return type(self)(self.db, self.prefix, self.delim)

    def items(self, key_from='', key_to=None, *args, **kwargs):
        """iterate over all items in the sublevel"""
        print("ITEMS")
        key_from = self.subkey(key_from)
        key_to = self.subkey('~' if (key_to is None) else key_to)
        yield from self.db.items(key_from=key_from,
                                 key_to=key_to,
                                 *args,
                                 **kwargs)


    def subkey(self, key):
        return self.prefix + self.delim + key

    def sublevel(self, key):
        return Sublevel(self.db, self.subkey(key), self.delim)
