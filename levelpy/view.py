#
# levelpy/view.py
#
"""
Provides a read-only object accessing a database or sublevel
"""

from .db_accessors import LevelReader


class View(LevelReader):
    """
    This is an immutable object providing read-only access to the database
    (or a sublevel)
    """

    def __init__(self, db, prefix='', delim='!', value_encoding='utf-8'):
        self.db = db
        self.prefix = prefix
        self._key_prefix = bytes(prefix, value_encoding) + bytes(delim, value_encoding)
        self.delim = delim
        # self.RangeIter = db.RangeIter

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

    def __contains__(self, key):
        return self.subkey(key) in self.db

    def __copy__(self):
        """
        Simple copy of sublevel - same db, prefix, and delimeter
        """
        return type(self)(self.db, self.prefix, self.delim)

    def items(self, key_from='', key_to='~', *args, **kwargs):
        """iterate over all items in the sublevel"""
        key_from = self.subkey(key_from)
        key_to = self.subkey(key_to)
        yield from self.db.items(key_from=key_from,
                                 key_to=key_to,
                                 *args,
                                 **kwargs)

    def subkey(self, key):
        if key is None:
            return None
        return self.prefix + self.delim + key

    def sublevel(self, key):
        return Sublevel(self.db, self.subkey(key), self.delim)

    def view(self, prefix):
        return View(self.db, self.prefix + self.delim + prefix)
