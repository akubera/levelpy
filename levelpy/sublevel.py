#
# levelpy/sublevel.py
#

from .db_accessors import (
    LevelAccessor,
    LevelReader,
    LevelWriter,
)
from .view import View


class Sublevel(LevelReader, LevelWriter):
    """
    A Sublevel can be thought of as a table or collection in a leveldb
    databaase. They are a group of keys which all start with the same prefix,
    allowing for an organized collection of similar entries.

    The class is implemented as a subclass of the database, exposing the same
    interface for getting, setting, and iterating values.

    """

    def __init__(self, db, prefix, delim='!', value_encoding='utf8'):
        LevelAccessor.__init__(self, prefix, delim, value_encoding)
        self._db = db

    def __copy__(self):
        """
        Simple copy of sublevel - same db, prefix, and delimeter
        """
        return type(self)(self._db, self.prefix, self.delim)

    def items(self, key_from='', key_to='~', *args, **kwargs):
        """iterate over all items in the sublevel"""
        key_from = self.subkey(key_from)
        key_to = self.subkey(key_to)
        yield from self._db.items(key_from=key_from,
                                  key_to=key_to,
                                  *args,
                                  **kwargs)

    def sublevel(self, key, delim=None, value_encoding='utf-8'):
        """
        Return a sublevel of the sublevel
        """
        delim = self.delim if (delim is None) else delim
        return Sublevel(self._db,
                        self.key_transform(key),
                        delim=delim,
                        value_encoding=value_encoding)

    def view(self, key, delim=None, value_encoding=None):
        """
        Return a read-only view of the sublevel
        """
        delim = self.delim if (delim is None) else delim
        enc = self.value_encoding if (value_encoding is None) else value_encoding
        return View(self._db,
                    self.key_transform(key),
                    delim=delim,
                    value_encoding=enc)
