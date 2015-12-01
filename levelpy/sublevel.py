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
        Simple copy of sublevel - same db, prefix, delimeter, and encoding
        """
        enc = self._get_encoding(None)
        return Sublevel(self._db, self.prefix, self.delim, enc)

    def sublevel(self, key, delim=None, value_encoding=None):
        """
        Return a sublevel of the sublevel
        """
        prefix = self.key_transform(key)
        delim = self.delim if (delim is None) else delim
        enc = self._get_encoding(value_encoding)
        return Sublevel(self._db,
                        prefix,
                        delim=delim,
                        value_encoding=enc)

    def view(self, key, delim=None, value_encoding=None):
        """
        Return a read-only view of the sublevel
        """
        prefix = self.key_transform(key)
        delim = self.delim if (delim is None) else delim
        enc = self._get_encoding(value_encoding)
        return View(self._db,
                    prefix,
                    delim=delim,
                    value_encoding=enc)
