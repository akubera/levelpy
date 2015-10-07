#
# levelpy/leveldb.py
#

from .leveldb_module_shims import NormalizeBackend
from .db_accessors import (LevelReader, LevelWriter)
from .sublevel import Sublevel


class LevelDB(LevelReader, LevelWriter):
    """
    LevelDB interface.

    This is the wrapper class around a 'real' implementation of LevelDB.
    """

    _db = None
    _leveldb_cls = None
    _leveldb_pkg = None
    path = None

    str_encoding = 'utf-8'

    def __init__(self,
                 db,
                 leveldb_cls='leveldb.LevelDB',
                 value_encoding='utf-8',
                 **kwargs):

        # if db is a string - create the db object from the leveldb_cls param
        if isinstance(db, str):

            # injected class name by string
            if isinstance(leveldb_cls, str):
                # everything before last dot is package, after is class name
                last_dot = leveldb_cls.rfind('.')
                pkg = leveldb_cls[:last_dot]
                db_classname = leveldb_cls[last_dot+1:]
                self._leveldb_pkg = __import__(pkg)
                self._leveldb_cls = getattr(self._leveldb_pkg, db_classname)

            # passed the class directly
            elif isinstance(leveldb_cls, type):
                self._leveldb_pkg = leveldb_cls.__module__
                self._leveldb_cls = leveldb_cls

            # leveldb_cls *should* provide the backend
            else:
                self._leveldb_pkg = None
                self._leveldb_cls = leveldb_cls

            # create the backend
            self._db = self._leveldb_cls(db, **kwargs)

            # The backend package was not determined.
            # Provided 'class' was just a factory function - inspect
            # resultant object to get real class and package names
            if self._leveldb_pkg is None:
                self._leveldb_cls = self._db.__class__
                self._leveldb_pkg = self._leveldb_cls.__module__

        # db was an object - just copy information
        else:
            self._db = db
            self._leveldb_cls = self._db.__class__
            self._leveldb_pkg = self._leveldb_cls.__module__

        NormalizeBackend(self, self._db)

        def update_attr(name, items):
            if hasattr(self, name):
                return
            for item in items:
                if hasattr(self._db, item):
                    setattr(self, name, getattr(self._db, item))
                    return
            setattr(self, name, None)

        # Alias methods
        update_attr('Put', ('Put', 'put'))
        update_attr('Get', ('Get', 'get'))
        update_attr('Delete', ('Delete', 'delete'))
        update_attr('Write', ('Write', 'write'))
        update_attr('RangeIter', ('RangeIter', 'range_iter'))
        update_attr('GetStats', ('GetStats', 'get_stats'))
        update_attr('CreateSnapshot', ('CreateSnapshot', 'snapshot'))

        # attempt to copy the path of the database
        update_attr('path', ('name', 'filename'))

    def __copy__(self):
        """
        Shallow copy of database - reusing the current instance connection
        """
        return type(self)(self._db)

    def decode(self, b):
        return b.decode(self.str_encoding)

    def encode(self, obj):
        if isinstance(obj, str):
            return obj.encode(self.str_encoding)
        return bytes(obj)

    def batch(self):
        return self.write_batch()

    def destroy_db(self):
        raise NotImplementedError

    def stats(self):
        return self.GetStats()

    def create_snapshot(self):
        return self.CreateSnapshot()

    def sublevel(self, key):
        """
        Generate a sublevel with prefix key.
        """
        return Sublevel(self, key)
