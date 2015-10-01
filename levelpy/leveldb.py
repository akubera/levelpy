#
# levelpy/leveldb.py
#

from .batch_context import BatchContext

from .leveldb_module_shims import NormalizeBackend

class LevelDB:
    """
    LevelDB interface.

    This is the wrapper class around a 'real' implementation of LevelDB.
    """

    _db = None
    _leveldb_cls = None
    _leveldb_pkg = None
    path = None

    def __init__(self,
                 db,
                 leveldb_cls='leveldb.LevelDB',
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
                self._leveldb_pkg = leveldb_cls.__modue__
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

        NormalizeBackend(self._db)

        def copy_attr(name, items):
            for item in items:
                if hasattr(self._db, item):
                    setattr(self, name, getattr(self._db, item))
                    return
            setattr(self, name, None)

        # Alias methods
        copy_attr('Put', ('Put', 'put'))
        copy_attr('Get', ('Get', 'get'))
        copy_attr('Delete', ('Delete', 'delete'))
        copy_attr('Write', ('Write', 'write'))
        copy_attr('RangeIter', ('RangeIter', 'range_iter'))
        copy_attr('GetStats', ('GetStats', 'get_stats'))
        copy_attr('CreateSnapshot', ('CreateSnapshot', 'snapshot'))
        copy_attr('WriteBatch', ('WriteBatch', 'write_batch'))

        # attempt to copy the path of the database
        copy_attr('path', ('name', 'filename'))

    def __getitem__(self, key):
        if isinstance(key, slice):
            if key.step is not None:
                raise ValueError("Step is not available for levelpy slices")
            return self.RangeIter(key_from=key.start, key_to=key.stop)
        elif isinstance(key, (tuple, list, set)):
            t = type(key)
            return t(self.Get(k) for k in key)
        else:
            return self.Get(key)

    def __setitem__(self, key, value):
        self.Put(bytes(key), bytes(value))

    def __delitem__(self, key):
        self.Delete(bytes(key))

    def __contains__(self, key):
        bkey = bytes(key)
        for next_key in self.keys(key_from=bkey, key_to=bkey + b'~'):
            if key == next_key:
                return True
        return False

    def __copy__(self):
        """
        Shallow copy of database - reusing the current instance connection
        """
        return type(self)(self._db, self.leveldb)

    def write_batch(self):
        return BatchContext(self)

    def batch(self):
        return self.write_batch()

    def items(self, *args, **kwargs):
        # yield from self.RangeIter(*args, **kwargs)
        print("items(", args, kwargs, ")")
        for item in self.RangeIter(*args, **kwargs):
            bitem = bytes(item)
            print("  ", bitem)
            yield bitem

    def keys(self, *args, **kwargs):
        kwargs['include_value'] = False
        yield from self.items(*args, **kwargs)

    def values(self, *args, **kwargs):
        kwargs['include_value'] = True
        for k, v in self.items(*args, **kwargs):
            yield v

    def destroy_db(self):
        self.leveldb.DestroyDB(self.path)

    def stats(self):
        return self.GetStats()

    def create_snapshot(self):
        return self.CreateSnapshot()

    def sublevel(self, key):
        """
        Generate a sublevel with prefix key.
        """
        return Sublevel(self, key)

# This must be imported after the class definition as the Sublevel requires the
# class to be defined
from .sublevel import Sublevel                                           # noqa
