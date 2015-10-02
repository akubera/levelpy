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

    str_encoding = 'utf-8'

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

            start = bytes(key.start, self.str_encoding) \
                    if isinstance(key.start, str)       \
                    else key.start

            stop = bytes(key.stop, self.str_encoding) \
                   if isinstance(key.stop, str)       \
                   else key.stop

            return self.RangeIter(key_from=start, key_to=stop)

        elif isinstance(key, (tuple, list, set)):
            t = type(key)
            return t(self[k] for k in key)
        elif isinstance(key, str):
            key = bytes(key, self.str_encoding)
            val = self._db.Get(key)
            return str(val, self.str_encoding)
        else:
            return bytes(self._db.Get(key))

    def __setitem__(self, key, value):
        if isinstance(key, str):
            key = bytes(key, self.str_encoding)
        if isinstance(value, str):
            value = bytes(value, self.str_encoding)

        self.Put(key, value)

    def __delitem__(self, key):
        if isinstance(key, str):
            key = bytes(key, self.str_encoding)
        self.Delete(key)

    def __contains__(self, key):
        if isinstance(key, str):
            key = bytes(key, self.str_encoding)
        return key in self.keys(key_from=key, key_to=key + b'~')

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

        enc = kwargs.pop('encoding', None)

        def transform(obj):
            if isinstance(obj, tuple):
                return tuple(map(transform, obj))
            elif enc is None:
                return bytes(obj)
            else:
                return obj.decode(enc)

        yield from map(transform, self.RangeIter(*args, **kwargs))

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
