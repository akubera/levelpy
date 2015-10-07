#
# levelpy/leveldb.py
#

from .batch_context import BatchContext
from .leveldb_module_shims import NormalizeBackend


class LevelAccessor:
    """
    A simple class with a method for transforming strings or bytes into keys,
    and a _key_prefix member which will be prepended to the outgoing key bytes.
    """

    _key_prefix = b''

    def set_key_prefix(self, prefix):
        """
        Sets the prefix of the accessor to given value (transformed to bytes)
        """
        self._key_prefix = bytes(prefix, 'utf8')      \
                           if isinstance(prefix, str) \
                           else bytes(prefix)

    def key_transform(self, key):  # -> bytes
        """
        Takes some potential key and returns the bytes that that this accessor
        can use to retrieve values from the database.
        """
        if isinstance(key, str):
            key = key.encode()
        else:
            key = bytes(key)
        return self._key_prefix + key


class LevelReader(LevelAccessor):
    """
    Class containing methods for reading from an established database.
    """

    _range_ending = b'~'

    def value_decode(self, byte_str: bytes):
        return self.decode(byte_str)

    def get(self, key):
        """
        Normalizes the key, gets bytes from databse, decodes bytes using the
        value_decode method.
        """
        key = self.key_transform(key)
        value_bytes = self.Get(key)
        return self.value_decode(value_bytes)

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

        else:
            return self.get(key)

    def items(self, *args, **kwargs):

        def transform(obj):
            if isinstance(obj, tuple):
                return (bytes(obj[0]), self.value_decode(obj[1]))
            else:
                return self.value_decode(obj)

        if 'include_value' in kwargs and kwargs['include_value'] is False:
            yield from map(bytes, self.RangeIter(*args, **kwargs))
        else:
            yield from map(transform, self.RangeIter(*args, **kwargs))

    def keys(self, *args, **kwargs):
        kwargs['include_value'] = False
        yield from self.items(*args, **kwargs)

    def values(self, *args, **kwargs):
        kwargs['include_value'] = True
        for k, v in self.items(*args, **kwargs):
            yield v

    def __contains__(self, key):
        key = self.key_transform(key)
        stop = key + self._range_ending
        return key in self.keys(key_from=key, key_to=stop)


class LevelWriter(LevelAccessor):

    def value_encode(self, obj):
        return self.encode(obj)

    def put(self, key, value):
        """
        Normalizes the key, encodes the value, and stores in the database.
        """
        key = self.key_transform(key)
        value = self.value_encode(value)
        self.Put(key, value)

    def __setitem__(self, key, value):
        self.put(key, value)

    def __delitem__(self, key):
        key = self.key_transform(key)
        self.Delete(key)

    def write_batch(self):
        return BatchContext(self)


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

    def decode(self, b):
        return b.decode(self.str_encoding)

    def encode(self, obj):
        if isinstance(obj, str):
            return obj.encode(self.str_encoding)
        return bytes(obj)

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

# This must be imported after the class definition as the Sublevel requires the
# class to be defined
from .sublevel import Sublevel                                           # noqa
