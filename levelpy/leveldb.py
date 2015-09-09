#
# levelpy/leveldb.py
#

from .batch_context import BatchContext


class LevelDB:
    """
    LevelDB interface
    """

    _db = None
    leveldb = None
    path = None

    def __init__(self,
                 db,
                 database_package='leveldb',
                 **kwargs):
        if isinstance(database_package, str):
            self.leveldb = __import__(database_package)
        else:
            self.leveldb = database_package

        if isinstance(db, str):
            self.path = db
            db = self._db = self.leveldb.LevelDB(self.path, **kwargs)
        else:
            self._db = db

        # Alias methods
        self.Put = self._db.Put
        self.Get = self._db.Get
        self.Delete = self._db.Delete
        self.Write = self._db.Write
        self.RangeIter = self._db.RangeIter
        self.GetStats = self._db.GetStats
        self.CreateSnapshot = self._db.CreateSnapshot

    def __getitem__(self, key):
        if isinstance(key, slice):
            if key.step is not None:
                raise ValueError("Step is not available for levelpy slices")
            return self.RangeIter(key_from=key.start, key_to=key.stop)
        else:
            return self.Get(key)

    def __setitem__(self, key, value):
        self.Put(key, value)

    def __delitem__(self, key):
        self.Delete(key)

    def __contains__(self, key):
        raise NotImplementedError()

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
        yield from self.RangeIter(*args, **kwargs)

    def keys(self, *args, **kwargs):
        kwargs['include_value'] = False
        yield from self.items(*args, **kwargs)

    def values(self, *args, **kwargs):
        for k, v in self.items(*args, **kwargs):
            yield v

    def has_key(self, key):
        return key in iter(self.keys())

    def destroy_db(self):
        self.leveldb.DestroyDB(self.path)

    def stats(self):
        return self.GetStats()

    def create_snapshot(self):
        return self.CreateSnapshot()
