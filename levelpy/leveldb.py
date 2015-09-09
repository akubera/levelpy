#
# levelpy/leveldb.py
#

from .batch_context import BatchContext


class LevelDB:
    """
    LevelDB interface
    """

    def __init__(self,
                 db,
                 database_package='leveldb',
                 **kwargs):
        if isinstance(database_package, str):
            self.leveldb = __import__(database_package)
        else:
            self.leveldb = database_package

        if isinstance(db, str):
            self._db = self.leveldb.LevelDB(db, **kwargs)
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
            return self._db.RangeIter(key_from=key.start, key_to=key.stop)
        else:
            return self._db.Get(key)

    def __setitem__(self, key, value):
        self._db.Put(key, value)

    def __delitem__(self, key):
        self._db.Delete(key)

    def __contains__(self, key):
        raise NotImplementedError()

    def __copy__(self):
        """
        Shallow copy of database - reusing the current instance connection
        """
        return type(self)(self._db, self.leveldb)

    def write(self):
        self._db.Write()

    def write_batch(self):
        return BatchContext(self)

    def batch(self):
        return self.write_batch()

    def items(self, *args, **kwargs):
        yield from self._db.RangeIter(*args, **kwargs)

    def keys(self):
        yield from self.items(include_value=False)

    def values(self):
        yield from self.items(include_keys=False)

    def has_key(self, key):
        return key in self

    def destroy_db(self):
        self.leveldb.DestroyDB(self._db.path)

    def stats(self):
        return self._db.GetStats()

    def create_snapshot(self):
        self.CreateSnapshot
