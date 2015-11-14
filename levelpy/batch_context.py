#
# levelpy/batch_context.py
#

from .db_accessors import (
    LevelAccessor,
    LevelReader,
    LevelWriter,
)


class BatchContext:
    """
    A python wrapper around LevelDB's WriteBatch functionality.

    Using python's with statement, you can guarantee that the put and delete
    operations applied to the BatchContext will be executed together, or if
    an error occurs, not at all.
    """

    def __init__(self, db, sync=False):
        self._db = db
        self.batch = db.WriteBatch()
        self.write_sync = sync

    def __enter__(self):
        """
        Copies the database, overwritting the Put and Delete methods,
        to be that of the batch object.
        """
        db = self.BatchDB(self._db, self.batch)
        return db

    def __exit__(self, exc_type, exc_value, exc_tb):
        """
        If there was no exception, write to the database.
        """
        if exc_type is None:
            self._db.Write(self.batch, self.write_sync)

    class BatchDB(LevelWriter, LevelReader):
        """
        Intermediate database class overloading LevelWriter's Put and Delete
        methods with a LevelDB batch context
        """

        def __init__(self, db, ctx):
            LevelAccessor.__init__(self,
                                   db.prefix,
                                   db.delim,
                                   (db.encode, db.decode))
            self._db = db
            self._context = ctx
            # self.Put = self._context.Put
            # self.Delete = self._context.Delete

        def Put(self, key, value):
            return self._context.Put(key, value)

        def Delete(self, key):
            return self._context.Delete(key)

        def write_batch(self):
            raise RuntimeError("Cannot create a batch context while in a"
                               "batch context.")
