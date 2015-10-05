#
# levelpy/batch_context.py
#

from copy import copy


class BatchContext:

    def __init__(self, db, sync=False):
        self.db = db
        self.batch = db.WriteBatch()
        self.write_sync = sync

    def __enter__(self):
        """
        Copies the database, overwritting the Put and Delete methods,
        to be that of the batch object.
        """
        db = copy(self.db)
        db.Put = self.batch.Put
        db.Delete = self.batch.Delete
        return db

    def __exit__(self, exc_type, exc_value, exc_tb):
        """
        If there was no exception, write to the database.
        """
        if exc_type is None:
            self.db.Write(self.batch, self.write_sync)
