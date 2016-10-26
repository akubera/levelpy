#
# levelpy/leveldb.py
#

from .leveldb_module_shims import NormalizeBackend
from .db_accessors import (LevelAccessor, LevelReader, LevelWriter)
from .sublevel import Sublevel
from .view import View


class LevelDB(LevelReader, LevelWriter):
    """
    LevelDB interface.

    This is the wrapper class around a 'real' implementation of
    LevelDB.
    """

    _db = None
    _leveldb_cls = None
    _leveldb_pkg = None
    path = None

    def __init__(self,
                 db,
                 leveldb_cls='leveldb.LevelDB',
                 value_encoding='utf-8',
                 **db_kwargs):
        """
        'Open' a leveldb directory.

        If db parameter is a string, this constructor will interpret
        it as a path to a leveldb directory.

        The backend class managing the database can be set with the
        leveldb_cls parameter, which is either a string (so may be
        switched via a configuration option) or a class object.

        All keyword arguments are forwarded to the constructor of the
        backend database class.

        To ensure a common interface to the backend library, the
        database object is sent to the `NormalizeBackend` function,
        which will add any missing methods to the object (if it
        recognizes the class).

        Parameters:
            db (str or LevelDB instance): Path to database or a
                pre-created database object.
            leveldb_cls (str): Full name of the class which will be the
                database backend. This uses python's __import__ function,
                so there is no need to manually import the package
                yourself.
            value_encoding (str): The default serialization for the
                database. See the Serializer class for more information
                on value_encoding.
            create_if_missing (bool): Argument passed to the database
                class which creates a new database in the filesystem if
                none exists.

            **db_kwargs: keyword arguments passed directly to the database
                class specified.
        """
        # if db is a string - create the db object from the leveldb_cls param
        if isinstance(db, str):
            self.path = db

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
            self._db = self._leveldb_cls(self.path, **db_kwargs)

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

        LevelAccessor.__init__(self, '', '', value_encoding)

        NormalizeBackend(self, self._db)

    def __copy__(self):
        """
        Shallow copy of database - reusing the backend 'connection'
        object.
        """
        return type(self)(self._db)

    def batch(self):
        """
        Alias of the write_batch() method - creates a BatchDB object.
        """
        return self.write_batch()

    def destroy_db(self):
        raise NotImplementedError

    def stats(self):
        """
        Returns the database stats.
        """
        return self.GetStats()

    def create_snapshot(self):
        """
        Returns the database stats.
        """
        return self.CreateSnapshot(self._db)

    def sublevel(self, key, delim=b'!', value_encoding=None):
        """
        Generate a sublevel with prefix key.
        """
        enc = self._get_encoding(value_encoding)
        return Sublevel(self._db,
                        self.key_transform(key),
                        delim=delim,
                        value_encoding=enc,
                        )

    def view(self, key, delim=b'!', value_encoding=None):
        """
        Generate a read-only view of a prefixed part of the database.
        """
        prefix = self.key_transform(key)
        enc = self._get_encoding(value_encoding)

        db = self._db if isinstance(self._db, (LevelDB, LevelReader)) else self

        return View(db,
                    prefix,
                    delim=delim,
                    value_encoding=enc,
                    )
