
LevelPy
=======

A pythonic interface to the LevelDB database.

About
-----

LevelPy is a project that does not directly access a LevelDB instance, but
intends to be a thin wrapper around other implementations, providing a simple
and pythonic interface to the lower level database.

**LevelPy alone does *NOT* provide access to a database, nor does it declare
one as a dependency. It is up to YOU to choose and install such a package.**


Usage
-----

Constructor
~~~~~~~~~~~

To create a 'connection' to a leveldb database, use the ``LevelPy.LevelDB``
class, providing a path and, optionally, the *full* classname (package + class)
of the backend to use. The default class is ``leveldb.LevelDB``, using the
`py-leveldb <https://github.com/rjpower/py-leveldb>`_ interface.

You can also give a class as the second parameter, which is interpreted to be
the *type* of the connection to create. This is called with the path and
expanded keyword arguments. Similarly, any callable can be used as the second
parameter, and the constructor will forward the path and any keyword arguments
to it (the database instance **MUST** be returned by the function).

Alternatively, you can create a separate leveldb connection and pass this to the
levelpy.LevelDB constructor. If the first parameter is not a string, it is
assumed to be an (already connected) backend, and the 'backend class' parameter
is ignored.

examples:

.. code:: python

  from levelpy import LevelDB

  db = LevelDB('/path/to/db')  # use the default leveldb.LevelDB backend
  --------------------
  db = LevelDB('/path/to/db', 'plyvel.DB', error_if_exists=True)  # use the Plyvel backend w/ keyword
  --------------------
  db = LevelDB('/path/to/db', 'my.custom.leveldb.DATABASE')  # use your own backend
  --------------------
  cls = leveldb.LevelDB
  db = LevelDB('/path/to/db', cls)  # use a specific class to create an instance
  --------------------
  db = LevelDB(cnx)  # use an already created leveldb connection (backend string is ignored)

levelpy.LevelDB will import the package and store an instance of this class,
forwarding any keyword arguments to the constructor.

Of course, there is no absolute standard interface so there is no guarantee
that all implementations will work. Currently this class assumes that all the
standard functionality of the original implementation of leveldb is exposed as
capitalized method names: ``db.Get(...)`` ``db.Put(...)`` etc. LevelPy's
LevelDB class aliases these methods to expose a 1-1 interface of the wrapper
and backend (if they exist). If the backend you wish to use has a different
convention, simply set the aliased methods after creating the connection:
``db.Get = db._db.retrieve_value`` (access the backend database is provided by
the ``_db`` attribute).


Access
~~~~~~

As LevelDB is really just a big key-value store, LevelPy implements a
dict-interface to the database (using the [] operators).

.. code:: python

  item = db['itemkey'] # store value with key 'itemkey' as item
  db['something'] = 'else' # store value 'else' with key 'something'

  a_to_b = db['a':'b']  # get all items between 'a' and 'b' (inclusive)

  has_5 = '5' in db  # tests if '5' is a key in the database

As mentioned in Constructor_, such access is also provided by the ``Get`` &
``Put`` members.

Iteration
~~~~~~~~~

Keeping the database dictlike, LevelPy exposes the methods items(), keys(),
values(), which provides generators to iterate over the expected items.

.. code:: python

  keystr = ' '.join(key for key in db.keys())

  for k, v in db.items():
      print(k, '->', v)
