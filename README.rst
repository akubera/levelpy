LevelPy
=======

master
   |travis-master| |coveralls-master| |version-master|
dev
   |travis-dev| |coveralls-dev|


A pythonic interface to the `LevelDB`_ database.


About
-----

LevelPy is a project that does not directly access a LevelDB instance, but intends to be a thin wrapper around other
implementations, providing a common, simple, pythonic interface to the lower level database.

**LevelPy alone does *NOT* provide access to a database, nor does it declare one as a dependency. It is up to YOU to
choose and install such a package.**

In addition to the pythonic interface, LevelPy database objects adhere to the `LevelDB API`_, which uses uppercase
methods such as 'Get'.
Any objects which provide this interface interacts directly to the underlying database object, so these expect python
bytes objects for keys and values.


Usage
-----

Constructor
~~~~~~~~~~~

To create a 'connection' to a leveldb database, use the ``LevelPy.LevelDB`` class, providing a path and, optionally, the
*full* classname (package + class) of the backend to use.
The default class is ``leveldb.LevelDB``, using the `py-leveldb <https://github.com/rjpower/py-leveldb>`_ interface.

You can also give a class as the second parameter, which is interpreted to be the *type* of the connection to create.
This is called with the path and expanded keyword arguments.
Similarly, any callable can be used as the second parameter, and the constructor will forward the path and any keyword
arguments to it (the database instance **MUST** be returned by the function).

Alternatively, you can create a separate leveldb connection and pass this to the levelpy.LevelDB constructor.
If the first parameter is not a string, it is assumed to be an (already connected) backend, and the 'backend class'
parameter is ignored.

examples:

.. code:: python

  from levelpy import LevelDB

  db = LevelDB('/path/to/db')  # use the default leveldb.LevelDB backend

  ##############################

  db = LevelDB('/path/to/db', 'plyvel.DB', error_if_exists=True)  # use the Plyvel backend w/ keyword

  ##############################

  db = LevelDB('/path/to/db', 'my.custom.leveldb.DATABASE')  # use your own backend

  ##############################

  cls = leveldb.LevelDB
  db = LevelDB('/path/to/db', cls)  # use a specific class to create an instance

  ##############################

  cnx = setup.your.own_db_instance("however", "you", "wish")
  db = LevelDB(cnx)  # use an already created leveldb connection (backend string is ignored)


levelpy.LevelDB will import the package and store an instance of this class, forwarding any keyword arguments to the
constructor.

Of course, there is no absolute standard interface so there is no guarantee that all implementations will work.
Currently this class assumes that all the standard functionality of the original implementation of leveldb is exposed as
capitalized method names: ``db.Get(...)`` ``db.Put(...)`` etc.
LevelPy's LevelDB class aliases these methods to expose a 1-1 interface of the wrapper and backend (if they exist).
If the backend you wish to use has a different convention, simply set the aliased methods after creating the connection:
``db.Get = db._db.retrieve_value`` (access the backend database is provided by the ``_db`` attribute).


Access
~~~~~~

As LevelDB is really just a big key-value store, LevelPy implements a dict-interface to the database (using the []
operators).

.. code:: python

  item = db['itemkey'] # store value with key 'itemkey' as item
  db['something'] = 'else' # store value 'else' with key 'something'

  a_to_b = db['a':'b']  # get all items between 'a' and 'b' (inclusive)

  has_5 = '5' in db  # tests if '5' is a key in the database

As mentioned in Constructor, such access is also provided by the ``Get`` &
``Put`` members.

Iteration
~~~~~~~~~

Keeping the database dictlike, LevelPy exposes the methods items(), keys(), values(), which provides generators to
iterate over the expected items.

.. code:: python

  keystr = ' '.join(key for key in db.keys())

  for k, v in db.items():
      print(k, '->', v)


Classes
~~~~~~~

Levelpy introduces some specialized classes to solve common problems while working with the database.


LevelDB
^^^^^^^

LevelDB is the main class responsible for loading and querying the database.
A "real" leveldb library/class must be used to actually handle the file io.
To make your own, simply write a class that implements the LevelDB API.


Views
^^^^^

Views are read-only structures that are built with a prefix which is automatically added to any request.
Views may contain other views, creating smaller slices of the full database.

Views provide the levelpy reading-interface: get and iteration.


Sublevels
^^^^^^^^^

Sublevels are like views but provide full read-write support to the database.
The user may create sublevels within a sublevel for more specific requests.
Views may be created from sublevels, but a sublevel cannot be created from a view, as they are read only.

Sublevels provide the levelpy read and write interfaces: get, put, delete, iteration, batch writes.


Serializer
^^^^^^^^^^

LevelDB requires keys and values in the database to be python byte objects, so all other types (such as strings) must be
encoded to bytes upon request or storage.
LevelPy provides a serialization module with functions that implement various encoding/decoding schemes.
Most LevelPy database objects have a value_encoding parameter in the constructor;
if this is a string, it searches the Serializer.transform_dict dictionary for the encode/decode pair with the string.
Alternatively, you can supply a tuple of 2 callables which encode incoming objects to bytes, and decode bytes into
objects.
This, mixed with sublevels, provide an excelent method to store countless different types in a single database, with
automatic type retrieval.

By default the Serializer provides string encoding ("utf8"), trivial binary encoding ("bin"), arbitrary json object
encoding for dicts ("json"), and the more efficient msgpack serialization library ("msgpack", must be installed
seperately)
Custom serialization keys may be added to the transform_dict, for easy access to custom serializations.
It is recommended to call Serializer.update() after modifying the transform_dict, which updates the Serializer's encode
and decode dictionaries.


License
-------

Levelpy is released under the `MIT <https://opensource.org/licenses/MIT>`_ license.



.. _LevelDB: http://leveldb.org/
.. _LevelDB API: http://leveldb.googlecode.com/svn/trunk/doc/index.html


.. |travis-master| image:: https://travis-ci.org/akubera/levelpy.svg?branch=master
                        :target: https://travis-ci.org/akubera/levelpy?branch=master
                        :alt: Testing Report (Master Branch)

.. |coveralls-master| image:: https://coveralls.io/repos/github/akubera/levelpy/badge.svg?branch=master
                           :target: https://coveralls.io/github/akubera/levelpy?branch=master
                           :alt: Coverage Report

.. |version-master| image:: https://img.shields.io/pypi/v/levelpy.svg
                         :target: https://pypi.python.org/pypi/levelpy/
                         :alt: Latest PyPI version


.. |travis-dev| image:: https://travis-ci.org/akubera/levelpy.svg?branch=dev
                     :target: https://travis-ci.org/akubera/levelpy?branch=dev
                     :alt: Testing Report (Master Branch)

.. |coveralls-dev| image:: https://coveralls.io/repos/github/akubera/levelpy/badge.svg?branch=dev
                        :target: https://coveralls.io/github/akubera/levelpy?branch=dev
                        :alt: Coverage Report
