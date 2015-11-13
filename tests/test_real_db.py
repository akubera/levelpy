#
# tests/test_real_db.py
#
# flake8: noqa
"""
Tests for storage/retreival of a real database.
"""
import pytest
from fixtures import leveldir
from levelpy.leveldb import LevelDB


@pytest.fixture(scope='module')
def backend_class_str():
    return "leveldb.LevelDB"


@pytest.fixture(scope='module')
def backend_package(backend_class_str):
    pkg_name = ".".join(backend_class_str.split(".")[:-1])
    return pytest.importorskip(pkg_name)


@pytest.fixture(scope='module')
def backend_class(backend_package, backend_class_str):
    classname = backend_class_str.split(".")[-1]
    return getattr(backend_package, classname)


@pytest.fixture
def opened_db(backend_class, leveldir):
    return backend_class(leveldir)


@pytest.fixture
def db(backend_class_str, leveldir):
    return LevelDB(leveldir, backend_class_str)


@pytest.fixture
def cleared_database(opened_db):
    fresh_db.DestroyDB()
    return fresh_db


@pytest.fixture
def fresh_db(cleared_database):
    return LevelDB(cleared_database)


def test_backend_class(backend_class, leveldir):
    db = backend_class(leveldir)
    assert isinstance(db, backend_class)


def test_db_type(db):
    assert isinstance(db, LevelDB)


@pytest.mark.parametrize('k, v, expected', (
  (b'KEY', b'VALUE', 'VALUE'),
  ('KEY', 'VALUE', 'VALUE'),
))
def test_item_access(db, k, v, expected):
    db[k] = v
    assert k in db
    assert db[k] == expected

@pytest.mark.parametrize('key, data, expected', (
  ('needle', {'a': 'b', 'z': 'y'}, False),
  ('needle', {'a': 'b', 'needle':'haystack', 'r':'t'}, True),
  ('x', {'a': 'x', 'c': 'd', 'r': 't'}, False),
))
def test_contains(db, key, data, expected):
    for k, v in data.items():
        db[k] = v
    assert (key in db) == expected

@pytest.mark.parametrize('data', (
  (
   ('a1', 'VALU'),
   ('b2', 'VAL'),
   ('c3', 'VALUE'),
  ),
))
def test_item_iteration(db, data):
    for k, v in data:
        db[k] = v

    # l = list(v for k, v in db.items())
    l = []
    for k, v in db.items():
        print(k,'->',v)
        l.append(v)

    assert len(l) is len(data)
    assert all([a[0] == a[1][1] for a in zip(l, data)])


@pytest.mark.parametrize('slice_, data, expected', (
    ( (b'A', b'B'),
      [('A0', 'a'), ('A1', 'b'), ('C', 'c')],
      ('a', 'b')
    ),
    ( ('A', 'B'),
      [('A0', 'a'), ('A1', 'b'), ('C', 'c')],
      ('a', 'b')
    ),
    ( ('A', None),
      [('A0', 'a'), ('A1', 'b'), ('C', 'c')],
      ('a', 'b', 'c')
    ),
    ( ('A1', None),
      [('A0', 'a'), ('A1', 'b'), ('C', 'c')],
      ('b', 'c')
    ),
    ( (None, 'A1'),
      [('A0', 'a'), ('A1', 'b'), ('C', 'c')],
      ('a', 'b')
    ),
    ( (None, None),
      [('A0', 'a'), ('A1', 'b'), ('C', 'c')],
      ['a', 'b', 'c']
    ),
))
def test_item_iteration_slice(db, slice_, data, expected):

    for k, v in data:
        db[k] = v

    l = type(expected)(v.decode() for k, v in db[slice_[0]:slice_[1]])

    assert len(l) is len(expected)
    assert l == expected
