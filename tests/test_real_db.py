#
# tests/test_real_db.py
#
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


@pytest.mark.parametrize('k, v', (
  (b'KEY', b'VALUE'),
  ('KEY', 'VALUE'),
))
def test_item_access(db, k, v):
    db[k] = v
    assert k in db
    assert db[k] == v
    # del db[k]


@pytest.mark.parametrize('its', (
  (
   ('a1', 'VALU'),
   ('b2', 'VAL'),
   ('c3', 'VALUE'),
  ),
))
def test_item_iteration(db, its):
    for k, v in its:
        db[k] = v

    l = list(v for k, v in db.items(encoding='utf-8'))

    assert len(l) is 3
    assert all(a[0] == a[1][1] for a in zip(l, its))
