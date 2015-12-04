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
from itertools import zip_longest

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
    return LevelDB(leveldir, backend_class_str, create_if_missing=True)


@pytest.fixture
def cleared_database(opened_db):
    fresh_db.DestroyDB()
    return fresh_db


@pytest.fixture
def fresh_db(cleared_database):
    return LevelDB(cleared_database)


@pytest.fixture
def filled_db(db, data):
    for k, v in data:
        db[k] = v
    return db


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

@pytest.mark.parametrize('data, range_, expected', (
    ([('A0', 'a'), ('A1', 'b'), ('C', 'c')],
      ('a', 'b'),
      [(b'A0', 'a'), (b'A1', 'b'), (b'C', 'c')]
    ),
))
def test_items(filled_db, range_, expected):
    db = filled_db
    items = db.items()
    for a, b in zip(items, expected):
        assert a == b


@pytest.mark.parametrize('data, range_, expected', (
    ([('A0', 'a'), ('A1', 'b'), ('C', 'c')],
      ('a', 'b'),
      [(b'C', 'c'), (b'A1', 'b'), (b'A0', 'a')]
    ),
))
def test_items_reversed(filled_db, range_, expected):
    db = filled_db
    items = db.items()
    for a, b in zip(reversed(items), expected):
        assert a == b


@pytest.mark.parametrize('data, range_, expected', (
    ([('A0', 'a'), ('A1', 'b'), ('C', 'c')],
      ('a', 'b'),
      [b'A0', b'A1', b'C']
    ),
))
def test_keys(filled_db, range_, expected):
    db = filled_db
    keys = db.keys()
    for a, b in zip(keys, expected):
        assert a == b


@pytest.mark.parametrize('data, range_, expected', (
    ([('A0', 'a'), ('A1', 'b'), ('C', 'c')],
      ('a', 'b'),
      [b'C', b'A1', b'A0']
    ),
))
def test_keys_reversed(filled_db, range_, expected):
    db = filled_db
    keys = db.keys()
    for a, b in zip(reversed(keys), expected):
        assert a == b


@pytest.mark.parametrize('data, range_, expected', (
    ([('A0', 'a'), ('A1', 'b'), ('C', 'c')],
      ('a', 'b'),
      ['a', 'b', 'c']
    ),
))
def test_values(filled_db, range_, expected):
    db = filled_db
    vals = db.values()
    for a, b in zip(vals, expected):
        assert a == b


@pytest.mark.parametrize('data, range_, expected', (
    ([('A0', 'a'), ('A1', 'b'), ('C', 'c')],
      ('a', 'b'),
      ['c', 'b', 'a']
    ),
))
def test_values_reversed(filled_db, range_, expected):
    db = filled_db
    vals = db.values()
    for a, b in zip(reversed(vals), expected):
        assert a == b


def test_tostring(db):
    vals = db.values()
    str(vals) == "<LevelValues>"


unique_subkey_data = [
    ('!A!X', 'a'),
    (b'!A!\xFF', 'a'),
    ('!B!n!0', 'b'),
    ('!B!n!3', 'c'),
    ('!B!n!1', 'xxx'),
    ('!B!m', 'h'),
    ('!B!m!4', 'g'),
    ('!B!z', 'd'),
    ('!Bc!z', 'bc'),
    ('!C', 'XX'),
    ('!D', 'XX'),
]


@pytest.mark.parametrize('data', [
    unique_subkey_data,
])
@pytest.mark.parametrize('viewkey, expected', [
    ('!B', [b'm', b'n', b'z']),
    ('!A', [b'X', b'\xFF']),
    ('!C', []),
    ('', [b'A', b'B', b'Bc', b'C', b'D']),
])
def test_unique_subkeys(filled_db, viewkey, expected):
    db = filled_db
    view = db.view(viewkey)
    ukeys = view.unique_subkeys()
    for a, b in zip_longest(ukeys, expected):
        assert a == b


@pytest.mark.parametrize('data', [
    unique_subkey_data,
])
@pytest.mark.parametrize('viewkey, expected', [
    ('!B', [b'z', b'n', b'm']),
    ('!A', [b'\xFF', b'X']),
    ('!C', []),
    ('', [b'D', b'C', b'Bc', b'B', b'A']),
])
def test_unique_subkeys_reversed(filled_db, viewkey, expected):
    db = filled_db
    view = db.view(viewkey)
    ukeys = view.unique_subkeys()
    for a, b in zip_longest(reversed(ukeys), expected):
        assert a == b


@pytest.mark.parametrize('data', [
    unique_subkey_data,
])
@pytest.mark.parametrize('viewkey, find_this, expected', [
    ('!Z', 'a', (None, None)),
    ('!B', 'n', (b'!B!n!0', 'b')),
    ('!A', 'n', (None, None)),
    ('!A', '', (b'!A!X', 'a')),
])
def test_find_first_matching(filled_db, viewkey, find_this, expected):
    db = filled_db
    view = db.view(viewkey)
    found = view.find_first_matching(find_this)
    assert found == expected


@pytest.mark.parametrize('data', [
    unique_subkey_data,
])
@pytest.mark.parametrize('viewkey, find_this, expected', [
    ('!Z', 'a', (None, None)),
    ('!B', 'n', (b'!B!n!3', 'c')),
    ('!A', 'n', (None, None)),
    ('!A', '', (b'!A!\xFF', 'a')),
])
def test_find_last_matching(filled_db, viewkey, find_this, expected):
    db = filled_db
    view = db.view(viewkey)
    found = view.find_last_matching(find_this)
    assert found == expected
