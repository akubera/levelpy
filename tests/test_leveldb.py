#
# tests/test_leveldb.py
#

import pytest
from unittest import mock
import levelpy.leveldb
from warnings import warn


@pytest.fixture
def mock_leveldb_backend():
    try:
        import leveldb
        db = mock.MagicMock(spec=leveldb.LevelDB)
    except ImportError:
        db = mock.MagicMock()
        warn("Error importing leveldb: "
             "Some tests may pass when they shouldn't")
    return db


@pytest.fixture
def mock_LevelDB(mock_leveldb_backend, recwarn):
    try:
        import leveldb
        module = mock.MagicMock(spec=leveldb)
    except ImportError:
        module = mock.MagicMock()
        warn("Error importing leveldb: "
             "Some tests may pass when they shouldn't")
    module.LevelDB.return_value = mock_leveldb_backend
    return module


@pytest.fixture
def db_path():
    return '.'


@pytest.fixture
def db(db_path, mock_LevelDB):
    return levelpy.leveldb.LevelDB(db=db_path, database_package=mock_LevelDB)


def test_constructor(db, db_path, mock_LevelDB, mock_leveldb_backend):
    assert isinstance(db, levelpy.leveldb.LevelDB)
    assert db._db is mock_leveldb_backend
    mock_LevelDB.LevelDB.assert_called_with(db_path)


def test_constructor_string_backend():
    with pytest.raises(ImportError):
        levelpy.leveldb.LevelDB('.', 'nopkg')


def test_getitem(db, mock_leveldb_backend):
    db['a']
    mock_leveldb_backend.Get.assert_called_with('a')


def test_setitem(db, mock_leveldb_backend):
    db['a'] = 'a'
    mock_leveldb_backend.Put.assert_called_with('a', 'a')


def test_delitem(db, mock_leveldb_backend):
    del db['a']
    mock_leveldb_backend.Delete.assert_called_with('a')


def test_get_slice_with_start(db, mock_leveldb_backend):
    db[1:]
    mock_leveldb_backend.RangeIter.assert_called_with(key_from=1,
                                                      key_to=None)


def test_get_slice_with_stop(db, mock_leveldb_backend):
    db[:1]
    mock_leveldb_backend.RangeIter.assert_called_with(key_from=None,
                                                      key_to=1)


def test_get_slice_with_start_stop(db, mock_leveldb_backend):
    db['a':'z']
    mock_leveldb_backend.RangeIter.assert_called_with(key_from='a',
                                                      key_to='z')


def test_get_slice_with_start_stop_step(db, mock_leveldb_backend):
    with pytest.raises(ValueError):
        db['a':'z':3]


@pytest.mark.parametrize('input', [
    (1, 2, 3),
    ['1', '2']
])
def test_get_slice_with_collection(db, input):
    something = db[input]
    assert type(something) is type(input)
    assert db._db.Get.call_count is len(input)


def test_contains(db, mock_leveldb_backend):
    # TODO: Remove implementation details
    key = 'a'
    key in db
    mock_leveldb_backend.RangeIter.assert_called_with(include_value=False,
                                                      key_from=key,
                                                      key_to=key+'~')


def test_items(db):
    for x in db.items():
        assert x
    # mock_leveldb_backend.__contains__.assert_called_with('a')


def test_keys(db):
    for x in db.keys():
        assert x
    # mock_leveldb_backend.__contains__.assert_called_with('a')


def test_values(db, mock_leveldb_backend):
    mock_leveldb_backend.RangeIter.return_value = [(True, True)]
    for x in db.values():
        assert x
    # mock_leveldb_backend.__contains__.assert_called_with('a')


def test_copy(db, mock_leveldb_backend):
    from copy import copy
    carbon = copy(db)
    assert carbon._db is mock_leveldb_backend
    # mock_leveldb_backend.__contains__.assert_called_with('a')


def test_write_batch(db):
    with db.write_batch() as ctx:
        assert ctx is not None


def test_batch(db):
    with db.batch() as ctx:
        assert ctx is not None


def test_destroy_db(db, mock_LevelDB):
    db.destroy_db()
    assert mock_LevelDB.DestroyDB.called


def test_stats(db, mock_leveldb_backend):
    db.stats()
    assert mock_leveldb_backend.GetStats.called


def test_create_snapshot(db, mock_leveldb_backend):
    db.create_snapshot()
    assert mock_leveldb_backend.CreateSnapshot.called


def test_create_sublevel(db, mock_leveldb_backend):
    a = db.sublevel('a')
    assert a.db is db
    assert a.prefix is 'a'
