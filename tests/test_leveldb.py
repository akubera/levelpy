#
# tests/test_leveldb.py
#

import pytest
from unittest import mock
import levelpy.leveldb


@pytest.fixture
def mock_WriteBatch():
    leveldb = pytest.importorskip('leveldb')
    return mock.MagicMock(spec=leveldb.WriteBatch)
    # return mock.create_autospec(leveldb.WriteBatch)


@pytest.fixture
def mock_leveldb_backend(mock_WriteBatch):
    leveldb = pytest.importorskip('leveldb')
    db = mock.MagicMock(spec=leveldb.LevelDB,
                        WriteBatch=lambda: mock_WriteBatch)
    db.Get.return_value = b'x'
    return db


def test_mock_batch(mock_WriteBatch, mock_leveldb_backend):
    b = mock_leveldb_backend.WriteBatch()
    assert b is mock_WriteBatch


@pytest.fixture
def mock_LevelDB(mock_leveldb_backend, recwarn):
    leveldb = pytest.importorskip('leveldb')
    module = mock.MagicMock(spec=leveldb)
    module.LevelDB.return_value = mock_leveldb_backend
    return module


@pytest.fixture
def db_path():
    return '.'


@pytest.fixture
def db(db_path, mock_LevelDB):
    return levelpy.leveldb.LevelDB(db=db_path,
                                   leveldb_cls=mock_LevelDB.LevelDB)


def test_constructor(db, db_path, mock_LevelDB, mock_leveldb_backend):
    assert isinstance(db, levelpy.leveldb.LevelDB)
    assert db._db is mock_leveldb_backend
    assert db._db.Get is mock_leveldb_backend.Get
    mock_LevelDB.LevelDB.assert_called_with(db_path)


def test_constructor_string_backend():
    with pytest.raises(ImportError):
        levelpy.leveldb.LevelDB('.', 'nopkg')


def test_getitem(db, mock_leveldb_backend):
    db['a']
    mock_leveldb_backend.Get.assert_called_with(b'a')


def test_setitem(db, mock_leveldb_backend):
    db['a'] = 'a'
    mock_leveldb_backend.Put.assert_called_with(b'a', b'a')


def test_delitem(db, mock_leveldb_backend):
    del db[b'a']
    mock_leveldb_backend.Delete.assert_called_with(b'a')


def test_delitem_strkey(db, mock_leveldb_backend):
    del db['a']
    mock_leveldb_backend.Delete.assert_called_with(b'a')


def test_get_slice_with_start(db, mock_leveldb_backend):
    db[1:]
    mock_leveldb_backend.RangeIter.assert_called_with(key_from=b'1',
                                                      key_to=None)


def test_get_slice_with_stop(db, mock_leveldb_backend):
    db[:1]
    mock_leveldb_backend.RangeIter.assert_called_with(key_from=None,
                                                      key_to=b'1')


def test_get_slice_with_start_stop(db, mock_leveldb_backend):
    db['a':'z']
    mock_leveldb_backend.RangeIter.assert_called_with(key_from=b'a',
                                                      key_to=b'z')


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
    bkey = 'a'.encode()
    key in db
    mock_leveldb_backend.RangeIter.assert_called_with(include_value=False,
                                                      key_from=bkey,
                                                      key_to=bkey + b'~')


def test_items(db):
    for x in db.items():
        assert x
    # mock_leveldb_backend.__contains__.assert_called_with('a')


def test_keys(db):
    for x in db.keys():
        assert x
    # mock_leveldb_backend.__contains__.assert_called_with('a')


def test_values(db, mock_leveldb_backend):
    mock_leveldb_backend.RangeIter.return_value = [(b'True', b'True')]
    for x in db.values():
        assert x == 'True'
    # mock_leveldb_backend.__contains__.assert_called_with('a')


def test_copy(db, mock_leveldb_backend):
    from copy import copy
    carbon = copy(db)
    assert carbon._db is mock_leveldb_backend
    # mock_leveldb_backend.__contains__.assert_called_with('a')

# def test_mock_batch(mock_WriteBatch, mock_leveldb_backend):
#     b = mock_leveldb_backend.WriteBatch()
#     assert b is mock_WriteBatch
#
#

def test_batch_context_type(db, mock_leveldb_backend, mock_WriteBatch):
    from levelpy.batch_context import BatchContext
    ctx = db.write_batch()
    assert isinstance(ctx, BatchContext)
    assert db.WriteBatch is mock_leveldb_backend.WriteBatch
    assert ctx.batch is mock_WriteBatch


def test_batch(db, mock_leveldb_backend, mock_WriteBatch):
    with db.batch() as ctx:
        ctx['1'] = '0'

    mock_WriteBatch.Put.assert_called_with(b'1', b'0')
    mock_leveldb_backend.Write.called_with(mock_WriteBatch, False)


def test_batch_exception(db, mock_leveldb_backend, mock_WriteBatch):

    with pytest.raises(Exception):
        with db.batch() as ctx:
            ctx['1'] = '0'
            raise Exception

    mock_WriteBatch.Put.assert_called_with(b'1', b'0')
    assert not mock_leveldb_backend.Write.called


def test_destroy_db(db, mock_LevelDB):
    with pytest.raises(NotImplementedError):
        db.destroy_db()
    # assert mock_LevelDB.DestroyDB.called


def test_stats(db, mock_leveldb_backend):
    db.stats()
    assert mock_leveldb_backend.GetStats.called


def test_create_snapshot(db, mock_leveldb_backend):
    db.create_snapshot()
    assert mock_leveldb_backend.CreateSnapshot.called


def test_create_sublevel(db, mock_leveldb_backend):
    a = db.sublevel('a')
    assert a._db is db
    assert a.prefix == b'a'
    assert a._key_prefix == b'a!'
