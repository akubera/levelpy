#
# tests/test_leveldb.py
#

import pytest
from unittest import mock
import levelpy.batch_context
import levelpy.leveldb
from warnings import warn


@pytest.fixture
def mock_write_batch():
    leveldb = pytest.importorskip('leveldb')
    return mock.create_autospec(leveldb.WriteBatch())


@pytest.fixture
def mock_db(mock_write_batch):
    db = mock.MagicMock(spec=levelpy.leveldb.LevelDB,
                        WriteBatch=lambda: mock_write_batch)
    db.Write = mock.Mock()
    db.encode = lambda x: x
    db.decode = lambda x: x
    return db


@pytest.fixture
def context(mock_db):
    context = levelpy.batch_context.BatchContext(mock_db)
    return context

@pytest.fixture
def batchdb(context):
    return context.__enter__()


def test_constructor(context, mock_db, mock_write_batch):
    assert context is not None
    assert context._db is mock_db
    assert context.batch is mock_write_batch


def test_enter(batchdb, mock_write_batch, mock_db):
    db = batchdb
    db.Put('foo', 'bar')
    db.Delete('foo')

    mock_write_batch.Put.assert_called_with('foo', 'bar')
    mock_write_batch.Delete.assert_called_with('foo')

    assert not mock_db.Put.called
    assert not mock_db.Delete.called

def test_exit(context, mock_db, mock_write_batch):
    # mock_db.
    context.__exit__(None, None, None)
    mock_db.Write.AssertCalledWith(mock_write_batch, False)


def test_with_noerror(context, mock_db, mock_write_batch):
    with context:
        pass
    mock_db.Write.AssertCalledWith(mock_write_batch, False)


def test_with_error(context, mock_db, mock_write_batch):
    with pytest.raises(Exception):
        with context:
            raise Exception()
    mock_db.Write.AssertNotCalled()


def test_sync_works(mock_db):
    BatchContext = levelpy.batch_context.BatchContext
    with BatchContext(mock_db, True):
        pass
    mock_db.Write.AssertCalledWith(mock_write_batch, True)


def test_write_batch(batchdb, mock_write_batch, mock_db):
    with pytest.raises(RuntimeError):
        s = batchdb.write_batch()
