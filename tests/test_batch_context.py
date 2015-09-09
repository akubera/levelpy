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
    import leveldb
    return mock.create_autospec(leveldb.WriteBatch())


@pytest.fixture
def mock_db(mock_write_batch):
    db = mock.MagicMock(spec=levelpy.leveldb.LevelDB)
    db.leveldb.WriteBatch.return_value = mock_write_batch
    db.Write = mock.Mock()
    return db


@pytest.fixture
def context(mock_db):
    context = levelpy.batch_context.BatchContext(mock_db)
    return context


def test_constructor(context, mock_db, mock_write_batch):
    assert context is not None
    assert context.db is mock_db
    assert context.batch is mock_write_batch


def test_enter(context, mock_write_batch):
    db = context.__enter__()
    assert db.Put is mock_write_batch.Put


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
