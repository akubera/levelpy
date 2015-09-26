#
# tests/test_leveldb.py
#

from copy import copy
import pytest
from unittest import mock
from levelpy.sublevel import Sublevel
from levelpy.leveldb import LevelDB


@pytest.fixture
def key():
    return 'A'


@pytest.fixture
def delim():
    return '!'


@pytest.fixture
def k_d(key, delim):
    return key + delim


@pytest.fixture
def db():
    return mock.create_autospec(LevelDB)


@pytest.fixture
def sub(db, key, delim):
    return Sublevel(db, key, delim)


def test_constructor(sub, db, key, delim):
    assert isinstance(sub, Sublevel)
    assert sub.db is db
    assert sub.prefix is key
    assert sub.delim is delim


def test_get_item(sub, db, k_d):
    sub['a']
    db.__getitem__.assert_called_with(k_d + 'a')


def test_get_slice(sub, db, k_d):
    sub['a':'b']
    db.__getitem__.assert_called_with(slice(k_d + 'a', k_d + 'b'))


def test_get_slice_with_start(sub, db, k_d):
    key = '1'
    sub[key:]
    db.__getitem__.assert_called_with(slice(k_d+key, None))


def test_get_slice_with_stop(sub, db, k_d):
    key = '1'
    sub[:key]
    db.__getitem__.assert_called_with(slice(None, k_d+key))


def test_get_bad_slice(sub, db, k_d):
    with pytest.raises(ValueError):
        sub['a':'b':2]


@pytest.mark.parametrize('input, args', [
    (('1', '2', '3'), ('A!1', 'A!2', 'A!3')),
    (['1', '2'], ['A!1', 'A!2']),
    ({'1', '2'}, {'A!1', 'A!2'}),
])
def test_get_slice_with_collection(sub, db, k_d, input, args):
    sub[input]
    db.__getitem__.assert_called_with(args)


def test_set_item(sub, db, k_d):
    key = 'input'
    sub[key] = 90
    db.__setitem__.assert_called_with(k_d + key, 90)


def test_del_item(sub, db, k_d):
    key = 'whatever'
    del sub[key]
    db.__delitem__.assert_called_with(k_d + key)


def test_copy(sub, db, key, delim):
    cp = copy(sub)
    assert cp.db is db
    assert cp.prefix is key
    assert cp.delim is delim


def test_create_sublevel(sub, db, k_d):
    key = 'a'
    a = sub.sublevel(key)
    expected_prefix = k_d + key
    assert a.db is db
    assert a.prefix == expected_prefix


def test_items(sub, db, k_d):
    start, stop = k_d, k_d + "~"
    for i in sub.items():
        pass
    db.items.assert_called_with(key_from=start, key_to=stop)


def test_contains(sub, db, k_d):
    key = 'a'
    key in sub
    assert db.__contains__.called_with(k_d + key)
