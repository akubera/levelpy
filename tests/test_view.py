#
# tests/test_view.py
#

from copy import copy
import pytest
from unittest import mock
from levelpy.view import View
from levelpy.leveldb import LevelDB


@pytest.fixture
def key():
    return b'A'


@pytest.fixture
def delim():
    return b'!'


@pytest.fixture
def k_d(key, delim):
    return key + delim


@pytest.fixture
def db():
    return mock.create_autospec(LevelDB)


@pytest.fixture
def view(db, key, delim):
    return View(db, key, delim)


def test_constructor(view, db, key, delim):
    assert isinstance(view, View)
    assert view.db is db
    assert view.prefix is key
    assert view.delim is delim


def test_get_item(view, db, k_d):
    view['a']
    db.__getitem__.assert_called_with(k_d + b'a')


def test_get_slice(view, db, k_d):
    view['a':'b']
    db.__getitem__.assert_called_with(slice(k_d + b'a', k_d + b'b'))


def test_get_slice_with_start(view, db, k_d):
    key = '1'
    view[key:]
    db.__getitem__.assert_called_with(slice(k_d+b'1', None))


def test_get_slice_with_stop(view, db, k_d):
    key = '1'
    view[:key]
    db.__getitem__.assert_called_with(slice(None, k_d + b'1'))


def test_get_bad_slice(view, db, k_d):
    with pytest.raises(ValueError):
        view['a':'b':2]


@pytest.mark.parametrize('input, args', [
    (('1', '2', '3'), (b'A!1', b'A!2', b'A!3')),
    (['1', '2'], [b'A!1', b'A!2']),
    ({'1', '2'}, {b'A!1', b'A!2'}),
])
def test_get_slice_with_collection(view, db, k_d, input, args):
    view[input]
    db.__getitem__.assert_called_with(args)


def test_copy(view, db, key, delim):
    cp = copy(view)
    assert cp.db is db
    assert cp.prefix is key
    assert cp.delim is delim


def test_create_view(view, db, k_d):
    key = 'a'
    a = view.view(key)
    expected_prefix = k_d + b'a'
    assert a.db is db
    assert a.prefix == expected_prefix


def test_items(view, db, k_d):
    start, stop = k_d, k_d + b"~"
    for i in view.items():
        pass
    db.items.assert_called_with(key_from=start, key_to=stop)


def test_contains(view, db, k_d):
    key = 'a'
    key in view
    assert db.__contains__.called_with(k_d + b'a')


def test_keys(view, db, k_d):
    for x in view.keys():
        pass
    db.items.assert_called_with(include_value=False,
                                key_from=k_d,
                                key_to=k_d+b'~')


def test_values(view, db, k_d):
    for x in view.values():
        pass
    db.items.assert_called_with(include_value=True,
                                key_from=k_d,
                                key_to=k_d+b'~')