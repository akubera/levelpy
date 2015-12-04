#
# tests/test_view.py
#

from copy import copy
import pytest
from unittest import mock
from levelpy.view import View
from levelpy.leveldb import LevelDB
from levelpy.iterviews import (
    LevelItems,
    LevelValues,
    LevelKeys,
)


@pytest.fixture
def key():
    return b'A'


@pytest.fixture
def delim():
    return b'!'


@pytest.fixture
def value_encode_str():
    return 'bin'


@pytest.fixture
def k_d(key, delim):
    return key + delim


@pytest.fixture
def db():
    return mock.create_autospec(LevelDB)


@pytest.fixture
def view(db, key, delim, value_encode_str):
    return View(db, key, delim, value_encode_str)


def test_constructor(view, db, key, delim, value_encode_str):
    assert isinstance(view, View)
    assert view._db is db
    assert view.prefix is key
    assert view.delim is delim
    assert view.value_encoding_str is value_encode_str


def test_get_item(view, db, k_d):
    view['a']
    db.Get.assert_called_with(k_d + b'a')


def test_get_slice(view, db, k_d):
    view['a':'b']
    db.RangeIter.assert_called_with(
        key_from=k_d + b'a',
        key_to=k_d + b'b',
    )


def test_get_slice_with_start(view, db, k_d):
    key = '1'
    view[key:]
    db.RangeIter.assert_called_with(
        key_from=k_d + b'1',
        key_to=view.range_end
    )


def test_get_slice_with_stop(view, db, k_d):
    key = '1'
    view[:key]
    range_end = k_d + key.encode()
    db.RangeIter.assert_called_with(
        key_from=view.range_begin,
        key_to=range_end
    )


def test_get_bad_slice(view, db, k_d):
    with pytest.raises(ValueError):
        view['a':'b':2]


@pytest.mark.parametrize('input, args', [
    (('1', '2', '3'), (b'A!1', b'A!2', b'A!3')),
    (['1', '2'], [b'A!1', b'A!2']),
    ({'1', '2'}, {b'A!1', b'A!2'}),
])
def test_get_with_collection(view, db, k_d, input, args):
    ret = view[input]
    assert type(ret) == type(input)
    expected_args = [((a,),) for a in args]
    # 'set' is special case - unknown call order
    if isinstance(input, set):
        assert all(c in expected_args for c in db.Get.call_args_list)
    else:
        assert db.Get.call_args_list == expected_args


def test_copy(view, db, key, delim):
    cp = copy(view)
    assert cp._db is db
    assert cp.prefix is key
    assert cp.delim is delim


def test_create_view(view, db, k_d):
    key = 'a'
    a = view.view(key)
    expected_prefix = k_d + b'a'
    assert a._db is db
    assert a.prefix == expected_prefix


def test_contains(view, db, k_d):
    key = 'a'
    key in view
    assert db.__contains__.called_with(view.subkey('a'))


def test_items(view, db, k_d):
    start, stop = k_d, k_d + b"~"
    items = view.items()
    assert isinstance(items, LevelItems)
    for i in items: pass

    db.RangeIter.assert_called_with(
        key_from=start,
        key_to=stop,
        include_value=True,
        reverse=False,
        )


def test_keys(view, db, k_d):
    keys = view.keys()
    assert keys._db._db is db
    assert isinstance(keys, LevelKeys)
    for k in keys: pass

    db.RangeIter.assert_called_with(
        include_value=False,
        reverse=False,
        key_from=k_d,
        key_to=k_d + b'~',
    )


def test_values(view, db, k_d):
    vals = view.values()
    assert isinstance(vals, LevelValues)
    for v in vals: pass
    db.RangeIter.assert_called_with(
        reverse=False,
        include_value=True,
        key_from=k_d,
        key_to=k_d + b'~',
    )

@pytest.mark.parametrize("keys, expected", [
    (["a", "b", "c"], b"a!b!c"),
])
def test_join(view, keys, expected):
    assert view.join(*keys) == expected


@pytest.mark.parametrize("key, inkey, expected", [
    ("abcd", "abcd!123", b"123"),
    ("abcd", "xabcd!123", b"xabcd!123"),
])
def test_strip_prefix(view, inkey, expected):
    assert view.strip_prefix(inkey) == expected
