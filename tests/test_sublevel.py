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
def sub(db, key, delim):
    return Sublevel(db, key, delim)


def test_constructor(sub, db, key, delim, k_d):
    assert isinstance(sub, Sublevel)
    assert sub._db is db
    assert sub.prefix is key
    assert sub.delim is delim
    assert sub.range_begin == k_d
    assert sub.range_end == k_d + b"~"


def test_get_item(sub, db, k_d):
    sub['a']
    db.Get.assert_called_with(k_d + b'a')


def test_get_slice(sub, db, k_d):
    sub['a':'b']
    db.RangeIter.assert_called_with(key_from=k_d + b'a', key_to=k_d + b'b')


def test_get_slice_with_start(sub, db, k_d):
    key = '1'
    sub[key:]
    db.RangeIter.assert_called_with(key_from=k_d + b'1', key_to=sub.range_end)


def test_get_slice_with_stop(sub, db, k_d):
    sub[:'1']
    range_end = k_d + b'1'
    db.RangeIter.assert_called_with(key_from=sub.range_begin, key_to=range_end)


def test_get_bad_slice(sub, db, k_d):
    with pytest.raises(ValueError):
        sub['a':'b':2]


@pytest.mark.parametrize('input, args', [
    (('1', '2', '3'), (b'A!1', b'A!2', b'A!3')),
    (['1', '2'], [b'A!1', b'A!2']),
    ({'1', '2'}, (b'A!1', b'A!2')),
])
def test_get_with_collection(sub, db, k_d, input, args):
    ret = sub[input]
    assert type(ret) == type(input)
    expected_args = [((a,),) for a in args]
    # 'set' is special case - unknown call order
    if isinstance(input, set):
        assert all(c in expected_args for c in db.Get.call_args_list)
    else:
        assert db.Get.call_args_list == expected_args

def test_set_item(sub, db, k_d):
    key = 'a'
    sub[key] = 90
    db.Put.assert_called_with(k_d + b'a', b'90')


def test_del_item(sub, db, k_d):
    key = 'a'
    del sub[key]
    db.Delete.assert_called_with(k_d + b'a')


def test_copy(sub, db, key, delim):
    cp = copy(sub)
    assert cp._db is db
    assert cp.prefix is key
    assert cp.delim is delim


def test_create_sublevel(sub, db, k_d):
    key = 'a'
    a = sub.sublevel(key)
    expected_prefix = k_d + b'a'
    assert a._db is db
    assert a.prefix == expected_prefix


def test_create_view(sub, db, k_d):
    key = 'a'
    a = sub.view(key)
    expected_prefix = k_d + b'a'
    assert a._db is db
    assert a.prefix == expected_prefix


def test_items(sub, db, k_d):
    start, stop = k_d, k_d + b"~"
    for i in sub.items():
        pass
    db.items.assert_called_with(key_from=start, key_to=stop)


def test_contains(sub, db, k_d):
    key = 'a'
    key in sub
    db.RangeIter.assert_called_with(key_from=b'A!a',
                                    key_to=k_d + b'a~',
                                    include_value=False,)
    # assert db.__contains__.called_with(k_d + b'a')


def test_keys(sub, db, k_d):
    return
    for x in sub.keys():
        pass
    db.items.assert_called_with(
        include_value=False,
        key_from=k_d,
        key_to=k_d + b'~'
    )


def test_values(sub, db, k_d):
    vals = sub.values()
    for _ in vals: pass
    db.RangeIter.assert_called_with(
        include_value=True,
        key_from=sub.range_begin,
        key_to=sub.range_end,
        reverse=False,
    )


def test_custom_encoder(db, key, delim, k_d):

    def encode_number(num):
        n_type = ('i' if isinstance(num, int) else
                  'f' if isinstance(num, float) else
                  '?')
        return (n_type + "%d" % num).encode()

    def decode_number(bytestr):
        typecode, *num_val = bytestr.decode()
        num_val = ''.join(num_val)
        n_type = {
            'i': int,
            'f': float,
            '?': float,
        }.get(typecode)
        return n_type(num_val)

    sub = Sublevel(db, key, delim, value_encoding=(encode_number, decode_number))
    assert sub.encode is encode_number
    assert sub.decode is decode_number

    sub['foo'] = 20
    db.Put.assert_called_with(k_d + b'foo', b'i20')

    sub['bar'] = 42.4
    db.Put.assert_called_with(k_d + b'bar', b'f42')

    db.Get.return_value = b'i20'
    twenty = sub['foo']
    assert twenty == 20 and isinstance(twenty, int)

    db.Get.return_value = b'f42'
    twenty = sub['bar']
    assert twenty == 42.0 and isinstance(twenty, float)
