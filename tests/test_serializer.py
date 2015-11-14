#
# tests/test_serializer.py
#

import pytest

import levelpy.serializer as serializer

@pytest.mark.parametrize("input, output", [
    ({'a': 1}, b'{"a": 1}'),
])
def test_json(input, output):
    res = serializer.json_encode(input)
    assert res == output
    res = serializer.json_decode(res)
    assert res == input


@pytest.mark.parametrize("input, output", [
    (b'the quick brown fox', b'the quick brown fox'),
    (bytearray(b'foo'), b'foo'),
    (10, b'10'),
])
def test_utf8_encode(input, output):
    res = serializer.utf8_encode(input)
    assert res == output


@pytest.mark.parametrize("input, output", [
    (b'the quick brown fox', 'the quick brown fox'),
    (bytearray(b'foo'), 'foo'),
])
def test_utf8_decode(input, output):
    res = serializer.utf8_decode(input)
    assert res == output


@pytest.mark.parametrize("input, output", [
    (b'the quick brown fox', b'the quick brown fox'),
    (bytearray(b'foo'), b'foo')
])
def test_binary_encode(input, output):
    res = serializer.binary_encode(input)
    assert res == output


@pytest.mark.parametrize("input, output", [
    (b'the quick brown fox', b'the quick brown fox'),
    (bytearray(b'foo'), b'foo')
])
def test_binary_decode(input, output):
    res = serializer.binary_decode(input)
    assert res == output


@pytest.mark.parametrize("input, output", [
    ('foo', b'\xa3foo'),
    (123, b'{'),
    (b'123', b'\xc4\x03123'),
    ({'a': 1}, b'\x81\xa1a\x01'),
])
def test_msgpack_encoding(input, output):
    msgpack = pytest.importorskip("msgpack")
    res = serializer.MsgPackSerializer.encode(input)
    assert res == output

    res = serializer.MsgPackSerializer.decode(res)
    assert res == input


@pytest.mark.parametrize("format, enc, dec", [
    ('json', serializer.json_encode, serializer.json_decode),
    ('utf-8', serializer.utf8_encode, serializer.utf8_decode),
])
def test_Serializer_constructor(format, enc, dec):
    ser = serializer.Serializer(format)
    assert ser.pack == enc
    assert ser.unpack == dec


def test_Serializer_update():
    ser = serializer.Serializer()
    trans = (1, 2)
    serializer.Serializer.transform_dict['mock'] = trans
    ser.update()
    assert ser.encode['mock'] is 1
    assert ser.decode['mock'] is 2


def test_MsgPackSerializer_constructor():
    ser = serializer.MsgPackSerializer()
    assert ser.pack == serializer.MsgPackSerializer.encode
    assert ser.unpack == serializer.MsgPackSerializer.decode
