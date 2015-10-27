#
# levelpy/serializer.py
#

import json


def json_encode(obj):
    return json.dumps(obj).encode()


def json_decode(byte_str):
    return json.loads(byte_str.decode())


def utf8_encode(obj):
    return json_encode(obj)


def utf8_decode(byte_str):
    return json_decode(byte_str)


def binary_encode(byte_str):
    return byte_str


def binary_decode(byte_str):
    return byte_str


class MsgPackSerializer:

    def __init__(self):
        self.pack, self.unpack = self.encode, self.decode

    @staticmethod
    def encode(obj):
        import msgpack
        return msgpack.packb(obj, use_bin_type=True)

    @staticmethod
    def decode(byte_str):
        import msgpack
        return msgpack.unpackb(byte_str, encoding='utf-8')


class Serializer:

    transform_dict = {
        'json': (json_encode, json_decode),
        'utf8': (utf8_encode, utf8_decode),
        'bin': (binary_encode, binary_decode),
        'none': (binary_encode, binary_decode),
        'msgpack': (MsgPackSerializer.encode, MsgPackSerializer.decode)
    }

    encode = {k: v[0] for k, v in transform_dict.items()}
    decode = {k: v[1] for k, v in transform_dict.items()}

    def __init__(self, method='utf-8'):
        self.pack, self.unpack = self.transform_dict[method]
