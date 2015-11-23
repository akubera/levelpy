#
# levelpy/db_accessors.py
#

from numbers import Number
from .serializer import Serializer
from .iterviews import (
    LevelItems,
    LevelKeys,
    LevelValues,
)


class LevelAccessor:
    """
    A simple class with a method for transforming strings or bytes into keys,
    and a _key_prefix member which will be prepended to the outgoing key bytes.
    """

    _prefix = b''
    _delim = b''

    def __init__(self, prefix, delim, value_encoding='utf8'):
        self.prefix = prefix
        self.delim = delim

        if isinstance(value_encoding, str):
            self.encode = Serializer.encode[value_encoding]
            self.decode = Serializer.decode[value_encoding]
            self.value_encoding_str = value_encoding

        elif isinstance(value_encoding, (tuple, list)):
            if not all(map(callable, value_encoding)):
                raise TypeError
            self.encode, self.decode = value_encoding
            self.value_encoding_str = None

        # Assume this is a protocol buffer
        elif hasattr(value_encoding, 'SerializeToString') and \
             hasattr(value_encoding, 'ParseFromString'):
            def decode_protocol_buffer(bytestr):
                obj = value_encoding()
                obj.ParseFromString(bytes(bytestr))
                return obj
            self.decode = decode_protocol_buffer
            self.encode = value_encoding.SerializeToString
            self.value_encoding_str = None

        else:
            raise TypeError("value_encoding must be a string or"
                            "encoding/decoding function tuple.")

    def key_transform(self, key):  # -> bytes
        """
        Takes some potential key and returns the bytes that this accessor will
        use to retrieve values from the database. This is done by prefixing the
        key with the accessor's prefix and delimiter.
        """
        return self._key_prefix + self.byteify(key)

    def subkey(self, key):
        """
        Equivalent to key_transform, but returns None if parameter is None
        """
        if key is None:
            return None
        return self.key_transform(key)

    @property
    def _key_prefix(self):
        return self._prefix + self._delim

    @property
    def prefix(self):
        return self._prefix

    @prefix.setter
    def prefix(self, value):
        self._prefix = self.byteify(value)

    @property
    def delim(self):
        return self._delim

    @delim.setter
    def delim(self, value):
        self._delim = self.byteify(value)

    @staticmethod
    def byteify(value) -> bytes:
        """
        Static method to return input as bytes. Currently tries to decode
        string, or if that doesn't work, calls the bytes constructor
        """
        try:
            return value.encode()
        except AttributeError:
            if value is None:
                # Not sure whether None or empty bytes should be returned.
                # return b''
                return None
            elif isinstance(value, (Number, )):
                return str(value).encode()
            else:
                return bytes(value)

    def _get_encoding(self, value_encoding):
        if value_encoding is not None:
            enc = value_encoding
        elif self.value_encoding_str:
            enc = self.value_encoding_str
        else:
            enc = (self.encode, self.decode)
        return enc


class LevelReader(LevelAccessor):
    """
    Class containing methods for reading from an established database.

    This class mimics the dict class by implementing __getitem__ as a handy
    wrapper around the backend's Get and RangeIter methods, along with 'keys',
    'values', 'get', and 'items'. These also pass any keyword arguments to the
    backend server, so no functionality is lost.
    """

    _range_ending = b'~'

    @property
    def range_begin(self):
        return self._key_prefix

    @property
    def range_end(self):
        return self.subkey(self._range_ending)

    def range_start_key(self, key):
        if key is None:
            return self.range_begin
        else:
            return self.key_transform(key)

    def range_stop_key(self, key):
        if key is None:
            return self.range_end
        else:
            return self.key_transform(key)

    def value_decode(self, byte_str: bytes):
        return self.decode(byte_str)

    def get(self, key):
        """
        Normalizes the key, gets bytes from databse, decodes bytes using the
        value_decode method.
        """
        key = self.key_transform(key)
        value_bytes = self.Get(key)
        return self.value_decode(value_bytes)

    def __getitem__(self, key):
        if isinstance(key, slice):
            if key.step is not None:
                raise ValueError("Step is not available for levelpy slices")

            start = self.range_start_key(key.start)
            stop = self.range_stop_key(key.stop)

            return self.RangeIter(key_from=start, key_to=stop)

        elif isinstance(key, (tuple, list, set)):
            t = type(key)
            return t(map(self.__getitem__, key))

        else:
            return self.get(key)

    def items(self, **kwargs):
        """
        Returns an iterator which iterates over the keys, value pairs in the
        database.
        """
        kwargs['key_from'] = self.range_start_key(kwargs.get('key_from', None))
        kwargs['key_to'] = self.range_stop_key(kwargs.get('key_to', None))
        return LevelItems(self, **kwargs)

    def keys(self, **kwargs):
        """
        Returns an iterator which iterates over the keys in the database,
        ignoring values.
        """
        kwargs['key_from'] = self.range_start_key(kwargs.get('key_from', None))
        kwargs['key_to'] = self.range_stop_key(kwargs.get('key_to', None))
        return LevelKeys(self, **kwargs)

    def values(self, **kwargs):
        """
        Returns an iterator which iterates over the values in the database,
        ignoring keys.
        """
        kwargs['key_from'] = self.range_start_key(kwargs.get('key_from', None))
        kwargs['key_to'] = self.range_stop_key(kwargs.get('key_to', None))
        return LevelValues(self, **kwargs)

    def __contains__(self, key):
        search_key = self.key_transform(key)
        stop_key = search_key + self._range_ending
        return search_key in self.RangeIter(key_from=search_key,
                                            key_to=stop_key,
                                            include_value=False)

    def Get(self, key):
        return self._db.Get(key)

    def RangeIter(self, *args, **kwargs):
        return self._db.RangeIter(*args, **kwargs)


class LevelWriter(LevelAccessor):
    """
    Class containing standard methods for writing and deleting items from a
    database.
    """

    def value_encode(self, obj):
        return self.encode(obj)

    def put(self, key, value):
        """
        Normalizes the key, encodes the value, and stores in the database.
        """
        key = self.key_transform(key)
        value = self.value_encode(value)
        self.Put(key, value)

    def __setitem__(self, key, value):
        self.put(key, value)

    def __delitem__(self, key):
        key = self.key_transform(key)
        self.Delete(key)

    def write_batch(self):
        from .batch_context import BatchContext
        return BatchContext(self)

    def Put(self, key, value):
        return self._db.Put(key, value)

    def Delete(self, key):
        return self._db.Delete(key)
