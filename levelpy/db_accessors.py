#
# levelpy/db_accessors.py
#

from .batch_context import BatchContext
from numbers import Number

class LevelAccessor:
    """
    A simple class with a method for transforming strings or bytes into keys,
    and a _key_prefix member which will be prepended to the outgoing key bytes.
    """

    _prefix = b''
    _delim = b''

    def __init__(self, prefix, delim):
        self.prefix = prefix
        self.delim = delim

    def key_transform(self, key):  # -> bytes
        """
        Takes some potential key and returns the bytes that that this accessor
        can use to retrieve values from the database.
        """
        return self._key_prefix + self.byteify(key)

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


class LevelReader(LevelAccessor):
    """
    Class containing methods for reading from an established database.
    """

    _range_ending = b'~'

    def __init__(self, prefix, delim):
        LevelAccessor.__init__(self, prefix, delim)

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

            # Note - if None, these should remain None
            start, stop = map(self.byteify, (key.start, key.stop))

            return self.RangeIter(key_from=start, key_to=stop)

        elif isinstance(key, (tuple, list, set)):
            t = type(key)
            return t(self[k] for k in key)

        else:
            return self.get(key)

    def items(self, *args, **kwargs):

        def transform(obj):
            if isinstance(obj, tuple):
                return (bytes(obj[0]), self.value_decode(obj[1]))
            else:
                return self.value_decode(obj)

        if 'include_value' in kwargs and kwargs['include_value'] is False:
            yield from map(bytes, self.RangeIter(*args, **kwargs))
        else:
            yield from map(transform, self.RangeIter(*args, **kwargs))

    def keys(self, *args, **kwargs):
        kwargs['include_value'] = False
        yield from self.items(*args, **kwargs)

    def values(self, *args, **kwargs):
        kwargs['include_value'] = True
        for k, v in self.items(*args, **kwargs):
            yield v

    def __contains__(self, key):
        key = self.key_transform(key)
        stop = key + self._range_ending
        return key in self.keys(key_from=key, key_to=stop)


class LevelWriter(LevelAccessor):

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
        return BatchContext(self)
