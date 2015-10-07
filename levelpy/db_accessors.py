#
# levelpy/db_accessors.py
#

from .batch_context import BatchContext


class LevelAccessor:
    """
    A simple class with a method for transforming strings or bytes into keys,
    and a _key_prefix member which will be prepended to the outgoing key bytes.
    """

    _key_prefix = b''

    def set_key_prefix(self, prefix):
        """
        Sets the prefix of the accessor to given value (transformed to bytes)
        """
        self._key_prefix = bytes(prefix, 'utf8')      \
                           if isinstance(prefix, str) \
                           else bytes(prefix)

    def key_transform(self, key):  # -> bytes
        """
        Takes some potential key and returns the bytes that that this accessor
        can use to retrieve values from the database.
        """
        if isinstance(key, str):
            key = key.encode()
        else:
            key = bytes(key)
        return self._key_prefix + key


class LevelReader(LevelAccessor):
    """
    Class containing methods for reading from an established database.
    """

    _range_ending = b'~'

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

            start = bytes(key.start, self.str_encoding) \
                    if isinstance(key.start, str)       \
                    else key.start

            stop = bytes(key.stop, self.str_encoding) \
                   if isinstance(key.stop, str)       \
                   else key.stop

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
