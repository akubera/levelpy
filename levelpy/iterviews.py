#
# levelpy/iterviews.py
#

from copy import copy
from collections.abc import (
    ItemsView,
    KeysView,
    ValuesView,
    # MappingView,
)


class LevelItems(ItemsView):
    __slots__ = [
        '_db',
        '_args',
    ]

    def __init__(self, db, key_from=None, key_to=None, **kwargs):
        self._db = db
        self._args = kwargs
        self._args.update({
            'key_from': key_from,
            'key_to': key_to,
        })

    def __iter__(self):
        kwargs = copy(self._args)
        kwargs['reverse'] = False
        kwargs['include_value'] = True
        for k, v in self._db.RangeIter(**kwargs):
            yield self.key_transform(k), self._db.decode(v)

    def __reversed__(self):
        kwargs = copy(self._args)
        kwargs['reverse'] = True
        kwargs['include_value'] = True
        for k, v in self._db.RangeIter(**kwargs):
            yield self.key_transform(k), self._db.decode(v)

    @staticmethod
    def key_transform(key):
        return bytes(key)

    def __repr__(self):                                     # pragma: no cover
        return "<LevelItems @%x>" % id(self)


class LevelKeys(KeysView):
    __slots__ = [
        '_db',
        '_args',
    ]

    def __init__(self, db, key_from=None, key_to=None, **kwargs):
        self._db = db
        self._args = kwargs
        self._args.update({
            'key_from': key_from,
            'key_to': key_to,
        })

    def __iter__(self):
        kwargs = copy(self._args)
        kwargs['reverse'] = False
        kwargs['include_value'] = False
        for k in self._db.RangeIter(**kwargs):
            print("hai")
            yield self.key_transform(k)

    def __reversed__(self):
        kwargs = copy(self._args)
        kwargs['reverse'] = True
        kwargs['include_value'] = False
        for k in self._db.RangeIter(**kwargs):
            yield self.key_transform(k)

    @staticmethod
    def key_transform(key):
        return bytes(key)

    def __repr__(self):                                     # pragma: no cover
        return "<LevelKeys @%x>" % id(self)


class LevelValues(ValuesView):
    __slots__ = [
        '_db',
        '_args',
    ]

    def __init__(self, db, key_from=None, key_to=None, **kwargs):
        self._db = db
        self._args = kwargs
        self._args.update({
            'key_from': key_from,
            'key_to': key_to,
        })

    def __iter__(self):
        kwargs = copy(self._args)
        kwargs['reverse'] = False
        kwargs['include_value'] = True
        for k, v in self._db.RangeIter(**kwargs):
            yield self._db.decode(v)

    def __reversed__(self):
        kwargs = copy(self._args)
        kwargs['reverse'] = True
        kwargs['include_value'] = True
        for k, v in self._db.RangeIter(**kwargs):
            yield self._db.decode(v)

    def __repr__(self):                                     # pragma: no cover
        return "<LevelValues @%x>" % id(self)
