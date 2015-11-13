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

    def __init__(self, db, key_from=None, key_to=None, verify_checksums=False):
        self._db = db
        self._args = {
            'key_from': key_from,
            'key_to': key_to,
            'verify_checksums': verify_checksums,
        }

    def __iter__(self):
        kwargs = copy(self._args)
        kwargs['reverse'] = False
        kwargs['include_value'] = True
        return self._db.RangeIter(**kwargs)

    def __reversed__(self, *args, **kwargs):
        kwargs = copy(self._args)
        kwargs['reverse'] = True
        kwargs['include_value'] = True
        return self._db.RangeIter(**kwargs)


class LevelKeys(KeysView):
    __slots__ = [
        '_db',
        '_args',
    ]

    def __init__(self, db, key_from=None, key_to=None, verify_checksums=False):
        self._db = db
        self._args = {
            'key_from': key_from,
            'key_to': key_to,
            'verify_checksums': verify_checksums,
        }

    def __iter__(self):
        kwargs = copy(self._args)
        kwargs['reverse'] = False
        kwargs['include_value'] = False
        return self._db.RangeIter(**kwargs)

    def __reversed__(self, *args, **kwargs):
        kwargs = copy(self._args)
        kwargs['reverse'] = True
        kwargs['include_value'] = False
        return self._db.RangeIter(**kwargs)


class LevelValues(ValuesView):
    __slots__ = [
        '_db',
        '_args',
    ]

    def __init__(self, db, key_from=None, key_to=None, verify_checksums=False):
        self._db = db
        self._args = {
            'key_from': key_from,
            'key_to': key_to,
            'verify_checksums': verify_checksums,
        }

    def __iter__(self):
        kwargs = copy(self._args)
        kwargs['reverse'] = False
        kwargs['include_value'] = True
        for k, v in self._db.RangeIter(**kwargs):
            yield v

    def __reversed__(self, *args, **kwargs):
        kwargs = copy(self._args)
        kwargs['reverse'] = True
        kwargs['include_value'] = True
        for k, v in self._db.RangeIter(**kwargs):
            yield v

    def __repr__(self):
        return "<LevelValues>"
