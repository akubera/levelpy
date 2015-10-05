#
# tests/test_with_plyvel.py
#
# pylint: disable=F811
#
"""
Tests for leveldb with plyvel as a backend.
This suite is skipped if plyvel is not installed.
"""

import pytest
from levelpy.leveldb import LevelDB

from fixtures import leveldir                                            # noqa


@pytest.fixture(scope='module')
def backend_class_str():
    return "plyvel.DB"


@pytest.fixture(scope='module')
def backend_package(backend_class_str):
    pkg_name = ".".join(backend_class_str.split(".")[:-1])
    return pytest.importorskip(pkg_name)


@pytest.fixture(scope='module')
def backend_class(backend_package, backend_class_str):
    classname = backend_class_str.split(".")[-1]
    return getattr(backend_package, classname)


@pytest.fixture
def backend(backend_class, leveldir):
    return backend_class(leveldir, create_if_missing=True)


def test_class(backend_class):
    assert backend_class is not None


def test_constructor(leveldir, backend_class_str, backend_class):
    lvl = LevelDB(leveldir, backend_class_str, create_if_missing=True)
    assert isinstance(lvl, LevelDB)
    assert isinstance(lvl._db, backend_class)


def test_constructor(leveldir, backend_class_str, backend_class):
    lvl = LevelDB(leveldir, backend_class_str, create_if_missing=True)
    assert isinstance(lvl, LevelDB)
    assert isinstance(lvl._db, backend_class)


def test_constructor_with_premade_backend(backend):
    lvl = LevelDB(backend)
    assert lvl.Put == backend.put
    assert lvl.Get == backend.get
    assert lvl.Delete == backend.delete
    assert lvl.Write is None

    # this needs to be figured out
    assert lvl.path is None
    # assert lvl.RangeIter == backend.range


def test_backend_package(backend_package):
    assert backend_package is not None
