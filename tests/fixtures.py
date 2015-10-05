#
# tests/fixtures.py
#
"""
Useful fixtures to be imported.
"""

import pytest
import tempfile
import shutil


@pytest.fixture
def leveldir(request):
    path = tempfile.mkdtemp(prefix='levelpy-test-')
    request.addfinalizer(lambda: shutil.rmtree(path))
    return path
