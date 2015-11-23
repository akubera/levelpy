#
# tests/test_utils.py
#

import pytest
from levelpy.utils import int_packer


@pytest.mark.parametrize("val, ex", [
    (0, '00'),
    (15, '0f'),
    (16, '10'),
    (100, '64'),
    (250, 'fa'),
    (251, 'fb00'),
    (252, 'fb01'),
    (5000, 'fc128d'),
    (12345, 'fc2f3e'),
    (21378213, 'fe014633aa'),
    (1e9, 'fe3b9ac905'),
    (68719476740, 'ff030fffffff0900')
])
def test_intpacker(val, ex):
    packed = int_packer.packinteger(val)
    assert packed == ex
    unpacked = int_packer.unpackinteger(packed)
    assert unpacked == val


@pytest.mark.parametrize("val, ex, err", [
    (32350670572674534828156, 'ff2a0db3777bb94c', 4.79e8),
    (21378213 * 1513254198219212 ** 2, 'ff5d09351659b187', 2.5e39),
    (21378213 * 1513254198219212 ** 3, 'ff8f0c6004db1f28', 2.5e39),
    (21378213 * 1513254198219212 ** 4.5, 'ffdb09a42cc72efa', 2.5e62),
    (21378213 * 1513254198219212 ** 7, 'fffb5e0a187f7c58cd', 8e99),
])
def test_lossy_intpacker(val, ex, err):
    packed = int_packer.packinteger(val)
    assert packed == ex
    unpacked = int_packer.unpackinteger(packed)
    assert abs(unpacked - val) < err

@pytest.mark.parametrize("val, ex", [
    (0, b'00'),
])
def test_intpacker_return_bytes(val, ex):
    packed = int_packer.packinteger(val, bytes)



def test_bad_intpacker():
    with pytest.raises(ValueError):
        int_packer.packinteger('python')
    with pytest.raises(ValueError):
        int_packer.unpackinteger('python')
    with pytest.raises(ValueError):
        int_packer.unpackinteger('fffa')
    with pytest.raises(ValueError):
        int_packer.packinteger(45, 'python')
