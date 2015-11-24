#
# levelpy/utils/int_packer.py
#
"""
A module containing methods for storing numbers in leveldb (i.e. as text) in a
manner that will preserve numberical order (so 2 appears before 10)
"""

import math


def packinteger(n, ret_type=str):
    """
    A lexicographic integer packing method

    :param n: The number to stringify
    :type n: int

    :param ret_type: The type of object to be returned after packing. If list
        or typle, it will that container of numbers.
    :type ret_type: type
    """
    n = int(n)
    M = 251
    x = n - M
    if n < M:
        b = [n]
    elif x == 0:
        b = [251, 0]
    else:
        # get the power of 256 we're dealing with
        p = int(math.log(x, 256))
        if p < 4:
            b = [M + p]
            b += [(x // 256 ** i) % 256 for i in range(p, 0, -1)] + [x % 256]
        else:
            # number of powers of 2 above 256**4
            exp = int(math.log(x, 2)) - 32
            # pack the exponent
            b = [255] + packinteger(exp, list)
            # divide x by small number
            res = int(x // 2 ** (exp - 11))
            # expand res into 6 powers of 256 (5->0)
            b += [(res // 256 ** i) % 256 for i in range(5, -1, -1)]

    if ret_type is str:
        return ''.join(map('{:02x}'.format, b))
    elif ret_type is bytes:
        try:
            return b''.join(map(lambda v: b'%02x' % v, b))
        except TypeError:
            return ''.join(map('{:02x}'.format, b)).encode()
    elif ret_type is list or ret_type is tuple:
        return ret_type(b)
    else:
        raise ValueError("Unsupported return type : %s" % ret_type)


def unpackinteger(s):
    """
    The lexicographic integer unpacking

    :param s: The encoding bytes/string to decode to an integer
    :type s: str/bytes
    """
    if isinstance(s, (str, bytes)):
        b = [int(s[i:i + 2], 16) for i in range(0, len(s), 2)]
    else:
        b = s

    # trivial case
    if len(b) == 1 and b[0] <= 251:
        return b[0]

    # the factored powers of 256
    elif len(b) <= 5 and b[0] - 249 == len(b):
        return 251 + sum(256 ** i * v for i, v in enumerate(b[-1:0:-1]))

    # recursive action
    elif len(b) > 5 and b[0] == 255:

        # last 6 entries rebuild 'res' from pack
        res = sum(x * 256 ** i for i, x in enumerate(b[-1:-7:-1]))

        # power of 2 exponent should be between first and last 6 elements
        p_exp = b[1:-6]

        # unpack the exponent
        if p_exp[0] + 32 < 251:
            exp = unpackinteger([p_exp[0] + 32]) - 11
        elif p_exp[0] < 251:
            exp = p_exp[0] + 21
        else:
            p_exp[-1] += 21
            exp = unpackinteger(p_exp)

        return 251 + res / (2 ** (32 - exp))

    raise ValueError("Could not unpack '{}'".format(s))
