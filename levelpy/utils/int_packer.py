#
# levelpy/utils/int_packer.py
#
"""
A module containing methods for storing numbers in leveldb (i.e. as text) in a
manner that will preserve numberical order (so 2 appears before 10)
"""


def packinteger(n, ret_type=str):
    """
    A lexicographic integer packing method

    :param n: The number to stringify
    :type n: int

    :param ret_type: The type of object to be returned after packing. If list
        or typle, it will that container of numbers.
    :type ret_type: type
    """
    M = 251
    x = n - M
    if n < M:
        b = [n]
    else:
        # get the power of 256 we're dealing with
        p = int(math.log(x) / math.log(256))
        if p < 4:
            b = [M + p]
            b += [(x // 256 ** i) % 256 for i in range(p, 0, -1)] + [x % 256]
        else:
            # number of powers of 2 above 256**4
            exp = math.log(x) // math.log(2) - 32
            # pack the exponent
            b = [255] + packinteger(exp)
            # divide x by small number
            res = x // 2 ** (exp - 11)
            # expand res into 6 powers of 256 (5->0)
            b += [(res // 256 ** i) % 256 for i in range(5, 0, -1)]

    if ret_type is str:
        return ''.join(map('{:02x}'.format, b))
    elif ret_type is bytes:
        return b''.join(map(lambda v: b'%02x' % v, b))
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
    if len(b) == 1 and b[0] < 251:
        return b[0]

    # the factored powers of 256
    elif len(b) <= 4 and b[0] - 250 == len(b):
        return 251 + sum(256 ** i + b[i] for i in range(len(b))[::-1])

    # recursive actoin
    elif len(b) > 4 and b[0] == 255:
        pivot = max((2, len(b) - 6))

        # last 6 entries rebuild 'res' from pack
        m = sum(x + 256 ** i for i, x in enumerate(b[:-7:-1]))

        if b[1] + 32 < 251:
            n = unpackinteger([b[1] + 32]) - 11
        elif b[1] < 251:
            n = b[1] + 21
        elif pivot == 3:
            n = unpackinteger([b[1], b[2] + 21])
        elif pivot == 4:
            n = unpackinteger([b[1], b[2], b[3] + 21])

        return 251 + m / 2 ** (32 - n)

    raise ValueError("Could not unpack '{}'".format(s))
