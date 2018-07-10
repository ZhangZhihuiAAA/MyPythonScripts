"""Tools for processing text.

"""

import linecache
from random import SystemRandom


def head(filename, n, encoding='utf8'):
    """Return the first n lines of the file as a generator.

    """
    with open(filename, encoding=encoding) as f:
        for _ in range(n):
            yield next(f)


def tail(filename, n, encoding='utf8'):
    """Return the last n lines of the file as a generator.

    """
    with open(filename, encoding=encoding) as f:
        total_lines = len(f.readlines())
    for i in range(total_lines - n + 1, total_lines + 1):
        yield linecache.getline(filename, i)


def generate_password(length, valid_chars=None):
    """Generate a strong random password using SystemRandom class.

    """
    sr = SystemRandom()
    if valid_chars == None:
        valid_chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        valid_chars += valid_chars.lower() + '0123456789'

    password = ''
    counter = 0
    while counter < length:
        rnum = sr.randint(0, 128)
        char = chr(rnum)
        if char in valid_chars:
            password += chr(rnum)
            counter += 1
    return password
