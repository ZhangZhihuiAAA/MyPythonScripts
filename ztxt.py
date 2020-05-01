"""
Tools for processing text.
"""

import linecache
from random import SystemRandom


def head(filename, n, encoding='utf8'):
    """Return the first n lines of the file as a generator."""
    with open(filename, encoding=encoding) as f:
        for _ in range(n):
            yield next(f)


def tail(filename, n, encoding='utf8'):
    """Return the last n lines of the file as a generator."""
    with open(filename, encoding=encoding) as f:
        total_lines = len(f.readlines())
    for i in range(total_lines - n + 1, total_lines + 1):
        yield linecache.getline(filename, i)


def generate_password(length, valid_chars=None):
    """Generate a strong random password using SystemRandom class."""
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


def replace_str_in_file_with_file(file1, str, file2):
    f1 = open(file1, 'r')
    f2 = open(file2, 'r')
    str_f1 = f1.read()
    str_f2 = f2.read()
    str_f1 = str_f1.replace(str, str_f2)
    f1.close()
    f2.close()
    f1 = open(file1, 'w')
    f1.write(str_f1)
    f1.close()
