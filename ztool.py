"""Just tools.

"""

def print_mro(cls):
    print(', '.join(c.__name__ for c in cls.__mro__))