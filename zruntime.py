"""Get runtime of a snippet of python code."""

from time import perf_counter as pc

t0 = pc();

for i in range(100):
    if i % 10 == 0:
        assert i < 100

t1 = pc();

runtime = t1 - t0;
