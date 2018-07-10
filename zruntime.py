"""Ways of calculating runtime.

"""

from time import perf_counter as pc

t0 = pc();

for i in range(100):
    if i % 10 == 0:
        print(i)

t1 = pc();

runtime = t1 - t0;

print(runtime)