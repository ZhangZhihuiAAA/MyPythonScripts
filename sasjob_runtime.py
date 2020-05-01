"""Calculate runtime of a sas job from its logs."""

import linecache
import sys
from datetime import datetime
from datetime import timedelta
from os import listdir
from os import sep
from re import compile


TIMESTAMP_RE = compile('Timestamp\s+([0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:'
                       '[0-9]{2}:[0-9]{2} [AP]M)')
DATETIME_FORMAT = '%m/%d/%Y %I:%M:%S %p'


class PatternNotFoundError(Exception):
    def __init__(self, file):
        super().__init__('Pattern not found in file {}!'.format(file))
        self.file = file


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


def get_starttime(log_file, n):
    """Search the first n lines for the start time and return it as a string.

    """

    for line in head(log_file, n):
        match = TIMESTAMP_RE.search(line)
        if match is not None:
            return match.groups()[0]


def get_finishtime(log_file, n):
    """Search the last n lines for the finish time and return it as a string.

    """

    for line in reversed(list(tail(log_file, n))):
        match = TIMESTAMP_RE.search(line)
        if match is not None:
            return match.groups()[0]


def get_runtime(starttime, finishtime, timeformat):
    """Return a timedelta object.

    """
    return datetime.strptime(finishtime, timeformat) - \
           datetime.strptime(starttime, timeformat)


if __name__ == '__main__':
    job_name = 'AGP'
    logs_dir = 'C:' + sep + 'ZZH'
    subdirs = ['r1', 'r2', 'r3', 'r4', 'r5', 'r6', 'r7', 'r8']
    head_lines = 50
    tail_lines = 20

    job_runtime = {}
    for subdir in subdirs:
        n = 0
        total_runtime = timedelta(0)
        min_runtime = timedelta(0)
        max_runtime = timedelta(0)
        runtimes = []
        for file in listdir(logs_dir + sep + subdir):
            if file.endswith('.log'):
                file_abspath = logs_dir + sep + subdir + sep + file
                try:
                    s_time = get_starttime(file_abspath, head_lines)
                    f_time = get_finishtime(file_abspath, tail_lines)
                    if s_time is None or f_time is None:
                        raise PatternNotFoundError(file_abspath)
                    runtime = get_runtime(s_time, f_time, DATETIME_FORMAT)
                    min_runtime = runtime if n == 0 \
                                  else min(min_runtime, runtime)
                    max_runtime = max(max_runtime, runtime)
                    n += 1
                    total_runtime += runtime
                    runtimes.append(str(runtime) +
                                    ' (calculated from {})'.format(file))
                except PatternNotFoundError as e:
                    print(e)
        min_runtime = min_runtime if min_runtime != timedelta(0) else None
        max_runtime = max_runtime if max_runtime != timedelta(0) else None
        avg_runtime = str(total_runtime / n).split(sep='.')[0] if n != 0 \
                      else None
        job_runtime[subdir] = (str(min_runtime),
                               str(max_runtime),
                               str(avg_runtime),
                               sorted(runtimes))

    output = sys.stdout
    sys.stdout = open(logs_dir + sep + job_name + '_runtime.txt', 'w')
    for subdir in job_runtime:
        print('--------------------------------------------------')
        print(subdir + ':')
        print('    ' + 'Minimum Runtime: ' + job_runtime[subdir][0])
        print('    ' + 'Maximum Runtime: ' + job_runtime[subdir][1])
        print('    ' + 'Average Runtime: ' + job_runtime[subdir][2])
        print('    ' + 'Runtime List: ')
        for rt in job_runtime[subdir][3]:
            print('        ' + rt)
    print('--------------------------------------------------')
    sys.stdout = output
