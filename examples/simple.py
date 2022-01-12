#!usr/bin/env python

import time

from dawgz import job, after, waitfor, require, ensure, schedule

@job
def a():
    print('a')
    time.sleep(3)
    print('a')
    raise Exception()

@require(lambda: 2 + 2 == 2 * 2)
@job
def b():
    time.sleep(1)
    print('b')
    time.sleep(1)
    print('b')

finished = [True] * 100
finished[42] = False

@after(a, status='any')
@require(lambda: type(finished) is list)
@require(lambda i: i < len(finished))
@ensure(lambda i: finished[i])
@job(array=100)
def c(i: int):
    print(f'c{i}')
    finished[i] = True

@after(b, c)
@waitfor('any')
@job
def d():
    print('d')

schedule(b, d, backend='local', prune=True)
