#!usr/bin/env python

import time

from dawgz import job, after, waitfor, require, ensure, schedule

@job
def a():
    print('a')
    time.sleep(3)
    print('a')
    raise Exception()

@require(lambda: 1 + 1 == 2)
@job
def b():
    time.sleep(1)
    print('b')
    time.sleep(1)
    print('b')

@after(a, status='success')
@ensure(lambda: 2 + 2 == 2 * 2)
@ensure(lambda: 1 + 2 + 3 == 1 * 2 * 3)
@job
def c():
    print('c')

finished = [True] * 100
finished[42] = False

@after(a, status='any')
@require(lambda: type(finished) is list)
@require(lambda i: i < len(finished))
@ensure(lambda i: finished[i])
@job(array=100)
def d(i: int):
    print(f'd{i}')
    finished[i] = True

@after(b, d)
@waitfor('any')
@job
def e():
    print('e')

schedule(c, e, backend='local', prune=True)