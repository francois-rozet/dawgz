r"""
In this example,

* `a` and `b` are concurrent;
* `c` waits for `a` to complete with any status;
* `c` requires `finished` to be a list;
* `c` also requires that `finished` contains all index `i`;
* `c` ensures that `finished[i] == True` after completion;
* `d` waits for any of its dependencies (`b` and `c`) to finish.

In `schedule`, the dependency graph of `d` is pruned with respect to the postconditions.
Here, `c` has a postcondition for which only `i = 42` is `False`. Therefore, all other indices are pruned.
Then, the jobs in the dependency graph are submitted.
"""

import time

from dawgz import job, after, waitfor, require, ensure, schedule

@job
def a():
    print('a')
    time.sleep(3)
    print('a')
    raise Exception()

@job
def b():
    time.sleep(1)
    print('b')
    time.sleep(1)
    print('b')

finished = [True] * 100
finished[42] = False

@after(a, status='any')  # (default status is 'success')
@require(lambda: type(finished) is list)
@require(lambda i: i < len(finished))
@ensure(lambda i: finished[i])
@job(array=100)
def c(i: int):
    print(f'c{i}')
    finished[i] = True

@after(b, c)
@waitfor('any')  # (default is 'all')
@job
def d():
    print('d')

schedule(d, backend='local', prune=True)  # prints b a a b d c42
