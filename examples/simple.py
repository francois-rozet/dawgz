import time

from dawgz import job, after, waitfor, ensure, schedule, leafs


@job
def a():
    print('a')
    time.sleep(3)
    print('a')
    raise Exception()

def check_something():
    return True

@ensure(check_something)
@job
def b():
    time.sleep(1)
    print('b')
    time.sleep(1)
    print('b')

finished = [True] * 100
finished[42] = False

@after(a, status='any')
@after(b)
@ensure(lambda i: finished[i])
@job(array=100)
def c(i: int):
    print(f'c{i}')
    finished[i] = True

@after(b, c)
@ensure(lambda: all(finished), when='before')  # Check precondition at runtime
@job
def d():
    print('d')
    time.sleep(1)
    print('d')

@after(c)
@job
def e():
    print('e')

jobs = leafs(a)  # Search for terminal nodes of `a` and prune automatically
print(jobs)  # prints {d, e}

schedule(*jobs, backend='local')  # prints a a c42 d e d (e is executed concurrently by asyncio)
