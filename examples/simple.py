import time

from dawgz import job, after, waitfor, require, ensure, schedule


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
@require(lambda: 1 >= 0)  # Check precondition at runtime
@require(lambda i: i >= 0)  # Array jobs can have preconditions based on the array
@job(array=3)
def d(i):
    print(f'd{i}')

@after(c)
@require(lambda: 1 == 1)  # Specifying an argument to a precondition that is not an array job will yield an AssertionError
@job
def e():
    print('e')

returns = schedule(d, e, backend='local')  # prints a a c42 d0 d1 d2 (e is executed concurrently by asyncio)
print(returns)
