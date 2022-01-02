# Directed Acyclic Workflow Graph Scheduling

```python
import time

from dawgz import job, after, waitfor, ensure, scheduler

@job(name='A')
def a():
    print('a')
    time.sleep(3)
    print('a')
    raise Exception()

@job(name='B')
def b():
    time.sleep(1)
    print('b')
    time.sleep(1)
    print('b')

finished = [True] * 1000
finished[420] = False

@ensure(lambda i: finished[i])  # postcondition
@after(a, b)
@waitfor('any')
@job(name='C', array=int(1e3))
def c(i: int):
    print(f'c{i}')
    finished[i] = True

@after(a, status='any')
@after(b, c, status='success')
@waitfor('all')
@job(name='D')
def d():
    print('d')
    time.sleep(1)
    print('d')

scheduler(backend=None)(d)  # prints a b b c420 a d d
```
