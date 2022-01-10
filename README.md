# Directed Acyclic Workflow Graph Scheduling

```python
import time

from dawgz import job, after, waitfor, ensure, schedule

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

finished = [True] * 100
finished[42] = False

@after(a, b)
@waitfor('any')
@ensure(lambda i: finished[i])
@job(name='C', array=100)
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

schedule(d, backend=None, pruning=True)  # prints a b b c42 a d d
```
