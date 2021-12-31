# Directed Acyclic Workflow Graph Scheduling

```python
import time

from dawgz import job, after, waitfor, scheduler

@job(name='A')
def a():
    print('a')
    time.sleep(3)
    print('a')

@job(name='B')
def b():
    time.sleep(1)
    print('b')
    time.sleep(1)
    print('b')
    raise Exception()

@after(a, status='success')
@after(b, status='any')
@waitfor('all')
@job(name='C')
def c():
    print('c')
    time.sleep(1)
    print('c')

scheduler(backend=None)(c)  # prints a b b a c c
```
