# Directed Acyclic Workflow Graph Scheduling

```python
import time

from dawgz import job

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

@job(name='C')
def c():
    print('c')
    time.sleep(1)
    print('c')

c.after(a, 'any')
c.after(b)

c()  # prints a b b a c c
```
