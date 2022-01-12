```python
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

schedule(d, backend='local', prune=True)
```

In summary,

* `a` and `b` are concurrent;
* `c` waits for `a` to complete with `'any'` status. The status can be `'success'` (default), `'failure'` or `'any'`;
* `c` requires `finished` to be a list;
* `c` requires `finished` to contain each index `i`;
* `c` ensures that `finished[i] == True` after successful completion;
* `d` waits for `'any'` of its dependencies (`b` and `c`) to succeed. By default, jobs wait for `'all'` of their dependencies to complete.

In `schedule`, the dependency graph of `d` is pruned with respect to the postconditions. The job `c` has a postcondition for which only `i = 42` is `False`. Therefore, all other indices are pruned out.

Then, the jobs in the workflow graph are submitted, which results in the following output

```console
b
a
a
b
d
c42
```
