# Examples

## Simple example

In [`simple.py`](simple.py) we define a workflow composed of 5 jobs. In summary,

* `a` and `b` are concurrent.
* `c` waits for `a` to complete with success.
* `c` ensures that `2 + 2 == 2 * 2` and `1 + 2 + 3 == 1 * 2 * 3`.
* `d` waits for `b` to complete with `'success'`.
* `d` ensures that `finished[i] == True` after successful completion.
* `e` waits for `'any'` of its dependencies (either `a` or `d`) to complete.

In `schedule`, the dependency graph of `c` and `e` is pruned with respect to the postconditions.

* `c`'s postconditions are both always `True`, resulting in `c` being pruned out from the graph, even though its dependency `a` fails.
* `d`'s postcondition returns `False` for `i = 42`. Therefore, all other indices are pruned out.

Then, the jobs in the workflow graph are submitted, which results in the following output

```
a
b
b
d42
e
a
```

as well as a warning about the failure of `a`.

```
DAWGZWarning: errors occurred while scheduling
-------------------------------------------------------------------------
Traceback (most recent call last):
  File "path/to/dawgz/schedulers.py", line 159, in exec
    return await to_thread(job.fn)
  File "path/to/dawgz/utils.py", line 99, in to_thread
    return await loop.run_in_executor(None, func_call)
  File "/usr/lib/python3.8/concurrent/futures/thread.py", line 57, in run
    result = self.fn(*self.args, **self.kwargs)
  File "path/to/dawgz/workflow.py", line 99, in call
    result = f(*args)
  File "simple.py", line 12, in a
    raise Exception()
Exception

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "path/to/dawgz/schedulers.py", line 73, in _submit
    return await self.exec(job)
  File "path/to/dawgz/schedulers.py", line 163, in exec
    raise JobFailedError(f'{job}') from e
dawgz.schedulers.JobFailedError: a
-------------------------------------------------------------------------
```
