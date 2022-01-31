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

## Train example

In [`train.py`](train.py) we define a workflow that alternates between training and evaluation steps. The training steps are consecutive, meaning that the `i`th is always executed after the `i-1`th and before the `i+1`th. However, the evaluation steps can be executed directly after their respective training step, even though preceding
evaluation steps have not completed yet. The workflow graph looks like

```
preprocessing → train_1 → train_2 → train_3
                   ↓         ↓         ↓
                 eval_1    eval_2    eval_3
```

and scheduling the dependency graph results in the following output

```
data preprocessing
training step 1
evaluation step 1
training step 2
evaluation step 2
training step 3
evaluation step 3
```

However, if we change the backend to `'dummy'`, we observe that the evaluation steps are not necessarily consecutive, as expected.

```
START preprocessing
END   preprocessing
START train_1
END   train_1
START eval_1
START train_2
END   eval_1
END   train_2
START eval_2
START train_3
END   train_3
START eval_3
END   eval_3
END   eval_2
```
