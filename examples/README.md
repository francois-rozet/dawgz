# Examples

## Simple example

In [`simple.py`](simple.py) we define a workflow composed of 5 jobs. In summary,

* `a` and `b` are concurrent.
* `c(i)` waits for `b` to complete with success.
* `c(i)` has already completed with success for `i != 42`.
* `d` waits for all `c(i)` to complete.
* `e` waits for `'any'` of its dependencies (either `a` or `d`) to complete.

The workflow graph of `e` is scheduled and submitted by `dawgz.schedule`, while pruning the jobs that have completed. We get the following ouput, where we notice that `e` finishes before `a`, despite its failure, reported in the error table.

```
$ python examples/simple.py
a
b
b
c42
d
e
a
Traceback (most recent call last):
  File ".../dawgz/examples/simple.py", line 13, in a
    raise RuntimeError("foo")
RuntimeError: foo
╭────┬─────┬───────────────────────────────────────────────────────────────────────────────────╮
│    │ Job │ Error                                                                             │
├────┼─────┼───────────────────────────────────────────────────────────────────────────────────┤
│  0 │ a   │ Traceback (most recent call last):                                                │
│    │     │   File ".../dawgz/dawgz/utils.py", line 157, in runpickle                         │
│    │     │     pickle.loads(data)(*args, **kwargs)                                           │
│    │     │     ~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^                                           │
│    │     │   File ".../dawgz/examples/simple.py", line 13, in a                              │
│    │     │     raise RuntimeError("foo")                                                     │
│    │     │ RuntimeError: foo                                                                 │
│    │     │                                                                                   │
│    │     │ The above exception was the direct cause of the following exception:              │
│    │     │                                                                                   │
│    │     │ Traceback (most recent call last):                                                │
│    │     │   File ".../dawgz/dawgz/schedulers.py", line 335, in exec                         │
│    │     │     await loop.run_in_executor(                                                   │
│    │     │     ...<2 lines>...                                                               │
│    │     │     )                                                                             │
│    │     │ RuntimeError: foo                                                                 │
│    │     │                                                                                   │
│    │     │ The above exception was the direct cause of the following exception:              │
│    │     │                                                                                   │
│    │     │ Traceback (most recent call last):                                                │
│    │     │   File ".../dawgz/schedulers.py", line 340, in exec                               │
│    │     │     raise JobFailedError(repr(job)) from e                                        │
│    │     │ dawgz.schedulers.JobFailedError: a()                                              │
╰────┴─────┴───────────────────────────────────────────────────────────────────────────────────╯
```

## Train example

In [`train.py`](train.py) we define a workflow that alternates between training and evaluation steps. The training steps are consecutive, meaning that the `i`th is always executed after the `i-1`th and before the `i+1`th. However, the evaluation steps can be executed directly after their respective training step, even though preceding evaluation steps have not completed yet. The workflow graph looks like

```
preprocessing → train_1 → train_2 → train_3
                   ↓         ↓         ↓
                 eval_1    eval_2    eval_3
```

and scheduling the dependency graph results in the following output

```
$ python examples/train.py
data preprocessing
training step 1
evaluation step 1
training step 2
evaluation step 2
training step 3
evaluation step 3
```

If we change the backend to `'dummy'`, we observe that the evaluation steps are not necessarily consecutive.

```
$ python examples/train.py
START preprocessing()
END   preprocessing()
START train(1)
END   train(1)
START evaluate(1)
START train(2)
END   train(2)
START evaluate(2)
START train(3)
END   evaluate(1)
END   train(3)
START evaluate(3)
END   evaluate(2)
END   evaluate(3)
```
