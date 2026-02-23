# Examples

## Simple example

In [`simple.py`](simple.py) we define a workflow composed of 5 jobs. In summary,

* `a` and `b` are concurrent.
* `c(i)` waits for `b` to complete with success.
* `c(i)` has already completed with success for `i != 42`.
* `d` waits for all `c(i)` to complete.
* `e` waits for `'any'` of its dependencies (either `a` or `d`) to complete.

The workflow graph of `e` is scheduled and submitted by `schedule`, while pruning the jobs that have completed. We get the following ouput, where we notice that `e` finishes before `a` and despite its failure, reported in the error table.

```
a
b
b
c42
d
e
a
    Job    Error
--  -----  -----------------------------------------------------------------------------------------------------------
 0  a()    Traceback (most recent call last):
             File "/home/francois/.venvs/dawgz/lib/python3.13/site-packages/dawgz/schedulers.py", line 257, in exec
               return await asyncio.get_running_loop().run_in_executor(self.executor, runpickle, dump)
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
             File "/home/francois/.venvs/dawgz/lib/python3.13/site-packages/dawgz/utils.py", line 69, in runpickle
               return pickle.loads(f)(*args, **kwargs)
                      ~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^
             File "/home/francois/Documents/Git/dawgz/examples/simple.py", line 12, in a
               raise Exception()
           Exception

           The above exception was the direct cause of the following exception:

           Traceback (most recent call last):
             File "/home/francois/.venvs/dawgz/lib/python3.13/site-packages/dawgz/schedulers.py", line 180, in _submit
               return await self.exec(job)
                      ^^^^^^^^^^^^^^^^^^^^
             File "/home/francois/.venvs/dawgz/lib/python3.13/site-packages/dawgz/schedulers.py", line 259, in exec
               raise JobFailedError(str(job)) from e
           dawgz.schedulers.JobFailedError: a()
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
