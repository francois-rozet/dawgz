## Simple example

In [`simple.py`](simple.py) we define a workflow composed of 4 jobs (`a`, `b`, `c` and `d`). In summary,

* `a` and `b` are concurrent;
* `b` requires `2 + 2` to be equal to `2 * 2`. If it is not the case, its execution will fail before completion;
* `b` ensures that `1 + 2 + 3` is equal to `1 * 2 * 3`. If it is not the case, its execution will fail after completion;
* `c` waits for `a` to complete with `'any'` status. The status can be `'success'` (default), `'failure'` or `'any'`;
* `c` requires `finished` to be a list;
* `c` requires `finished` to contain each index `i`;
* `c` ensures that `finished[i] == True` after successful completion;
* `d` waits for `'any'` of its dependencies (`b` and `c`) to succeed. By default, jobs wait for `'all'` of their dependencies to complete.

In `schedule`, the dependency graph of `b` and `d` is pruned with respect to the postconditions. The job `c` has a postcondition for which only `i = 42` returns `False`. Therefore, all other indices are pruned out.

Then, the jobs in the workflow graph are submitted, which results in the following output

```console
b
a
a
b
d
c42
```
