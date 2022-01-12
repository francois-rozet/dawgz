# Examples

## Simple example

In [`simple.py`](simple.py) we define a workflow composed of 4 jobs (`a`, `b`, `c` and `d`). In summary,

* `a` and `b` are concurrent;
* `b` requires `1 + 1` to be equal to `2`. Not satisfying a *precondition* prior to completion results in an `AssertionError`;
* `c` waits for `a` to complete with `'any'` status. The status can be `'success'` (default), `'failure'` or `'any'`;
* `c` requires `finished` to be a list;
* `c` requires `finished` to contain each index `i`;
* `c` ensures that `finished[i] == True` after successful completion. Not satisfying a *postcondition* after completion results in an `AssertionError`;
* `d` waits for `b` to complete with success;
* `d` ensures that `2 + 2 == 2 * 2` and `1 + 2 + 3 == 1 * 2 * 3`;
* `e` waits for `'any'` of its dependencies (`b` and `c`) to complete with success. By default, jobs wait for `'all'` of their dependencies to complete.

In `schedule`, the dependency graph of `d` and `e` is pruned with respect to the postconditions.

* `c`'s postcondition returns `False` for `i = 42`. Therefore, all other indices are pruned out.
* `d`'s postconditions are both always `True`, resulting in `d` being pruned out entirely.

Then, the jobs in the workflow graph are submitted, which results in the following output

```console
b
a
a
b
e
c42
```
