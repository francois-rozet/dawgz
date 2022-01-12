# Examples

## Simple example

In [`simple.py`](simple.py) we define a workflow composed of 4 jobs (`a`, `b`, `c` and `d`). In summary,

* `a` and `b` are concurrent;
* `b` requires `1 + 1` to be equal to `2`. Not satisfying all *preconditions* prior to completion results in an `AssertionError`;
* `c` waits for `a` to complete with success. The status can be `'success'` (default), `'failure'` or `'any'`;
* `c` ensures that `2 + 2 == 2 * 2` and `1 + 2 + 3 == 1 * 2 * 3`. Not satisfying all *postconditions* after completion results in an `AssertionError`;
* `d` waits for `a` to complete with `'any'` status;
* `d` requires `finished` to be a list;
* `d` requires `finished` to contain each index `i`;
* `d` ensures that `finished[i] == True` after successful completion.
* `e` waits for `'any'` of its dependencies (`b` and `d`) to complete with success. By default, jobs wait for `'all'` of their dependencies to complete.

In `schedule`, the dependency graph of `c` and `e` is pruned with respect to the postconditions.

* `c`'s postconditions are both always `True`, resulting in `d` being pruned out from the graph, eventhough the dependency `a` of `d` never succeeds.
* `d`'s postcondition returns `False` for `i = 42`. Therefore, all other indices are pruned out.


Then, the jobs in the workflow graph are submitted, which results in the following output

```console
b
a
a
b
e
d42
```
