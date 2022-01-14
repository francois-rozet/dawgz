# Directed Acyclic Workflow Graph Scheduling

Would you like fully reproducible and reusable workflows that run on HPC clusters as seamlessly as on your machine? Tired of writing and managing large Slurm submission scripts? Do you have to comment out large parts of your pipeline whenever something failed? Hate YAML and config files? Then `dawgz` is made for you!

The `dawgz` package allows you to define and execute complex workflows, directly in Python. It provides a lightweight interface that enables to define jobs along with their dependencies, requirements, postconditions, etc. After defining the workflow, you can schedule target jobs in a single line of code and all their dependencies will be automatically scheduled as well. Importantly, the scheduling backend can be changed with a single parameter, which enables to execute the same workflow on your personal computer and HPC clusters.

> `dawgz` is the successor of [`awflow`](https://github.com/JoeriHermans/awflow)

## Installation

The `dawgz` package is available on [PyPi](https://pypi.org/project/dawgz/), which means it is installable via `pip`.

```
$ pip install dawgz
```

Alternatively, if you need the latest features, you can install it using

```
$ pip install git+https://github.com/francois-rozet/dawgz
```

## Getting started

In `dawgz`, a job is a Python function decorated by the `@dawgz.job` decorator. This decorator allows to define the job's parameters, like its name, whether it is a job array, the resources it needs, etc. Importantly, a job can have other jobs as dependencies, which implicitely defines a workflow graph. The `@dawgz.after` decorator is used to declare such dependencies. Additionally, to ensure that the job completed successfuly, [postconditions](https://en.wikipedia.org/wiki/Postcondition) can be added with the `@dawgz.ensure` decorator.

Finally, `dawgz` provides the `dawgz.schedule` function in order to schedule target jobs with a selected backend. This function automatically takes care of scheduling the dependency graph of the target jobs.

```python
import glob
import numpy as np
import os

from dawgz import job, after, ensure, schedule

samples = 10000
tasks = 5

@ensure(lambda i: os.path.exists(f'pi_{i}.npy'))
@job(array=tasks, cpus=1, ram='2GB', timelimit='5:00')
def generate(i: int):
    print(f'Task {i + 1} / {tasks}')

    x = np.random.random(samples)
    y = np.random.random(samples)
    within_circle = x ** 2 + y ** 2 <= 1

    np.save(f'pi_{i}.npy', within_circle)

@after(generate)
@job(cpus=2, ram='4GB', timelimit='15:00')
def estimate():
    files = glob.glob('pi_*.npy')
    stack = np.vstack([np.load(f) for f in files])
    pi_estimate = stack.mean() * 4

    print(f'π ≈ {pi_estimate}')

schedule(estimate, backend='local')
```

In the preceding example, we define two jobs: `generate` and `estimate`. The former is a *job array*, meaning that it is executed concurrently for all values of `i = 0` up to `tasks - 1`. It also defines a postcondition verifying whether a file exists after the job's completion. If it is not the case, the job raises an `AssertionError` at runtime. The job `estimate` only starts after `generate` succeeded.

Executing this script with the `'local'` backend displays

```
$ python examples/pi.py
Task 1 / 5
Task 2 / 5
Task 3 / 5
Task 4 / 5
Task 5 / 5
π ≈ 3.1418666666666666
```

Alternatively, on a Slurm HPC cluster, changing the backend to `'slurm'` results in the following job queue.

```
$ squeue -u username
             JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)
           1868832       all estimate username PD       0:00      1 (Dependency)
     1868831_[2-4]       all generate username PD       0:00      1 (Resources)
         1868831_0       all generate username  R       0:01      1 compute-xx
         1868831_1       all generate username  R       0:01      1 compute-xx
```

Check out the [examples](examples/) and the [interface](#Interface) to discover the functionalities of `dawgz`.

## Interface

### Decorators

The package provides five decorators:

* `@dawgz.job` registers a function as a job, optionally with parameters (name, array, resources, ...). It should always be the first (lowest) decorator. In the following example, `a` is a job with the name `'A'` and a time limit of one hour.

    ```python
    @job(name='A', timelimit='01:00:00')
    def a():
    ```

* `@dawgz.after` adds one or more dependencies to a job. By default, the job waits for its dependencies to complete with success. The desired status can be set to `'success'` (default), `'failure'` or `'any'`. In the following example, `b` waits for `a` to complete with `'failure'`.

    ```python
    @after(a, status='failure')
    @job
    def b():
    ```

* `@dawgz.waitfor` declares whether the job waits for `'all'` (default) or `'any'` of its dependencies to be satisfied before starting. In the following example, `c` waits for either `a` or `b` to complete (with success).

    ```python
    @after(a, b)
    @waitfor('any')
    @job
    def c():
    ```

* `@dawgz.require` adds a [precondition](https://en.wikipedia.org/wiki/Preconditions) to a job, *i.e.* a condition that must be `True` prior to the execution of the job. If preconditions are not satisfied, the job is never executed. In the following example, `d` requires `tmp` to be an existing directory.

    ```python
    @require(lambda: os.path.isdir('tmp'))
    @job
    def d():
    ```

* `@dawgz.ensure` adds a [postcondition](https://en.wikipedia.org/wiki/Postconditions) to a job, *i.e.* a condition that must be `True` after the execution of the job. In the following example, `e` ensures that the file `tmp/dump.log` exists.

    ```python
    @after(d)
    @ensure(lambda: os.path.exists('tmp/dump.log'))
    @job
    def e():
    ```

    > Traditionally, postconditions are only **necessary** indicators that the job completed with success. In `dawgz`, they are considered both necessary and **sufficient** indicators. Therefore, postconditions can be used to detect jobs that have already been executed and prune them out from the workflow graph.

### Backends

Currently, `dawgz.schedule` only supports two backends: `local` and `slurm`.

* `local` schedules locally the jobs by waiting asynchronously for dependencies to finish before submitting each job. It does not take the required resources into account.
* `slurm` submits the jobs to the Slurm workload manager by generating automatically the `sbatch` submission scripts.
