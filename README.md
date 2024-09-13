# Directed Acyclic Workflow Graph Scheduling

Would you like fully reproducible and reusable experiments that run on HPC clusters as seamlessly as on your machine? Do you have to comment out large parts of your pipelines whenever something failed? Tired of writing and submitting [Slurm](https://wikipedia.org/wiki/Slurm_Workload_Manager) scripts? Then `dawgz` is made for you!

The `dawgz` package provides a lightweight and intuitive Python interface to declare jobs along with their dependencies, requirements, settings, etc. A single line of code is then needed to execute automatically all or part of the workflow, while complying to the dependencies. Importantly, `dawgz` can also hand over the execution to resource management backends like [Slurm](https://wikipedia.org/wiki/Slurm_Workload_Manager), which enables to execute the same workflow on your machine and HPC clusters.

## Installation

The `dawgz` package is available on [PyPi](https://pypi.org/project/dawgz/), which means it is installable via `pip`.

```
pip install dawgz
```

Alternatively, if you need the latest features, you can install it using

```
pip install git+https://github.com/francois-rozet/dawgz
```

## Getting started

In `dawgz`, a job is a Python function decorated by `@dawgz.job`. This decorator allows to define the job's parameters, like its name, whether it is a job array, the resources it needs, etc. The job's dependencies are declared with the `@dawgz.after` decorator. At last, the `dawgz.schedule` function takes care of scheduling the jobs and their dependencies, with a selected backend. For more information, check out the [interface](#Interface) and the [examples](examples/).

Follows a small example demonstrating how one could use `dawgz` to calculate `π` (very roughly) using the [Monte Carlo method](https://en.wikipedia.org/wiki/Monte_Carlo_method). We define two jobs: `generate` and `estimate`. The former is a *job array*, meaning that it is executed concurrently for all values of `i = 0` up to `tasks - 1`. It also defines a [postcondition](https://en.wikipedia.org/wiki/Postconditions) ensuring that the file `pi_{i}.npy` exists after the job's completion. The job `estimate` has `generate` as dependency, meaning it should only start after `generate` succeeded.

```python
import glob
import numpy as np
import os

from dawgz import job, after, ensure, schedule

samples = 10000
tasks = 5

@ensure(lambda i: os.path.exists(f"pi_{i}.npy"))
@job(array=tasks, cpus=1, ram="2GB", time="5:00")
def generate(i: int):
    print(f"Task {i + 1} / {tasks}")

    x = np.random.random(samples)
    y = np.random.random(samples)
    within_circle = x**2 + y**2 <= 1

    np.save(f"pi_{i}.npy", within_circle)

@after(generate)
@job(cpus=2, ram="4GB", time="15:00")
def estimate():
    files = glob.glob("pi_*.npy")
    stack = np.vstack([np.load(f) for f in files])
    pi_estimate = stack.mean() * 4

    print(f"π ≈ {pi_estimate}")

schedule(estimate, name="pi.py", backend="async")
```

Running this script with the `'async'` backend displays

```
$ python examples/pi.py
Task 1 / 5
Task 2 / 5
Task 3 / 5
Task 4 / 5
Task 5 / 5
π ≈ 3.141865
```

Alternatively, on a Slurm HPC cluster, changing the backend to `'slurm'` results in the following job queue.

```
$ squeue -u username
             JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)
           1868832       all estimate username PD       0:00      1 (Dependency)
     1868831_[2-4]       all generate username PD       0:00      1 (Resources)
         1868831_0       all generate username  R       0:01      1 node-x
         1868831_1       all generate username  R       0:01      1 node-y
```

In addition to the Python interface, `dawgz` provides a simple command-line interface (CLI) to list the scheduled workflows, the jobs of a workflow or the output(s) of a job.

```
$ dawgz
    Name    ID        Date                 Backend      Jobs    Errors
--  ------  --------  -------------------  ---------  ------  --------
 0  pi.py   8094aa20  2022-02-28 16:37:58  async           2         0
 1  pi.py   9cc409fd  2022-02-28 16:38:33  slurm           2         0
$ dawgz 1
    Name                ID  State
--  -------------  -------  -------
 0  generate[0-4]  1868831  MIXED
 1  estimate       1868832  PENDING
$ dawgz 1 0
    Name         State      Output
--  -----------  ---------  ----------
 0  generate[0]  COMPLETED  Task 1 / 5
 1  generate[1]  COMPLETED  Task 2 / 5
 2  generate[2]  RUNNING
 3  generate[3]  RUNNING
 4  generate[4]  PENDING
```

## Interface

### Decorators

The package provides four decorators:

* `@dawgz.job` registers a function as a job, with its settings (name, array, resources, ...). It should always be the first (lowest) decorator. In the following example, `a` is a job with the name `'A'` and a time limit of one hour.

    ```python
    @job(name="A", time="01:00:00")
    def a():
    ```

    All keyword arguments other than `name`, `array` and `array_throttle` are passed as settings to the scheduler. For example, with the `slurm` backend, the following would lead to a job array of 64 tasks, with a maximum of 3 simultaneous tasks running exclusively on `tesla` or `quadro` partitions.

    ```python
    @job(array=64, array_throttle=3, partition="tesla,quadro")
    ```

    Importantly, a job is **shipped with its context**, meaning that modifying global variables after it has been created does not affect its execution.

* `@dawgz.after` adds one or more dependencies to a job. By default, the job waits for its dependencies to complete with success. The desired status can be set to `"success"` (default), `"failure"` or `"any"`. In the following example, `b` waits for `a` to complete with `"failure"`.

    ```python
    @after(a, status="failure")
    @job
    def b():
    ```

* `@dawgz.waitfor` declares whether the job has to wait for `"all"` (default) or `"any"` of its dependencies to be satisfied before starting. In the following example, `c` waits for either `a` or `b` to complete (with success).

    ```python
    @after(a, b)
    @waitfor("any")
    @job
    def c():
    ```

* `@dawgz.ensure` adds a [postcondition](https://wikipedia.org/wiki/Postconditions) to a job, i.e. a condition that must be `True` after the execution of the job. Not satisfying all postconditions after execution results in an `AssertionError` at runtime. In the following example, `d` ensures that the file `log.txt` exists.

    ```python
    @ensure(lambda: os.path.exists("log.txt"))
    @job
    def d():
    ```

    Traditionally, postconditions are only **necessary** indicators that a task completed with success. In `dawgz`, they are considered both necessary and **sufficient** indicators. Therefore, postconditions can be used to detect jobs that have already been executed and prune them out of the workflow. To do so, set `prune=True` in `dawgz.schedule`.

### Backends

Currently, `dawgz.schedule` supports three backends: `async`, `dummy` and `slurm`.

* `async` waits asynchronously for dependencies to complete before executing each job. The jobs are executed by the current Python interpreter.
* `dummy` is equivalent to `async`, but instead of executing the jobs, prints their name before and after a short (random) sleep time. The main use of `dummy` is debugging.
* `slurm` submits the jobs to the Slurm workload manager by automatically generating `sbatch` submission scripts.
