# Directed Acyclic Workflow Graph Scheduling

`dawgz` provides a lightweight and intuitive Python interface to declare, schedule and execute job workflows. It can also delegate execution to resource management backends such as [Slurm](https://wikipedia.org/wiki/Slurm_Workload_Manager), which means you can write, configure, and submit your workflows without ever leaving Python.

## Installation

The `dawgz` package is available on [PyPi](https://pypi.org/project/dawgz/) and can be installed with `pip`.

```
pip install dawgz
```

Alternatively, if you need the latest features, you can install it from source.

```
pip install git+https://github.com/francois-rozet/dawgz
```

## Getting started

In `dawgz`, a job is a Python function. The `dawgz.job` decorator allows to declare the resources a job requires and capture its arguments. A job's dependencies within the workflow are declared with the `dawgz.Job.after` method. After declaration, the `dawgz.schedule` function takes care of scheduling the jobs and their dependencies, with a selected execution backend. For more information, check out the [interface](#Interface) and the [examples](examples/).

Follows a small example demonstrating how one could use `dawgz` to calculate `π` (very roughly) using the [Monte Carlo method](https://en.wikipedia.org/wiki/Monte_Carlo_method). We define two jobs, `generate` and `estimate`. Five instances of `generate` are declared that will be executed concurrently. The `estimate` job has all `generate` instances as dependencies, meaning that it will only start after they have completed successfuly.

```python
import dawgz
import glob
import numpy as np

@dawgz.job(cpus=1, ram="2GB", time="5:00")
def generate(i: int):
    print(f"Task {i + 1}")

    x = np.random.random(10000)
    y = np.random.random(10000)
    within_circle = x**2 + y**2 <= 1

    np.save(f"pi_{i}.npy", within_circle)

@dawgz.job(cpus=2, ram="4GB", time="15:00")
def estimate():
    files = glob.glob("pi_*.npy")
    stack = np.vstack([np.load(f) for f in files])
    pi_estimate = stack.mean() * 4

    print(f"π ≈ {pi_estimate}")

if __name__ == "__main__":
    generate_jobs = [generate(i) for i in range(5)]
    estimate_job = estimate().after(*generate_jobs)

    dawgz.schedule(estimate_job, name="pi.py", backend="async")
```

```
$ python examples/pi.py
Task 1 / 5
Task 2 / 5
Task 3 / 5
Task 4 / 5
Task 5 / 5
π ≈ 3.141865
```

Alternatively, on a Slurm HPC cluster, changing the backend to `"slurm"` results in the following job queue.

```
$ python examples/pi.py
$ squeue -u username
             JOBID PARTITION          NAME     USER ST       TIME  NODES NODELIST(REASON)
           1868833       all 0005_estimate username PD       0:00      1 (Dependency)
           1868832       all 0004_generate username PD       0:00      1 (Resources)
           1868831       all 0003_generate username PD       0:00      1 (Resources)
           1868830       all 0002_generate username PD       0:00      1 (Resources)
           1868828       all 0000_generate username  R       0:02      1 node-x
           1868829       all 0001_generate username  R       0:01      1 node-y
```

In addition to the Python interface, `dawgz` provides a simple command-line interface to list the scheduled workflows, the jobs of a workflow or the output of a job.

```
$ dawgz
    Name    ID                        Date                 Backend      Jobs    Errors
--  ------  ------------------------  -------------------  ---------  ------  --------
 0  pi.py   handsome_jicama_bfc5a3e4  2022-02-28 16:37:58  async           2         0
 1  pi.py   crowded_machine_23bdd047  2022-02-28 16:38:33  slurm           2         0
$ dawgz 1
    Job               ID  State
--  -----------  -------  ---------
 0  generate(0)  1868838  COMPLETED
 1  generate(1)  1868829  RUNNING
 2  generate(2)  1868830  RUNNING
 3  generate(3)  1868831  PENDING
 4  generate(4)  1868832  PENDING
 5  estimate()   1868833  PENDING
$ dawgz 1 0
    Job          State      Output
--  -----------  ---------  ----------
 0  generate(0)  COMPLETED  Task 1 / 5
```

## Interface

* `dawgz.job` registers a function as a job, with its settings (name, resources, ...). In the following example, `a` is a job with the name `"A"`, a time limit of one hour, and running on `tesla` or `quadro` partitions.

    ```python
    @dawgz.job(name="A", time="01:00:00", partition="tesla,quadro")
    def a(n: int, x: float):
        ...
    a_job = a(3, 0.14)
    ```

    When the decorated function is called, its context and arguments are captured in a `dawgz.Job` instance for later execution. Modifying global variables after it has been created will not affect its execution. However, the content of Python modules is not captured, which means that modifying a module after a job has been submitted can affect its execution. If this becomes an issue for you, you can register your module such that it is pickled by value rather than by reference.

    ```python
    import cloudpickle
    import my_module

    cloudpickle.register_pickle_by_value(my_module)

    @dawgz.job
    def a():
        my_module.my_function()
    ```

    To declare that a job must wait for another one to complete, you can use the `dawgz.Job.after` method. By default, the job waits for its dependencies to complete with success. The desired completion status can be set to `"success"` (default), `"failure"` or `"any"`.

    ```python
    @dawgz.job
    def b():
        ...
    b_job = b().after(a_job, status="failure")
    ```

    If a job has several dependencies, the `dawgz.Job.waitfor` method can be used to declare whether it should wait for `"all"` (default) or `"any"` of them to be satisfied before starting.

    ```python
    @dawgz.job
    def c():
        ...
    c_job = c().after(a_job, b_job).waitfor("any")
    ```

    When running the same workflow multiple times, you may want to skip jobs that have already been executed. You can mark these jobs as completed with the `dawgz.Job.mark` method, and they will be automatically pruned out of the workflow. The completion status can be set to `"pending"` (default), `"success"`, `"failure"` or `"cancelled"`.

    ```python
    @dawgz.job
    def d():
        ...
    d_job = d().mark("success")
    ```

* `dawgz.array` creates a job array from a group of independent jobs. The primary use case of job arrays is to schedule a large number of small jobs while throttling the number of simultaneously running jobs without saturating the Slurm queue. The returned object is itself a `dawgz.Job` instance and supports the methods presented above (`after`, `waitfor`, ...).

    ```python
    @dawgz.job
    def e(i):
        ...

    e_jobs = [e(i) for i in range(42)]
    e_array = dawgz.array(*e_jobs, throttle=3)
    e_array.after(d_job)

    dawgz.schedule(e_array, backend="slurm")
    ```

* `dawgz.schedule` schedules a set of jobs, along their dependencies. Three backends are currently supported: `async`, `dummy` and `slurm`.

    1. `async` waits asynchronously for dependencies to complete before executing each job. The jobs are executed by the current Python interpreter.
    2. `dummy` is equivalent to `async`, but instead of executing the jobs, prints their name before and after a short (random) sleep time. The main use of `dummy` is debugging.
    3. `slurm` submits the jobs to the Slurm workload manager by automatically generating `sbatch` submission scripts.
