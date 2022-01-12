# Directed Acyclic Workflow Graph Scheduling

Would you like fully reproducible and reusable workflows that run on HPC clusters as seamlessly as on your machine? Tired of writing and managing large Slurm submission scripts? Do you have to comment out large parts of your pipeline whenever something failed? Hate YAML and config files? Then `dawgz` is made for you!

The `dawgz` package allows you to define and execute complex workflows, directly in Python. It provides a lightweight interface that enables to define jobs along with their dependencies, requirements, postconditions, etc. After defining the workflow, you can schedule target jobs in a single line of code and all their dependencies will be automatically scheduled as well. Importantly, the scheduling backend can be changed with a single parameter, which enables to execute the same workflow on your personal computer and HPC clusters.

> `dawgz` should be pronounced *dogs* :dog:

## Installation

The `dawgz` package is available on [PyPi](https://pypi.org/project/dawgz/), which means it is installable via `pip`.

```console
$ pip install dawgz
```

Alternatively, if you need the latest features, you can install it using

```console
$ pip install git+https://github.com/francois-rozet/dawgz
```

## Getting started

TODO: `dawgz`'s interface is based on decorators...

TODO: explain the example

```python
import glob
import numpy as np
import os

from dawgz import job, after, ensure, schedule

samples = 10000
tasks = 10

@ensure(lambda i: os.path.exists(f'pi_{i}.npy'))
@job(cpus=2, ram='2GB', array=tasks)
def sample(i: int):
    print(f'Task {i + 1} / {tasks}')

    x = np.random.random(samples)
    y = np.random.random(samples)
    within_circle = x ** 2 + y ** 2 <= 1

    np.save(f'pi_{i}.npy', within_circle)

@after(sample)
@job(cpus=4, ram='4GB', timelimit='15:00')
def estimate():
    files = glob.glob('pi_*.npy')
    stack = np.vstack([np.load(f) for f in files])
    pi_estimate = stack.mean() * 4

    print(f'π ≈ {pi_estimate}')

schedule(estimate, backend='local')
```

TODO: submitting

```console
$ python examples/pi.py
TODO
```

On a Slurm HPC cluster, changing the backend to `'slurm'` gives the following job queue.

```
$ squeue -u username
TODO
```

Check out the [examples](examples/) to explore the functionalities.

### Backends

Currently, `dawgz.schedule` only supports two backends: `local` and `slurm`.

* `local` schedules locally the submitted jobs by waiting asynchronously for dependencies to finish before submitting each job. It does not take the required resources into account.
* `slurm` submits the jobs to the Slurm workload manager by generating automatically the `sbatch` submission scripts.

## Contributing

TODO
