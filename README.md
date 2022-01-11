# Directed Acyclic Workflow Graph Scheduling

Would you like fully reproducible and reusable workflows that run on HPC clusters as seamlessly as on your machine? Tired of writing and managing large Slurm submission scripts? Do you have to comment out large parts of your pipeline whenever something failed? Hate YAML and config files? Then `dawgz` is made for you!

The `dawgz` package allows you to define and execute complex workflows, directly in Python. It provides a lightweight interface that enables to define jobs along with their dependencies, requirements, postconditions, etc. After defining the workflow, you can schedule target jobs in a single line of code and all their dependencies will be automatically scheduled as well. Importantly, the scheduling backend can be changed with a single parameter, which enables to execute the same workflow on your personal computer and HPC clusters.

> `dawgz` should be pronounced *dogs* :dog:

## Getting started

```python
import glob
import numpy as np
import os

from dawgz import after, ensure, job, schedule

n = 10000
tasks = 10

@ensure(lambda i: os.path.exists(f'pi-{i}.npy'))
@job(cpus='4', memory='4GB', array=tasks)
def estimate(i: int):
    print(f'Executing task {i + 1} / {tasks}.')
    x = np.random.random(n)
    y = np.random.random(n)
    pi_estimate = (x**2 + y**2 <= 1)
    np.save(f'pi-{i}.npy', pi_estimate)

@after(estimate)
@ensure(lambda: os.path.exists('pi.npy'))
@job(cpus='4')
def merge():
    files = glob.glob('pi-*.npy')
    stack = np.vstack([np.load(f) for f in files])
    pi_estimate = stack.sum() / (n * tasks) * 4
    print('π ≅', pi_estimate)
    np.save('pi.npy', pi_estimate)

schedule(merge, backend='local')  # Executes merge and its dependencies
```
Executing this Python program (`python examples/pi.py`) on a Slurm HPC cluster will launch the following jobs.
```
           1803299       all    merge username PD       0:00      1 (Dependency)
     1803298_[6-9]       all estimate username PD       0:00      1 (Resources)
         1803298_3       all estimate username  R       0:01      1 compute-xx
         1803298_4       all estimate username  R       0:01      1 compute-xx
         1803298_5       all estimate username  R       0:01      1 compute-xx
```

Check the [examples](examples/) directory to explore the functionality.

## Installation

The `dawgz` package is available on [PyPi](https://pypi.org/project/dawgz/), which means it is installable via `pip`.
```console
you@local:~ $ pip install dawgz
```
If you would like the latest features, you can install it using this Git repository.
```console
you@local:~ $ pip install git+https://github.com/francois-rozet/dawgz
```
If you would like to run the examples as well, be sure to install the *optional* example dependencies.
```console
you@local:~ $ pip install 'dawgz[examples]'
```

## Available backends

Currently, `dawgz.schedule` only supports a `local` and `slurm` backend.

## Contributing

TODO

## License

As described in the [`LICENSE`](LICENSE.txt) file.
