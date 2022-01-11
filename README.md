Reproducible research and reusable acyclic workflows in Python. Execute code on HPC systems as if you executed them on your machine!

## Motivation

Would you like fully reproducible research or reusable workflows that seamlessly run on HPC clusters?
Tired of writing and managing large Slurm submission scripts? Do you have comment out large parts of your pipeline whenever its results have been generated? Hate YAML?
Don't waste your precious time! `dawgz` allows you to directly describe complex pipelines in Python, that run on your personal computer and large HPC clusters.


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

merge.prune()  # Prune jobs whose postconditions have been satisfied

schedule(merge, backend='local')  # Executes merge and its dependencies
```
Executing this Python program (`python examples/pi.py --backend slurm`) on a Slurm HPC cluster will launch the following jobs.
```
           1803299       all    merge username PD       0:00      1 (Dependency)
     1803298_[6-9]       all estimate username PD       0:00      1 (Resources)
         1803298_3       all estimate username  R       0:01      1 compute-xx
         1803298_4       all estimate username  R       0:01      1 compute-xx
         1803298_5       all estimate username  R       0:01      1 compute-xx
```
The following example shows how workflow graphs can be dynamically allocated:
```python
from dawgz import after, job, schedule, terminal_nodes

@job(cpus='2', memory='4GB', array=5)
def generate(i: int):
    print(f'Generating data block {i}.')

@after(generate)
@job(cpus='1', memory='2GB', array=5)
def postprocess(i: int):
    print(f'Postprocessing data block {i}.')

def do_experiment(parameter):
    r"""This method allocates a `fit` and `make_plot` job
    based on the specified parameter."""

    @after(postprocess)
    @job(name=f'fit_{parameter}')  # By default, the name is equal to the function name
    def fit():
        print(f'Fit {parameter}.')

    @after(fit)
    @job(name=f'plt_{parameter}')  # Simplifies the identification of the logfile
    def make_plot():
        print(f'Plot {parameter}.')

# Programmatically build workflow
for parameter in [0.1, 0.2, 0.3, 0.4, 0.5]:
    do_experiment(parameter)

leafs = terminal_nodes(generate, prune=True)  # Find terminal nodes of workflow graph
schedule(*leafs, backend='local')
```


Check the [examples](examples/) directory to explore the functionality.

## Usage

TODO

## Available backends

Currently, `dawgz.schedule` only supports a `local` and `slurm` backend.

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

## Roadmap and TODO

- [ ] Add utilities for common post-conditions.
- [ ] Should `schedule` return metadata of jobs and workflow?
- [ ] More examples and documentation.
- [ ] Utilities to cleanup generated metadata and crashed jobs for the Slurm backend.
- [ ] Can jobs submit jobs on both local and Slurm backend?

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md).

## License

As described in the [`LICENSE`](LICENSE.txt) file.
