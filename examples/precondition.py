r"""This example will show you how to use preconditions."""

import glob
import os
import numpy as np

from dawgz import job, after, waitfor, require, ensure, schedule


@ensure(lambda i: os.path.exists(f'normal-{i}.npy'))
@job(array=10)
def generate_normal(i: int):
    np.save(f'normal-{i}.npy', np.random.normal())

@ensure(lambda i: os.path.exists(f'uniform-{i}.npy'))
@job(array=100)
def generate_uniform(i: int):
    np.save(f'uniform-{i}.npy', np.random.random())

@job
def print_intro():
    print('Hello world!')

@after(generate_normal)
@after(generate_uniform)
@after(print_intro)
@require(lambda: len(glob.glob('*.npy')) >= 10 + 100)  # Precondition that depends on several dependencis
@job(array=25)
def do_something(i: int):
    print(f'Step {i}')

@after(do_something)
@job
def last():
    print('Done!')


schedule(last, backend='local')
