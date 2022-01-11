import argparse
import glob
import sys
import numpy as np
import os

from dawgz import after, ensure, job, schedule, leafs



# Prepare argument parser
parser = argparse.ArgumentParser('dawgz π demo.')
parser.add_argument('--backend', type=str, default='local', help='Compute backend (default: local).')
parser.add_argument('--partition', type=str, default=None, help='Partition to deploy the jobs on and can only be specified through the Slurm backend (default: none).')
arguments, _ = parser.parse_known_args()


## BEGIN Workflow definition ###################################################

# Workflow parameters
n = 10000
tasks = 25

@ensure(lambda i: os.path.exists(f'pi-{i}.npy'))
@job(cpus='4', memory='4GB', array=tasks)
def estimate(i):
    print(f'Executing task {i + 1} / {tasks}.')
    x = np.random.random(n)
    y = np.random.random(n)
    pi_estimate = (x**2 + y**2 <= 1)
    np.save(f'pi-{i}.npy', pi_estimate)

@after(estimate)
@ensure(lambda: os.path.exists('pi.npy'))
@ensure(lambda: True)  # You can add multiple postconditions!
@job(cpus='4', name='merge_and_show')  # Ability to overwrite job name
def merge():
    files = glob.glob('pi-*.npy')
    stack = np.vstack([np.load(f) for f in files])
    pi_estimate = stack.sum() / (n * tasks) * 4
    print('π ≅', pi_estimate)
    np.save('pi.npy', pi_estimate)

r"""Find the terminal nodes of the specified root node (estimate)
and prune the jobs from the workflow whose postconditions
have been satisfied.
"""
jobs = leafs(estimate)
print(jobs)  # prints merge_and_show

# Schedule the jobs for execution
schedule(*jobs, backend=arguments.backend)
if arguments.backend == 'slurm':
    print('Jobs have been submitted!')
