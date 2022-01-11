import glob
import sys
import numpy as np
import os

from dawgz import after, ensure, job, schedule


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
@ensure(lambda: 2 + 2 == 2 + 2)  # You can add multiple postconditions!
@job(name='merge_and_show', cpus='4')  # Ability to overwrite job name
def merge():
    files = glob.glob('pi-*.npy')
    stack = np.vstack([np.load(f) for f in files])
    pi_estimate = stack.sum() / (n * tasks) * 4
    print('π ≅', pi_estimate)
    np.save('pi.npy', pi_estimate)

# Schedule merge and its dependencies for execution
schedule(merge, backend='local')
