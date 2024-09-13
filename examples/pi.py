#!/usr/bin/env python

import glob
import numpy as np

from dawgz import after, ensure, job, schedule
from os.path import exists

samples = 10000
tasks = 5


@ensure(lambda i: exists(f"pi_{i}.npy"))
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


if __name__ == "__main__":
    schedule(estimate, name="pi.py", backend="async")
