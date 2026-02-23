#!/usr/bin/env python

import glob
import numpy as np

import dawgz


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
