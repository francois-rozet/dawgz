#!/usr/bin/env python

import dawgz


@dawgz.job
def preprocessing():
    print("data preprocessing")


@dawgz.job
def train(i: int):
    print(f"training step {i}")


@dawgz.job
def evaluate(i: int):
    print(f"evaluation step {i}")


if __name__ == "__main__":
    main_jobs = [preprocessing()]
    eval_jobs = []

    for i in range(4):
        main_jobs.append(train(i).after(main_jobs[-1]))
        eval_jobs.append(evaluate(i).after(main_jobs[-1]))

    dawgz.schedule(*eval_jobs, name="train.py", backend="async")
