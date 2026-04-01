#!/usr/bin/env python

import dawgz


@dawgz.job
def preprocessing() -> None:
    print("data preprocessing")


@dawgz.job
def train(i: int) -> None:
    print(f"training step {i}")


@dawgz.job
def evaluate(i: int) -> None:
    print(f"evaluation step {i}")


if __name__ == "__main__":
    main_jobs = [preprocessing()]
    eval_jobs = []

    for i in range(1, 4):
        main_jobs.append(train(i).after(main_jobs[-1]))
        eval_jobs.append(evaluate(i).after(main_jobs[-1]))

    dawgz.schedule(*eval_jobs, backend="async", max_workers=4)
