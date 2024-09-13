#!/usr/bin/env python

from dawgz import after, job, schedule


@job
def preprocessing():
    print("data preprocessing")


evals = []
previous = preprocessing

for i in range(1, 4):

    @after(previous)
    @job(name=f"train_{i}")
    def train():
        print(f"training step {i}")

    @after(train)
    @job(name=f"eval_{i}")
    def evaluate():
        print(f"evaluation step {i}")

    evals.append(evaluate)
    previous = train

if __name__ == "__main__":
    schedule(*evals, name="train.py", backend="async")
