#!/usr/bin/env python

import time

from dawgz import after, ensure, job, schedule, waitfor
from os.path import exists


@job
def a():
    print("a")
    time.sleep(3)
    print("a")
    raise Exception()


@job
def b():
    time.sleep(1)
    print("b")
    time.sleep(1)
    print("b")


@after(a, status="success")
@ensure(lambda: 2 + 2 == 2 * 2)
@ensure(lambda: 1 + 2 + 3 == 1 * 2 * 3)
@job
def c():
    print("c")


@after(b)
@ensure(lambda i: i != 42 or exists(f"{i}.log"))
@job(array=100)
def d(i: int):
    print(f"d{i}")

    with open(f"{i}.log", "w") as file:
        file.write("done")


@after(a, d)
@waitfor("any")
@job
def e():
    print("e")


if __name__ == "__main__":
    schedule(c, e, name="simple.py", backend="async", prune=True)
