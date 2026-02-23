#!/usr/bin/env python

import time

import dawgz


@dawgz.job
def a():
    print("a")
    time.sleep(3)
    print("a")
    raise Exception()


@dawgz.job
def b():
    time.sleep(1)
    print("b")
    time.sleep(1)
    print("b")


@dawgz.job
def c(i: int):
    print(f"c{i}")


@dawgz.job
def d():
    print("d")


@dawgz.job
def e():
    print("e")


if __name__ == "__main__":
    a_job = a()
    b_job = b()
    c_jobs = [c(i).after(b_job).mark("pending" if i == 42 else "success") for i in range(100)]
    d_job = d().after(*c_jobs)
    e_job = e().after(a_job, d_job).waitfor("any")

    dawgz.schedule(e_job, name="simple.py", backend="async", prune=True)
