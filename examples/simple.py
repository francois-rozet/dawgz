#!/usr/bin/env python

import time

import dawgz


@dawgz.job
def a() -> None:
    print("a")
    time.sleep(3)
    print("a")
    raise RuntimeError("foo")


@dawgz.job
def b() -> None:
    time.sleep(1)
    print("b")
    time.sleep(1)
    print("b")


@dawgz.job
def c(i: int) -> None:
    print(f"c{i}")


@dawgz.job
def d() -> None:
    print("d")


@dawgz.job
def e() -> None:
    print("e")


if __name__ == "__main__":
    a_job = a()
    b_job = b()
    c_jobs = [c(i).after(b_job).mark("pending" if i == 42 else "success") for i in range(100)]
    d_job = d().after(*c_jobs)
    e_job = e().after(a_job, d_job).waitfor("any")

    dawgz.schedule(e_job, backend="async", max_workers=4)
