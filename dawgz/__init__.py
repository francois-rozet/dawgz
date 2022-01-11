r"""Directed Acyclic Workflow Graph Scheduling

The dawgz package contains modules and data structures to represent
and execute directed acyclic workflows. The workflows can be run on
your laptop, or when requested, code will be specifically generated
to run the workflows on HPC clusters without the need to specify
those annoying submission scripts.
"""

__version__ = '0.1.6'


from functools import partial
from typing import Callable, Union

from .schedulers import schedule
from .workflow import Job, leafs, roots


def job(f: Callable = None, /, **kwargs) -> Union[Callable, Job]:
    if f is None:
        return partial(job, **kwargs)
    else:
        return Job(f, **kwargs)


def after(*deps, status: str = 'success') -> Callable:
    def decorator(self: Job) -> Job:
        self.after(*deps, status=status)
        return self

    return decorator


def waitfor(mode: str) -> Callable:
    def decorator(self: Job) -> Job:
        self.waitfor = mode
        return self

    return decorator


def ensure(condition: Callable) -> Callable:
    def decorator(self: Job) -> Job:
        self.ensure(condition)
        return self

    return decorator


def require(condition: Callable) -> Callable:
    def decorator(self: Job) -> Job:
        self.require(condition)
        return self

    return decorator


def empty(self: Job) -> Job:
    self.f = None
    return self
