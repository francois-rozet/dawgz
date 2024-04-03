r"""Directed Acyclic Workflow Graph Scheduling"""

__version__ = '0.5.2'

from functools import partial
from typing import *

from .schedulers import schedule
from .workflow import Job, leafs, roots


def job(f: Callable = None, /, **kwargs) -> Union[Callable, Job]:
    if f is None:
        return partial(job, **kwargs)
    else:
        return Job(f, **kwargs)


def after(*deps: Job, status: str = 'success') -> Callable:
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
