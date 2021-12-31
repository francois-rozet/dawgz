r"""Directed Acyclic Workflow Graph Scheduling"""

__version__ = '0.0.3'


from functools import partial
from typing import Callable, Union
from .workflow import Job


def job(f: Callable = None, /, **kwargs) -> Union[Callable, Job]:
    if f is None:
        return partial(job, **kwargs)
    return Job(f, **kwargs)


def after(*jobs, cond: str = 'success') -> Callable:
    def decorator(self: Job) -> Job:
        self.after(*jobs, condition=cond)
        return self
    return decorator


def waitfor(mode: str) -> Callable:
    def decorator(self: Job) -> Job:
        self.waitfor = mode
        return self
    return decorator


def backend(backend: str) -> Callable:
    def decorator(self: Job) -> Job:
        self.backend = backend
        return self
    return decorator
