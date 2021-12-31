r"""Directed Acyclic Workflow Graph Scheduling"""

__version__ = '0.0.2'


from functools import partial
from typing import Callable, Union
from .workflow import Job


def job(f: Callable = None, /, **kwargs) -> Union[Callable, Job]:
    if f is None:
        return partial(job, **kwargs)
    return Job(f, **kwargs)
