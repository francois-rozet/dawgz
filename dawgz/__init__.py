r"""Directed Acyclic Workflow Graph Scheduling"""

__version__ = "1.0.0"

from functools import partial
from tabulate import tabulate
from typing import Any, Callable, Dict, Iterable, Optional, Union

# isort: split
from .schedulers import (
    AsyncScheduler,
    DummyScheduler,
    Scheduler,
    SlurmScheduler,
)
from .utils import eprint
from .workflow import Job


def job(
    f: Callable = None,
    *,
    name: Optional[str] = None,
    array: Optional[Union[int, Iterable[int]]] = None,
    array_throttle: Optional[int] = None,
    settings: Dict[str, Any] = {},  # noqa: B006
    **kwargs,
) -> Union[Callable, Job]:
    r"""Transforms a function into a job.

    Arguments:
        f: A function.
        name: The job name.
        array: An array size or set of indices. A job array is a group of jobs that can
            be launched concurrently. They are described by the same function, but
            differ by their index.
        array_throttle: The maximum number of simultaneously running jobs in an array.
            Only affects the Slurm backend.
        settings: The settings of the job, interpreted by the scheduler. Settings include
            the allocated resources (e.g. `cpus=4`, `ram="16GB"`), the estimated runtime
            (e.g. `time="03:14:15"`), the partition (e.g. `partition="gpu"`) and much
            more.
        kwargs: Additional keyword arguments added to `settings`.
    """

    kwargs.update(
        name=name,
        array=array,
        array_throttle=array_throttle,
        settings=settings,
    )

    if f is None:
        return partial(job, **kwargs)
    else:
        return Job(f, **kwargs)


def after(*deps: Job, status: str = "success") -> Callable:
    r"""Adds dependencies to a job.

    Arguments:
        deps: A set of job dependencies.
        status: The desired dependency status. Options are `"success"`, `"failure"` or `"any"`.
    """

    def decorator(self: Job) -> Job:
        self.after(*deps, status=status)
        return self

    return decorator


def waitfor(mode: str) -> Callable:
    r"""Modifies the waiting mode of a job.

    Arguments:
        mode: The dependency waiting mode. Options are `"all"` (default) or `"any"`.
    """

    def decorator(self: Job) -> Job:
        self.waitfor = mode
        return self

    return decorator


def ensure(condition: Callable) -> Callable:
    r"""Adds a postcondition to a job.

    Arguments:
        condition: A predicate that should be `True` after the execution of the job.
    """

    def decorator(self: Job) -> Job:
        self.ensure(condition)
        return self

    return decorator


def schedule(
    *jobs: Job,
    backend: str,
    prune: bool = False,
    quiet: bool = False,
    **kwargs,
) -> Scheduler:
    r"""Schedules a group of jobs.

    Arguments:
        jobs: A group of jobs describing a workflow.
        backend: The scheduling backend. Options are `"async"`, `"dummy"` or `"slurm"`.
        prune: Whether to prune jobs that have already been executed or not,
            as determined by their postconditions.
        quiet: Whether to display eventual job errors or not.
        kwargs: Keyword arguments passed to the scheduler's constructor.

    Returns:
        The workflow scheduler.
    """

    backends = {
        s.backend: s
        for s in (
            AsyncScheduler,
            DummyScheduler,
            SlurmScheduler,
        )
    }

    scheduler = backends.get(backend)(**kwargs)
    scheduler(*jobs, prune=prune)
    scheduler.dump()

    if scheduler.traces and not quiet:
        eprint(tabulate(scheduler.traces.items(), ("Job", "Error"), showindex=True))

    return scheduler
