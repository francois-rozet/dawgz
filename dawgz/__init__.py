r"""Directed Acyclic Workflow Graph Scheduling"""

from __future__ import annotations

__version__ = "2.0.0"

from collections.abc import Callable
from functools import partial, wraps
from tabulate import tabulate
from typing import Any, Literal, ParamSpec, overload

from .schedulers import (
    AsyncScheduler,
    DummyScheduler,
    Scheduler,
    SlurmScheduler,
)
from .utils import eprint
from .workflow import Job

P = ParamSpec("P")


@overload
def job(
    fun: Callable[P, Any],
    /,
    *,
    name: str | None = ...,
    settings: dict[str, Any] = ...,
    **kwargs,
) -> Callable[P, Job]: ...


@overload
def job(
    *,
    name: str | None = ...,
    settings: dict[str, Any] = ...,
    **kwargs,
) -> Callable[[Callable[P, Any]], Callable[P, Job]]: ...


def job(
    fun: Callable[P, Any] | None = None,
    /,
    *,
    name: str | None = None,
    interpreter: str | None = None,
    settings: dict[str, Any] = {},  # noqa: B006
    **kwargs,
) -> Callable[P, Job] | Callable[[Callable[P, Any]], Callable[P, Job]]:
    r"""Decorator to capture the arguments of a function for later execution.

    Arguments:
        fun: A function.
        name: The job name.
        interpreter: An optional Python interpreter command. For example, `uv run` or `torchrun`.
        settings: The settings of the job, interpreted by the scheduler. Settings include
            the allocated resources (e.g. `cpus=4`, `ram="16GB"`), the estimated runtime
            (e.g. `time="03:14:15"`), the partition (e.g. `partition="gpu"`) and much
            more.
        kwargs: Additional keyword arguments added to `settings`.
    """

    settings = settings.copy()
    settings.update(kwargs)

    if fun is None:
        return partial(job, **kwargs)

    @wraps(fun)
    def factory(*args, **kwargs) -> Job:
        return Job(
            fun,
            args,
            kwargs,
            name=name,
            interpreter=interpreter,
            settings=settings,
        )

    return factory


def schedule(
    *jobs: Job,
    backend: Literal["async", "dummy", "slurm"],
    quiet: bool = False,
    **kwargs,
) -> Scheduler:
    r"""Schedules a group of jobs.

    Jobs that have already been executed, as determined by their completion status, will be pruned.

    Arguments:
        jobs: A group of jobs describing a workflow.
        backend: The scheduling backend.
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
    scheduler(*jobs)
    scheduler.dump()

    if scheduler.traces and not quiet:
        eprint(tabulate(scheduler.traces.items(), ("Job", "Error"), showindex=True))

    return scheduler
