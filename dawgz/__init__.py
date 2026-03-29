r"""Directed Acyclic Workflow Graph Scheduling"""

__version__ = "2.5.2"

import os
import rich.box
import rich.console
import rich.table
import shutil

from collections.abc import Callable
from functools import partial, wraps
from typing import Any, Literal, ParamSpec, overload

import __main__

from .constants import get_dawgz_dir, set_dawgz_dir  # noqa: F401
from .schedulers import (
    AsyncScheduler,
    DummyScheduler,
    Scheduler,
    SlurmScheduler,
)
from .workflow import Job, JobArray

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
    shell: str = "/bin/bash",
    interpreter: str = "python",
    env: list[str] | None = None,
    settings: dict[str, Any] = {},  # noqa: B006
    **kwargs,
) -> Callable[P, Job] | Callable[[Callable[P, Any]], Callable[P, Job]]:
    r"""Decorator to capture the arguments of a function for later execution.

    Arguments:
        fun: A function.
        name: The job name.
        shell: The scripting shell.
        interpreter: The interpreter command. For example, `python`, `uv run` or `torchrun`.
        env: A sequence of shell commands to execute before the function is run. For example,
            exporting environment variables or loading modules.
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
            shell=shell,
            interpreter=interpreter,
            env=env,
            settings=settings,
        )

    return factory


def array(*jobs: Job, throttle: int | None = None) -> JobArray:
    r"""Creates an array from a group of independent jobs.

    Arguments:
        jobs: A group of jobs. These jobs should not have dependencies or dependents.
        throttle: The maximum number of simultaneously running jobs in the array.
            Only affects the Slurm backend.
    """

    return JobArray(*jobs, throttle=throttle)


def schedule(
    *jobs: Job,
    backend: Literal["async", "dummy", "slurm"],
    name: str | None = None,
    quiet: bool = False,
    **kwargs,
) -> Scheduler:
    r"""Schedules a group of jobs.

    The `async` and `dummy` backends execute jobs within the current Python interpreter,
    and will therefore ignore interpreter, environment, and resource settings.

    Jobs that have already been executed, as determined by their completion status, will
    be pruned from the workflow.

    Arguments:
        jobs: A group of jobs describing a workflow.
        backend: The scheduling backend.
        name: The worflow name. If `None`, use the caller's filename instead.
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

    if name is None:
        name = os.path.basename(__main__.__file__)

    scheduler = backends[backend](name=name, **kwargs)
    scheduler(*jobs)
    scheduler.dump()

    if scheduler.traces and not quiet:
        table = rich.table.Table(box=rich.box.ROUNDED)
        table.add_column("", justify="right", no_wrap=True, min_width=2)
        table.add_column("Job", justify="left", no_wrap=True)
        table.add_column("Error", justify="left", no_wrap=False)

        for job, trace in scheduler.traces.items():
            table.add_row(str(scheduler.order[job]), str(job), trace)
            table.add_section()

        try:
            rich.console.Console(
                stderr=True,
                width=shutil.get_terminal_size((1_000_000, 0)).columns,
            ).print(table)
        except BrokenPipeError:
            pass

    return scheduler
