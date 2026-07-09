r"""Local scheduling backends"""

from __future__ import annotations

import asyncio
import concurrent.futures as cf
import random

from collections.abc import Iterator
from contextlib import contextmanager
from functools import partial

from .core import (
    JobFailedError,
    JobNeverSatisfiedError,
    JobNotFailedError,
    Scheduler,
)
from ..utils import future, runpickle
from ..workflow import Job, JobArray


class AsyncScheduler(Scheduler):
    r"""Asynchronous scheduler.

    Jobs are executed asynchronously. A job is launched as soon as its dependencies are
    satisfied.
    """

    backend: str = "async"

    def __init__(self, name: str, max_workers: int | None = 1) -> None:
        r"""
        Arguments:
            name: The name of the workflow.
            max_workers: The maximum number of parallel processes.
                If `None`, use all CPU cores.
        """

        super().__init__(name=name)

        self.max_workers = max_workers

    @contextmanager
    def context(self) -> Iterator[None]:
        self.executor = cf.ProcessPoolExecutor(max_workers=self.max_workers)

        try:
            yield None
        finally:
            del self.executor

    async def satisfy(self, job: Job) -> None:
        pending = [
            asyncio.gather(self.submit(dep), future(status))
            for dep, status in job.dependencies.items()
        ]

        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

            for task in done:
                result, status = task.result()

                if isinstance(result, JobFailedError) and status != "success":
                    result = None
                elif not isinstance(result, JobFailedError) and status == "failure":
                    result = JobNotFailedError(repr(job))

                if isinstance(result, Exception):
                    if job.wait_mode == "all":
                        raise JobNeverSatisfiedError(repr(job)) from result
                elif job.wait_mode == "any":
                    break
            else:
                continue
            break
        else:
            if job.wait_mode == "any":
                raise JobNeverSatisfiedError(repr(job))

    async def exec(self, job: Job) -> None:
        loop = asyncio.get_running_loop()

        tag = self.tag(job)
        logfile = self.path / f"{tag}.log"

        try:
            if isinstance(job, JobArray):
                futures = [
                    loop.run_in_executor(
                        self.executor,
                        partial(
                            runpickle,
                            job[i].pkl,
                            logfile=str(logfile).replace(".log", f"_{i}.log"),
                        ),
                    )
                    for i in range(len(job))
                ]

                await asyncio.gather(*futures)
            else:
                await loop.run_in_executor(
                    self.executor,
                    partial(runpickle, job.pkl, logfile=logfile),
                )
        except Exception as e:
            raise JobFailedError(repr(job)) from e


class DummyScheduler(AsyncScheduler):
    r"""Dummy asynchronous scheduler.

    Jobs are scheduled asynchronously, but instead of executing them, their name is
    printed before and after a short (random) sleep time. Useful for debugging.
    """

    backend: str = "dummy"

    async def exec(self, job: Job) -> None:
        print(f"START {job!r}")
        await asyncio.sleep(random.random())
        print(f"END   {job!r}")
