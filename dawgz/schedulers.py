r"""Scheduling backends"""

from __future__ import annotations

import asyncio
import concurrent.futures as cf
import csv
import os
import random
import shutil
import subprocess

from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from functools import cache
from pathlib import Path
from tabulate import tabulate
from typing import Any

from .constants import get_dawgz_dir
from .utils import cat, future, human_uuid, pickle, runpickle, slugify, trace
from .workflow import Job, JobArray, cycles, prune


class Scheduler(ABC):
    r"""Abstract workflow scheduler."""

    backend: str = None

    def __init__(
        self,
        name: str | None = None,
        settings: dict[str, Any] = {},  # noqa: B006
        **kwargs,
    ) -> None:
        r"""
        Arguments:
            name: The name of the workflow.
            settings: A dictionnary of settings.
            kwargs: Keyword arguments added to `settings`.
        """

        super().__init__()

        self.name = name
        self.date = datetime.now().replace(microsecond=0)
        self.uid = human_uuid()

        self.path = get_dawgz_dir() / self.uid
        self.path.mkdir(parents=True)

        # Settings
        self.settings = settings.copy()
        self.settings.update(kwargs)

        # Jobs
        self.order = {}
        self.results = {}
        self.traces = {}

    def dump(self) -> None:
        with open(self.path / "dump.pkl", "wb") as f:
            pickle.dump(self, f)

        with open(self.path.parent / "workflows.csv", mode="a", newline="") as f:
            csv.writer(f).writerow((
                self.name,
                self.uid,
                self.date,
                self.backend,
                len(self.order),
                len(self.traces),
            ))

    @staticmethod
    def load(path: Path) -> Scheduler:
        with open(path / "dump.pkl", "rb") as f:
            return pickle.load(f)

    def tag(self, job: Job) -> str:
        if job in self.order:
            i = self.order[job]
        else:
            i = self.order[job] = len(self.order)

        return f"{i:04d}_{slugify(job.name)}"

    def state(self, job: Job, i: int | None = None) -> str:
        if job in self.traces:
            return "FAILED"
        else:
            return "COMPLETED"

    def output(self, job: Job, i: int | None = None) -> Any:
        if isinstance(job, JobArray):
            return self.results[job][i]
        else:
            return self.results[job]

    def report(self, job: Job | None = None, i: int | None = None) -> str:
        if job is None:
            headers = ("Job", "State")
            rows = [(str(job), self.state(job)) for job in self.order]

            return tabulate(rows, headers, showindex=True, maxcolwidths=[None, 48, 16])
        else:
            headers = ("Job", "State", "Output")

            if job in self.traces:
                index = [None]
                rows = [(str(job), self.state(job), self.traces[job])]
            elif isinstance(job, JobArray):
                index = list(range(len(job))) if i is None else [i]
                rows = [(str(job[i]), self.state(job, i), self.output(job, i)) for i in index]
            else:
                index = [None]
                rows = [(str(job), self.state(job), self.output(job))]

            terminal_width = shutil.get_terminal_size((0, 0)).columns

            rows = [
                (
                    repr,
                    state,
                    cat("" if output is None else str(output), width=terminal_width - 64 - 3 * 2),
                )
                for repr, state, output in rows
            ]

            return tabulate(rows, headers, showindex=index, maxcolwidths=[None, 48, 16, None])

    def cancel(self, job: Job | None = None, i: int | None = None) -> str:
        raise NotImplementedError(f"'cancel' is not implemented for the {self.backend} backend.")

    @contextmanager
    def context(self) -> Iterator[None]:
        try:
            yield None
        finally:
            pass

    def __call__(self, *jobs: Job) -> None:
        for cycle in cycles(*jobs, backward=True):
            raise CyclicDependencyGraphError(" <- ".join(map(str, cycle)))

        jobs = prune(*dict.fromkeys(jobs))

        with self.context():
            asyncio.run(self.wait(*jobs))

    async def wait(self, *jobs: Job) -> None:
        self.tasks: dict[Job, asyncio.Task] = {}

        if jobs:
            await asyncio.wait(map(asyncio.create_task, map(self.submit, jobs)))
            await asyncio.wait(self.tasks.values())

        del self.tasks

    async def submit(self, job: Job) -> Any:
        if job in self.tasks:
            task = self.tasks[job]
        else:
            task = self.tasks[job] = future(self._submit(job), return_exceptions=True)

        result = await task

        if isinstance(result, Exception):
            self.traces[job] = trace(result)
        else:
            self.results[job] = result

        return result

    async def _submit(self, job: Job) -> Any:
        try:
            if job.satisfy_status == "ready":
                asyncio.ensure_future(self.satisfy(job))
            elif job.satisfy_status == "wait":
                await self.satisfy(job)
            elif job.satisfy_status == "never":
                raise DependencyNeverSatisfiedError(str(job))
            else:
                raise ValueError(f"Unknown status '{job.satisfy_status}'.")
        finally:
            self.tag(job)

        return await future(self.exec(job), return_exceptions=True)

    @abstractmethod
    async def satisfy(self, job: Job) -> None:
        pass

    @abstractmethod
    async def exec(self, job: Job) -> Any:
        pass


class AsyncScheduler(Scheduler):
    r"""Asynchronous scheduler.

    Jobs are executed asynchronously. A job is launched as soon as its dependencies are
    satisfied.
    """

    backend: str = "async"

    def __init__(self, name: str | None = None, pools: int | None = None, **kwargs) -> None:
        r"""
        Arguments:
            name: The name of the workflow.
            pools: The number of processing pools. If `None`, use threads instead.
            kwargs: Keyword arguments passed to :class:`Scheduler`.
        """

        super().__init__(name=name, **kwargs)

        self.pools = pools

    @contextmanager
    def context(self) -> Iterator[None]:
        if self.pools is None:
            self.executor = cf.ThreadPoolExecutor()
        else:
            self.executor = cf.ProcessPoolExecutor(self.pools)

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
                    result = JobNotFailedError(str(job))

                if isinstance(result, Exception):
                    if job.wait_mode == "all":
                        raise DependencyNeverSatisfiedError(str(job)) from result
                elif job.wait_mode == "any":
                    break
            else:
                continue
            break
        else:
            if job.wait_mode == "any":
                raise DependencyNeverSatisfiedError(str(job))

    async def exec(self, job: Job) -> Any:
        loop = asyncio.get_running_loop()

        try:
            if isinstance(job, JobArray):
                futures = [
                    loop.run_in_executor(self.executor, runpickle, job[i].exe)
                    for i in range(len(job))
                ]

                results = await asyncio.gather(*futures, return_exceptions=True)

                for result in results:
                    if isinstance(result, Exception):
                        raise result

                return results
            else:
                return await loop.run_in_executor(self.executor, runpickle, job.exe)
        except Exception as e:
            raise JobFailedError(str(job)) from e


class DummyScheduler(AsyncScheduler):
    r"""Dummy asynchronous scheduler.

    Jobs are scheduled asynchronously, but instead of executing them, their name is
    printed before and after a short (random) sleep time. Useful for debugging.
    """

    backend: str = "dummy"

    async def exec(self, job: Job) -> None:
        print(f"START {job}")
        await asyncio.sleep(random.random())
        print(f"END   {job}")


class SlurmScheduler(Scheduler):
    r"""Slurm scheduler.

    Jobs are submitted to the Slurm queue. Resources are allocated by the Slurm manager
    according to the job and scheduler settings. Job settings have precendence over
    scheduler settings.

    Most settings (e.g. `account`, `export`, `partition`) are passed directly to
    `sbatch`. A few settings (e.g. `cpus`, `gpus`, `ram`) are translated into their
    `sbatch` equivalents.
    """

    backend: str = "slurm"
    translate: dict[str, str] = {  # noqa: RUF012
        "tasks": "ntasks",
        "tasks_per_node": "ntasks-per-node",
        "cpus": "cpus-per-task",
        "gpus": "gpus-per-task",
        "ram": "mem",
        "memory": "mem",
        "timelimit": "time",
        "timeout": "time",
    }

    def __init__(
        self,
        name: str | None = None,
        shell: str = os.environ.get("SHELL", "/bin/sh"),
        interpreter: str = "python",
        env: list[str] | None = None,
        **kwargs,
    ) -> None:
        r"""
        Arguments:
            name: The name of the workflow.
            shell: The scripting shell.
            interpreter: The Python interpreter.
            env: A sequence of commands to execute before each job is launched.
            kwargs: Keyword arguments passed to :class:`Scheduler`.
        """

        super().__init__(name=name, **kwargs)

        assert shutil.which("sbatch") is not None, "sbatch executable not found"

        # Environment
        self.shell = shell
        self.interpreter = interpreter
        self.env = env

    @staticmethod
    @cache
    def sacct(jobid: str) -> dict[str, str]:
        text = subprocess.run(
            ["sacct", "-j", jobid, "-o", "JobID,State", "-n", "-P", "-X"],
            capture_output=True,
            check=True,
            text=True,
        ).stdout.strip("\n")

        return dict(line.split("|") for line in text.splitlines())

    def state(self, job: Job, i: int | None = None) -> str | None:
        if job in self.traces:
            return "CANCELLED"

        jobid = self.results[job]
        table = self.sacct(jobid)

        if isinstance(job, JobArray):
            if i is None:
                return ", ".join(sorted(set(table.values())))
            else:
                return table.get(f"{jobid}_{i}", None)
        else:
            return table.get(jobid, None)

    def output(self, job: Job, i: int | None = None) -> str | None:
        tag = self.tag(job)

        if isinstance(job, JobArray):
            logfile = self.path / f"{tag}_{i}.log"
        else:
            logfile = self.path / f"{tag}.log"

        if logfile.exists():
            with open(logfile, newline="", errors="replace") as f:
                return f.read()
        else:
            return None

    def report(self, job: Job | None = None, i: int | None = None) -> str:
        if job is None:
            headers = ("Job", "ID", "State")
            rows = []

            for job in self.order:
                if job in self.traces:
                    jobid = None
                else:
                    jobid = self.results[job]

                rows.append((str(job), jobid, self.state(job)))

            return tabulate(rows, headers, showindex=True, maxcolwidths=[None, 48, 16, 16])
        else:
            return super().report(job, i)

    def cancel(self, job: Job | None = None, i: int | None = None) -> str:
        if job is None:
            jobids = list(self.results.values())
        else:
            jobid = self.results[job]
            if i is not None:  # fails if job is not array
                jobid = f"{jobid}_{i}"
            jobids = [jobid]

        return subprocess.run(
            ["scancel", "-v", *jobids],
            capture_output=True,
            check=True,
            text=True,
        ).stderr.strip("\n")

    async def satisfy(self, job: Job) -> str:
        results = await asyncio.gather(*map(self.submit, job.dependencies))

        for result in results:
            if isinstance(result, Exception):
                raise DependencyNeverSatisfiedError(str(job)) from result

    async def exec(self, job: Job) -> str:
        tag = self.tag(job)
        logfile = self.path / (f"{tag}_%a.log" if isinstance(job, JobArray) else f"{tag}.log")
        pklfile = self.path / f"{tag}.pkl"
        pyfile = self.path / f"{tag}.py"
        shfile = self.path / f"{tag}.sh"

        # Submission script
        lines = [
            f"#!{self.shell}",
            "#",
            f"#SBATCH --job-name={tag}",
        ]

        if isinstance(job, JobArray):
            if job.throttle is None:
                lines.append(f"#SBATCH --array=0-{len(job) - 1}")
            else:
                lines.append(f"#SBATCH --array=0-{len(job) - 1}%{job.throttle}")

        lines.append(f"#SBATCH --output={logfile}")
        lines.append("#")

        ## Settings
        settings = self.settings | job.settings
        settings = {self.translate.get(k, k).replace("_", "-"): v for k, v in settings.items()}

        assert "clusters" not in settings, "multi-cluster jobs not supported"

        if "ntasks" not in settings:
            settings.setdefault("nodes", 1)
            settings.setdefault("ntasks-per-node", 1)

        for key, value in sorted(settings.items()):
            if isinstance(value, bool) and value:
                lines.append(f"#SBATCH --{key}")
            else:
                lines.append(f"#SBATCH --{key}={value}")

        ## Dependencies
        sep = "?" if job.wait_mode == "any" else ","
        after = {
            "success": "afterok",
            "failure": "afternotok",
            "any": "afterany",
        }

        deps = [
            f"{after[status]}:{await self.submit(dep)}" for dep, status in job.dependencies.items()
        ]

        if deps:
            lines.append("#")
            lines.append("#SBATCH --dependency=" + sep.join(deps))

        lines.append("")

        ## Environment
        if self.env:
            lines.extend(self.env)
            lines.append("")

        if job.env:
            lines.extend(job.env)
            lines.append("")

        ## Pickle job
        if isinstance(job, JobArray):
            pklfile = str(pklfile).replace(".pkl", "_{}.pkl")

            for i in range(len(job)):
                with open(pklfile.format(i), mode="wb") as f:
                    f.write(job[i].exe)

            with open(pyfile, mode="w") as f:
                f.write(
                    "\n".join([
                        "#!/usr/bin/env python",
                        "import os",
                        "import pickle",
                        "i = os.environ['SLURM_ARRAY_TASK_ID']",
                        f"with open('{pklfile}'.format(i), 'rb') as f:",
                        "    pickle.load(f)()",
                    ])
                )
        else:
            with open(pklfile, mode="wb") as f:
                f.write(job.exe)

            with open(pyfile, mode="w") as f:
                f.write(
                    "\n".join([
                        "#!/usr/bin/env python",
                        "import pickle",
                        f"with open('{pklfile}', 'rb') as f:",
                        "    pickle.load(f)()",
                    ])
                )

        if job.interpreter is None:
            interpreter = self.interpreter
        else:
            interpreter = job.interpreter

        lines.append(f"srun {interpreter} {pyfile}")
        lines.append("")

        ## Save
        with open(shfile, mode="w") as f:
            f.write("\n".join(lines))

        # Submit script
        try:
            text = subprocess.run(
                ["sbatch", "--parsable", str(shfile)],
                capture_output=True,
                check=True,
                text=True,
            ).stdout

            jobid, *_ = text.strip("\n").split(";")  # ignore cluster name

            return jobid
        except Exception as e:
            if isinstance(e, subprocess.CalledProcessError):
                e = subprocess.SubprocessError(e.stderr.strip("\n"))

            raise JobSubmissionError(str(job)) from e


class CyclicDependencyGraphError(Exception):
    pass


class DependencyNeverSatisfiedError(Exception):
    pass


class JobFailedError(Exception):
    pass


class JobNotFailedError(Exception):
    pass


class JobSubmissionError(Exception):
    pass
