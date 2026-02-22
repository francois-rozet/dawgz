r"""Scheduling backends"""

from __future__ import annotations

import asyncio
import concurrent.futures as cf
import csv
import os
import shutil
import subprocess

from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime
from functools import lru_cache
from inspect import isawaitable
from pathlib import Path
from random import random
from tabulate import tabulate
from typing import Any, Dict, Sequence

from .utils import cat, future, pickle, runpickle, slugify, trace, unique_id
from .workflow import Job, cycles, prune

DIR = os.environ.get("DAWGZ_DIR", ".dawgz")
DIR = Path(DIR).resolve()


class Scheduler(ABC):
    r"""Abstract workflow scheduler."""

    backend: str = None

    def __init__(
        self,
        name: str = None,
        settings: Dict[str, Any] = {},  # noqa: B006
        **kwargs,
    ):
        r"""
        Arguments:
            name: The name of the workflow.
            settings: A dictionnary of settings.
            kwargs: Keyword arguments added to `settings`.
        """

        super().__init__()

        self.name = name
        self.date = datetime.now().replace(microsecond=0)
        self.uid = unique_id()

        self.path = DIR / self.uid
        self.path.mkdir(parents=True)

        # Settings
        self.settings = settings.copy()
        self.settings.update(kwargs)

        # Jobs
        self.order = {}
        self.results = {}
        self.traces = {}

    def dump(self):
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

        return f"{i:04d}_{slugify(job.fun_name)}"

    def state(self, job: Job) -> str:
        if job in self.traces:
            return "FAILED"
        else:
            return "COMPLETED"

    def output(self, job: Job) -> Any:
        return self.results[job]

    def report(self, job: Job = None) -> str:
        if job is None:
            headers = ("Job", "State")
            rows = [(str(job), self.state(job)) for job in self.order]

            return tabulate(rows, headers, showindex=True)
        else:
            headers = ("Job", "State", "Output")

            if job in self.traces:
                output = self.traces[job]
            else:
                output = self.output(job)

            if output is not None:
                output = cat(str(output), width=120)

            rows = [(str(job), self.state(job), output)]

            return tabulate(rows, headers, showindex=[None])

    def cancel(self, job: Job = None) -> str:
        raise NotImplementedError(f"'cancel' is not implemented for the {self.backend} backend.")

    @contextmanager
    def context(self):
        try:
            yield None
        finally:
            pass

    def __call__(self, *jobs: Job):
        for cycle in cycles(*jobs, backward=True):
            raise CyclicDependencyGraphError(" <- ".join(map(str, cycle)))

        jobs = prune(*jobs)

        with self.context():
            asyncio.run(self.wait(*jobs))

    async def wait(self, *jobs: Job):
        if jobs:
            await asyncio.wait(map(asyncio.create_task, map(self.submit, jobs)))
            await asyncio.wait(map(asyncio.create_task, map(self.submit, self.order)))

    async def submit(self, job: Job) -> Any:
        if job in self.results:
            result = self.results[job]
        else:
            result = self.results[job] = future(self._submit(job), return_exceptions=True)

        if isawaitable(result):
            result = self.results[job] = await result

            if isinstance(result, Exception):
                self.traces[job] = trace(result)

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

        return await self.exec(job)

    @abstractmethod
    async def satisfy(self, job: Job):
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

    def __init__(self, name: str = None, pools: int = None, **kwargs):
        r"""
        Arguments:
            name: The name of the workflow.
            pools: The number of processing pools. If `None`, use threads instead.
            kwargs: Keyword arguments passed to :class:`Scheduler`.
        """

        super().__init__(name=name, **kwargs)

        self.pools = pools

    @contextmanager
    def context(self):
        if self.pools is None:
            self.executor = cf.ThreadPoolExecutor()
        else:
            self.executor = cf.ProcessPoolExecutor(self.pools)

        try:
            yield None
        finally:
            del self.executor

    async def satisfy(self, job: Job):
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
                elif not isinstance(result, Exception) and status == "failure":
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
        dump = pickle.dumps(job.run)

        try:
            return await asyncio.get_running_loop().run_in_executor(self.executor, runpickle, dump)
        except Exception as e:
            raise JobFailedError(str(job)) from e


class DummyScheduler(AsyncScheduler):
    r"""Dummy asynchronous scheduler.

    Jobs are scheduled asynchronously, but instead of executing them, their name is
    printed before and after a short (random) sleep time. Useful for debugging.
    """

    backend: str = "dummy"

    async def exec(self, job: Job):
        print(f"START {job}")
        await asyncio.sleep(random())
        print(f"END   {job}")
        return None


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
    translate: Dict[str, str] = {
        "cpus": "cpus-per-task",
        "gpus": "gpus-per-task",
        "ram": "mem",
        "memory": "mem",
        "timelimit": "time",
    }

    def __init__(
        self,
        name: str = None,
        shell: str = os.environ.get("SHELL", "/bin/sh"),
        interpreter: str = "python",
        env: Sequence[str] = (),
        **kwargs,
    ):
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

    @lru_cache(None)  # noqa: B019
    def sacct(self, jobid: str) -> Dict[str, str]:
        text = subprocess.run(
            ["sacct", "-j", jobid, "-o", "JobID,State", "-n", "-P", "-X"],
            capture_output=True,
            check=True,
            text=True,
        ).stdout

        return dict(line.split("|") for line in text.splitlines())

    def state(self, job: Job) -> str:
        if job in self.traces:
            return "CANCELLED"

        jobid = self.results[job]
        table = self.sacct(jobid)

        return table.get(jobid, None)

    def output(self, job: Job) -> str:
        tag = self.tag(job)
        logfile = self.path / f"{tag}.log"

        if logfile.exists():
            with open(logfile, newline="", errors="replace") as f:
                return f.read()
        else:
            return None

    def report(self, job: Job = None) -> str:
        if job is None:
            headers = ("Job", "ID", "State")
            rows = []

            for job in self.order:
                if job in self.traces:
                    jobid = None
                else:
                    jobid = self.results[job]

                rows.append((str(job), jobid, self.state(job)))

            return tabulate(rows, headers, showindex=True)
        else:
            return super().report(job)

    def cancel(self, job: Job = None) -> str:
        if job is None:
            jobids = list(self.results.values())
        else:
            jobid = self.results[job]
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

    async def exec(self, job: Job) -> Any:
        tag = self.tag(job)
        logfile = self.path / f"{tag}.log"
        pklfile = self.path / f"{tag}.pkl"
        pyfile = self.path / f"{tag}.py"
        shfile = self.path / f"{tag}.sh"

        # Submission script
        lines = [
            f"#!{self.shell}",
            "#",
            f"#SBATCH --job-name={tag}",
            f"#SBATCH --output={logfile}",
        ]

        ## Settings
        settings = self.settings.copy()
        settings.update(job.settings)

        assert "clusters" not in settings, "multi-cluster jobs not supported"

        for key in settings:
            assert not key.startswith("ntasks"), "multi-task jobs not supported"

        nodes = settings.pop("nodes", 1)

        lines.append("#")
        lines.append("#SBATCH --nodes=" + f"{nodes}")
        lines.append("#SBATCH --ntasks-per-node=1")

        for key, value in settings.items():
            key = self.translate.get(key, key)

            if type(value) is bool:
                if value:
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

        ## Pickle job
        with open(pklfile, "wb") as f:
            pickle.dump(job.run, f)

        with open(pyfile, "w") as f:
            f.write(
                "\n".join([
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
        with open(shfile, "w") as f:
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
