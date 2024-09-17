r"""Scheduling backends"""

from __future__ import annotations

import asyncio
import concurrent.futures as cf
import csv
import os
import re
import shutil
import subprocess
import uuid

from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime
from functools import lru_cache, partial
from inspect import isawaitable
from pathlib import Path
from random import random
from tabulate import tabulate
from typing import Any, Callable, Dict, Sequence

# isort: split
from .utils import comma_separated, future, pickle, runpickle, slugify, trace, wrap
from .workflow import Job, cycles
from .workflow import prune as _prune

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
        self.uuid = uuid.uuid4().hex

        self.path = DIR / self.uuid
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
                self.uuid,
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

    def state(self, job: Job, i: int = None) -> str:
        if job in self.traces:
            return "FAILED"
        else:
            return "COMPLETED"

    def output(self, job: Job, i: int = None) -> Any:
        if job.array is None:
            return self.results[job]
        else:
            return self.results[job].get(i)

    def report(self, job: Job = None) -> str:
        if job is None:
            headers = ("Name", "State")
            rows = [(str(job), self.state(job)) for job in self.order]

            return tabulate(rows, headers, showindex=True)
        else:
            headers = ("Name", "State", "Output")
            array = [None]

            if job in self.traces:
                rows = [(str(job), self.state(job), self.traces[job])]
            elif job.array is None:
                rows = [(str(job), self.state(job), self.output(job))]
            else:
                array = sorted(job.array)
                rows = [
                    (f"{job.name}[{i}]", self.state(job, i), self.output(job, i)) for i in array
                ]

            rows = [
                (
                    name,
                    state,
                    None if output is None else wrap(output, width=120),
                )
                for name, state, output in rows
            ]

            return tabulate(rows, headers, showindex=array)

    def cancel(self, job: Job = None) -> str:
        raise NotImplementedError(f"'cancel' is not implemented for the {self.backend} backend.")

    @contextmanager
    def context(self):
        try:
            yield None
        finally:
            pass

    def __call__(self, *jobs: Job, prune: bool = False):
        for cycle in cycles(*jobs, backward=True):
            raise CyclicDependencyGraphError(" <- ".join(map(str, cycle)))

        if prune:
            jobs = _prune(*jobs)

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
            if job.satisfiable:
                await self.satisfy(job)
            else:
                raise DependencyNeverSatisfiedError(str(job))
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
                    result = JobNotFailedError(f"{job}")

                if isinstance(result, Exception):
                    if job.waitfor == "all":
                        raise DependencyNeverSatisfiedError(str(job)) from result
                elif job.waitfor == "any":
                    break
            else:
                continue
            break
        else:
            if job.dependencies and job.waitfor == "any":
                raise DependencyNeverSatisfiedError(str(job))

    async def exec(self, job: Job) -> Any:
        dump = pickle.dumps(job.run)
        call = partial(self.remote, runpickle, dump)

        try:
            if job.array is None:
                return await call()
            else:
                results = await asyncio.gather(*map(call, job.array), return_exceptions=True)

                for result in results:
                    if isinstance(result, Exception):
                        raise result

                return dict(zip(job.array, results))
        except Exception as e:
            raise JobFailedError(str(job)) from e

    async def remote(self, f: Callable, /, *args) -> Any:
        return await asyncio.get_running_loop().run_in_executor(self.executor, f, *args)


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

        return None if job.array is None else {}


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
        env: Sequence[str] = [],  # noqa: B006
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

    def state(self, job: Job, i: int = None) -> str:
        if job in self.traces:
            return "CANCELLED"

        jobid = self.results[job]
        table = self.sacct(jobid)

        if job.array is None:
            return table.get(jobid, None)
        elif i in job.array:
            jobid = f"{jobid}_{i}"

            if jobid in table:
                return table[jobid]

        states = set(table.values())

        if len(states) > 1:
            if i in job.array:
                return "PENDING"
            else:
                return "MIXED"
        else:
            return states.pop()

    def output(self, job: Job, i: int = None) -> str:
        tag = self.tag(job)

        if job.array is None:
            logfile = self.path / f"{tag}.log"
        else:
            logfile = self.path / f"{tag}_{i}.log"

        if logfile.exists():
            with open(logfile, newline="") as f:
                text = f.read()

            text = re.sub(r"\r+", r"\r", text)
            text = re.sub(r".*\r(.+)", r"\1", text)

            return text
        else:
            return None

    def report(self, job: Job = None) -> str:
        if job is None:
            headers = ("Name", "ID", "State")
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
        # Submission script
        lines = [
            f"#!{self.shell}",
            "#",
            f'#SBATCH --job-name="{job.name}"',
        ]

        if job.array is not None:
            indices = comma_separated(job.array)

            if job.array_throttle is None:
                lines.append(f"#SBATCH --array={indices}")
            else:
                lines.append(f"#SBATCH --array={indices}%{job.array_throttle}")

        tag = self.tag(job)

        if job.array is None:
            logfile = self.path / f"{tag}.log"
        else:
            logfile = self.path / f"{tag}_%a.log"

        lines.append(f"#SBATCH --output={logfile}")

        ## Settings
        settings = self.settings.copy()
        settings.update(job.settings)

        assert "clusters" not in settings, "multi-cluster jobs not supported"

        for key in settings:
            assert not key.startswith("ntasks"), "multi-task jobs not supported"

        lines.append("#")
        lines.append("#SBATCH --ntasks=1")

        for key, value in settings.items():
            key = self.translate.get(key, key)

            if type(value) is bool:
                if value:
                    lines.append(f"#SBATCH --{key}")
            else:
                lines.append(f"#SBATCH --{key}={value}")

        ## Dependencies
        sep = "?" if job.waitfor == "any" else ","
        types = {
            "success": "afterok",
            "failure": "afternotok",
            "any": "afterany",
        }

        deps = [
            f"{types[status]}:{await self.submit(dep)}" for dep, status in job.dependencies.items()
        ]

        if deps:
            lines.append("#")
            lines.append("#SBATCH --dependency=" + sep.join(deps))

        lines.append("")

        ## Environment
        if self.env:
            lines.extend([*self.env, ""])

        ## Pickle job
        pklfile = self.path / f"{tag}.pkl"

        with open(pklfile, "wb") as f:
            pickle.dump(job.run, f)

        pyfile = self.path / f"{tag}.py"

        with open(pyfile, "w") as f:
            f.write(
                "\n".join([
                    "import argparse",
                    "import pickle",
                    "",
                    "parser = argparse.ArgumentParser()",
                    "parser.add_argument('-i', '--index', type=int, default=None)",
                    "",
                    "args = parser.parse_args()",
                    "",
                    "with open('{}', 'rb') as f:".format(pklfile),
                    "    if args.index is None:",
                    "        pickle.load(f)()",
                    "    else:",
                    "        pickle.load(f)(args.index)",
                    "",
                ])
            )

        if job.array is None:
            lines.append(f"srun {self.interpreter} {pyfile}")
        else:
            lines.append(f"srun {self.interpreter} {pyfile} -i $SLURM_ARRAY_TASK_ID")

        lines.append("")

        ## Save
        shfile = self.path / f"{tag}.sh"

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
