r"""Scheduling backends"""

from __future__ import annotations

import asyncio
import concurrent.futures as cf
import csv
import random
import re
import rich.box
import rich.highlighter
import rich.style
import rich.syntax
import rich.table
import rich.text
import subprocess

from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from functools import cache
from pathlib import Path
from typing import Any, Literal

from .constants import get_dawgz_dir
from .utils import (
    bytes_dump,
    cat,
    future,
    human_uuid,
    pickle,
    runpickle,
    slugify,
    trace,
)
from .workflow import Job, JobArray, cycles, prune


class StateHighlighter(rich.highlighter.Highlighter):
    def highlight(self, text: rich.text.Text) -> None:
        for match in re.finditer(r"\w+", text.plain):
            state, i, j = match.group(), match.start(), match.end()

            if state == "PENDING":
                text.stylize("dim", i, j)
            elif state == "RUNNING":
                text.stylize("cyan", i, j)
            elif state == "COMPLETED":
                text.stylize("green", i, j)
            elif state == "FAILED":
                text.stylize("red", i, j)
            elif state == "CANCELLED":
                text.stylize("dark_orange", i, j)
            else:
                pass


class ANSITheme(rich.syntax.ANSISyntaxTheme):
    def __init__(self) -> None:
        super().__init__({
            token: style + rich.style.Style(bold=False)
            for token, style in rich.syntax.ANSI_DARK.items()
        })


class Scheduler(ABC):
    r"""Abstract workflow scheduler."""

    backend: str = None

    def __init__(self, name: str) -> None:
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

        # Jobs
        self.order: dict[Job, int] = {}
        self.results: dict[Job, Any] = {}
        self.traces: dict[Job, str] = {}

    def dump(self) -> None:
        with open(self.path / "dump.pkl", mode="wb") as f:
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
        with open(path / "dump.pkl", mode="rb") as f:
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

    def output(self, job: Job, i: int | None = None) -> str | None:
        if job in self.traces:
            return self.traces[job]
        elif job in self.results:
            if isinstance(job, JobArray):
                result = self.results[job][i]
            else:
                result = self.results[job]

            return None if result is None else str(result)
        else:
            return None

    def settings(self, job: Job, i: int | None = None) -> str | None:
        return None

    def report(
        self,
        job: Job | int | None = None,
        i: int | None = None,
        *,
        entry: Literal["source", "settings", "input", "output"] = "output",
    ) -> rich.table.Table:
        table = rich.table.Table(box=rich.box.ROUNDED)
        table.add_column("", justify="right", no_wrap=True, min_width=2)
        table.add_column("Job", justify="left", no_wrap=True)
        table.add_column("State", justify="left", no_wrap=True)

        hl = StateHighlighter()

        if job is None:
            for job, i in self.order.items():  # noqa: PLR1704
                table.add_row(str(i), str(job), hl(self.state(job)))
        else:
            table.add_column(entry.capitalize(), justify="left", no_wrap=False)

            if entry == "source":
                getter = lambda job, i: rich.syntax.Syntax(
                    getattr(job if i is None else job[i], "source", ""),
                    lexer="python",
                    theme=ANSITheme(),
                    dedent=True,
                )
            elif entry == "settings":
                getter = self.settings
            elif entry == "input":
                getter = lambda job, i: rich.syntax.Syntax(
                    repr(job if i is None else job[i]),
                    lexer="python",
                    theme=ANSITheme(),
                )
            elif entry == "output":
                getter = self.output
            else:
                raise NotImplementedError(f"Unknown entry '{entry}'.")

            if isinstance(job, int):
                job = list(self.order)[job]

            if isinstance(job, JobArray):
                if i is None:
                    indices = range(len(job))
                else:
                    indices = [i % len(job)]

                for j in indices:
                    table.add_row(str(j), str(job[j]), hl(self.state(job, j)), getter(job, j))
                    table.add_section()
            else:
                table.add_row(
                    str(self.order[job]), str(job), hl(self.state(job)), getter(job, None)
                )

        return table

    def cancel(self, job: Job | int | None = None, i: int | None = None) -> str:
        raise NotImplementedError(f"'cancel' is not implemented for the '{self.backend}' backend.")

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
                result = await future(self.satisfy(job), return_exceptions=True)
                if isinstance(result, Exception):
                    return result
            elif job.satisfy_status == "never":
                raise DependencyNeverSatisfiedError(repr(job))
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

    def __init__(self, name: str, pools: int | None = None) -> None:
        r"""
        Arguments:
            name: The name of the workflow.
            pools: The number of processing pools. If `None`, use threads instead.
        """

        super().__init__(name=name)

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
                    result = JobNotFailedError(repr(job))

                if isinstance(result, Exception):
                    if job.wait_mode == "all":
                        raise DependencyNeverSatisfiedError(repr(job)) from result
                elif job.wait_mode == "any":
                    break
            else:
                continue
            break
        else:
            if job.wait_mode == "any":
                raise DependencyNeverSatisfiedError(repr(job))

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

    def __init__(self, name: str) -> None:
        r"""
        Arguments:
            name: The name of the workflow.
        """

        super().__init__(name=name)

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
                return cat(f.read(), -1).strip("\n")
        else:
            return None

    def settings(self, job: Job, i: int | None = None) -> rich.syntax.Syntax | None:
        tag = self.tag(job)
        shfile = self.path / f"{tag}.sh"

        if shfile.exists():
            return rich.syntax.Syntax(
                shfile.read_text(encoding="utf-8").strip("\n"),
                lexer="sh",
                theme=ANSITheme(),
            )
        else:
            return None

    def report(
        self, job: Job | int | None = None, i: int | None = None, **kwargs
    ) -> rich.table.Table:
        if job is None:
            table = rich.table.Table(box=rich.box.ROUNDED)
            table.add_column("", justify="right", no_wrap=True, min_width=2)
            table.add_column("Job", justify="left", no_wrap=True)
            table.add_column("State", justify="left", no_wrap=True)
            table.add_column("ID", justify="right", no_wrap=True)

            hl = StateHighlighter()

            for job, i in self.order.items():  # noqa: PLR1704
                if job in self.traces:
                    jobid = None
                else:
                    jobid = self.results[job]

                table.add_row(str(i), str(job), hl(self.state(job)), jobid)
        else:
            table = super().report(job, i, **kwargs)

        return table

    def cancel(self, job: Job | int | None = None, i: int | None = None) -> str:
        if job is None:
            jobids = list(self.results.values())
        else:
            if isinstance(job, int):
                job = list(self.order)[job]
            jobid = self.results[job]
            if i is not None:  # noop if job is not array
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
                raise DependencyNeverSatisfiedError(repr(job)) from result

    async def exec(self, job: Job) -> str:
        tag = self.tag(job)
        logfile = self.path / (f"{tag}_%a.log" if isinstance(job, JobArray) else f"{tag}.log")
        pklfile = self.path / f"{tag}.pkl"
        pyfile = self.path / f"{tag}.py"
        shfile = self.path / f"{tag}.sh"

        # Submission script
        lines = [
            f"#!{job.shell}",
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
        settings = {self.translate.get(k, k).replace("_", "-"): v for k, v in job.settings.items()}

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
        if job.env:
            lines.extend(job.env)
            lines.append("")

        ## Pickle job
        if isinstance(job, JobArray):
            pklfile = str(pklfile).replace(".pkl", ".pkls")

            with open(pklfile, mode="wb") as f:
                bytes_dump(f, [job[i].exe for i in range(len(job))])

            with open(pyfile, mode="w") as f:
                f.write(
                    "\n".join([
                        "#!/usr/bin/env python",
                        "import os",
                        "import pickle",
                        "from dawgz.utils import bytes_load",
                        "i = os.environ['SLURM_ARRAY_TASK_ID']",
                        f"with open('{pklfile}', mode='rb') as f:",
                        "    pickle.loads(bytes_load(f, int(i)))()",
                        "",
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
                        f"with open('{pklfile}', mode='rb') as f:",
                        "    pickle.load(f)()",
                        "",
                    ])
                )

        lines.append(f"srun {job.interpreter} {pyfile}")
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

            raise JobSubmissionError(repr(job)) from e


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
