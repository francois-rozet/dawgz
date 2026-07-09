r"""Abstract scheduler and shared helpers"""

from __future__ import annotations

import asyncio
import csv
import re
import rich.box
import rich.console
import rich.highlighter
import rich.style
import rich.syntax
import rich.table
import rich.text

from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from ..constants import get_dawgz_dir
from ..utils import cat, future, human_uuid, pickle, slugify, trace
from ..workflow import Job, JobArray, cycles, prune


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
        self.traces: dict[Job, str] = {}
        self.results: dict[Job, Any] = {}

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
            if "JobNeverSatisfiedError" in self.traces[job]:
                return "CANCELLED"
            else:
                return "FAILED"
        elif job in self.results:
            return "COMPLETED"
        else:
            return "UNKNOWN"

    def logs(self, job: Job, i: int | None = None) -> str | None:
        tag = self.tag(job)

        if isinstance(job, JobArray):
            logfile = self.path / f"{tag}_{i}.log"
        else:
            logfile = self.path / f"{tag}.log"

        if logfile.exists():
            with open(logfile, newline="", errors="replace") as f:
                return cat(f.read(), -1).strip("\n")
        elif job in self.traces:
            return self.traces[job].strip("\n")
        else:
            return None

    def settings(self, job: Job, i: int | None = None) -> str | None:
        return None

    def lookup(
        self,
        job: Job,
        i: int | None = None,
        *,
        entry: Literal["source", "settings", "input", "state", "logs"] = "logs",
    ) -> rich.console.RenderableType:
        if entry == "source":
            return rich.syntax.Syntax(
                getattr(job if i is None else job[i], "source", ""),
                lexer="python",
                theme=ANSITheme(),
                dedent=True,
            )
        elif entry == "settings":
            return self.settings(job, i)
        elif entry == "input":
            return repr(job if i is None else job[i])
        elif entry == "state":
            return StateHighlighter()(self.state(job, i))
        elif entry == "logs":
            return rich.text.Text(self.logs(job, i) or "")
        else:
            raise NotImplementedError(f"Unknown entry '{entry}'.")

    def report(
        self,
        job: Job | int | None = None,
        i: int | None = None,
        *,
        entry: Literal["source", "settings", "input", "logs"] = "logs",
        raw: bool = False,
    ) -> list[rich.console.RenderableType]:
        table = rich.table.Table(box=rich.box.ROUNDED)
        table.add_column("", justify="right", no_wrap=True, min_width=2)
        table.add_column("Job", justify="left", no_wrap=True)
        table.add_column("State", justify="left", no_wrap=True)

        if job is None:
            for job, i in self.order.items():  # noqa: PLR1704
                table.add_row(str(i), str(job), self.lookup(job, entry="state"))
        else:
            table.add_column(entry.capitalize(), justify="left", no_wrap=False)

            if isinstance(job, int):
                job = list(self.order)[job]

            if isinstance(job, JobArray):
                if i is None:
                    indices = range(len(job))
                else:
                    indices = [i % len(job)]

                for j in indices:
                    table.add_row(
                        str(j),
                        str(job[j]),
                        self.lookup(job, j, entry="state"),
                        self.lookup(job, j, entry=entry),
                    )
                    table.add_section()
            else:
                table.add_row(
                    str(self.order[job]),
                    str(job),
                    self.lookup(job, entry="state"),
                    self.lookup(job, entry=entry),
                )

            if raw:
                return list(table.columns[3].cells)

        return [table]

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
                raise JobNeverSatisfiedError(repr(job))
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


class CyclicDependencyGraphError(Exception):
    pass


class JobNeverSatisfiedError(Exception):
    pass


class JobFailedError(Exception):
    pass


class JobNotFailedError(Exception):
    pass


class JobSubmissionError(Exception):
    pass


class StateHighlighter(rich.highlighter.Highlighter):
    STYLES = {
        "PENDING": "dim",
        "RUNNING": "cyan",
        "COMPLETED": "green",
        "FAILED": "red",
        "CANCELLED": "dark_orange",
        "UNKNOWN": "magenta",
    }

    def highlight(self, text: rich.text.Text) -> None:
        for match in re.finditer(r"\w+", text.plain):
            state, i, j = match.group(), match.start(), match.end()

            if state in self.STYLES:
                text.stylize(self.STYLES[state], i, j)


class ANSITheme(rich.syntax.ANSISyntaxTheme):
    def __init__(self) -> None:
        super().__init__({
            token: style + rich.style.Style(bold=False)
            for token, style in rich.syntax.ANSI_DARK.items()
        })
