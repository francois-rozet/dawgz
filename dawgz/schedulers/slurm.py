r"""Slurm scheduling backend"""

from __future__ import annotations

import asyncio
import rich.box
import rich.syntax
import rich.table
import subprocess
import time

from pathlib import Path

from .core import (
    ANSITheme,
    JobNeverSatisfiedError,
    JobSubmissionError,
    Scheduler,
)
from ..workflow import Job, JobArray

SACCT_CACHE: dict[str, tuple[float, dict[str, str]]] = {}
SACCT_TTL: float = 5.0  # seconds


class SlurmScheduler(Scheduler):
    r"""Slurm scheduler.

    Jobs are submitted to the Slurm queue. Resources are allocated by the Slurm manager
    according to the job settings. Most settings (e.g. `account`, `export`, `partition`)
    are passed directly to `sbatch`. A few settings (e.g. `cpus`, `gpus`, `ram`) are
    translated into their `sbatch` equivalents.
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

    @staticmethod
    def sacct(jobid: str) -> dict[str, str]:
        now = time.monotonic()
        then, states = SACCT_CACHE.get(jobid, (float("-inf"), None))

        if now < then + SACCT_TTL:
            return states

        text = subprocess.run(
            ["sacct", "-j", jobid, "-o", "JobID,State", "-n", "-P", "-X"],
            capture_output=True,
            check=True,
            text=True,
        ).stdout.strip("\n")

        states = dict(line.split("|") for line in text.splitlines())

        SACCT_CACHE[jobid] = (now, states)

        return states

    def state(self, job: Job, i: int | None = None) -> str:
        if job in self.traces:
            return "CANCELLED"

        jobid = self.results[job]
        table = self.sacct(jobid)

        if isinstance(job, JobArray):
            if i is None:
                return ",".join(sorted(set(table.values())))
            else:
                return table.get(f"{jobid}_{i}", "PENDING")
        else:
            return table.get(jobid, "UNKNOWN")

    def settings(self, job: Job, i: int | None = None) -> rich.syntax.Syntax | None:
        tag = self.tag(job)
        shfile = self.path / f"{tag}.sh"

        if shfile.exists():
            return rich.syntax.Syntax(
                shfile.read_text().strip("\n"),
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

            for job, i in self.order.items():  # noqa: PLR1704
                if job in self.traces:
                    jobid = None
                else:
                    jobid = self.results[job]

                table.add_row(str(i), str(job), self.lookup(job, entry="state"), jobid)
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
                raise JobNeverSatisfiedError(repr(job)) from result

    async def exec(self, job: Job) -> str:
        loop = asyncio.get_event_loop()

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

        ## Interpreter
        lines.append(f"srun {job.interpreter} {pyfile}")
        lines.append("")

        await loop.run_in_executor(None, shfile.write_text, "\n".join(lines))

        # Pickle files
        if isinstance(job, JobArray):
            pklfile = str(pklfile).replace(".pkl", "_{}.pkl")

            await asyncio.wait([
                loop.run_in_executor(None, Path(pklfile.format(i)).write_bytes, job[i].pkl)
                for i in range(len(job))
            ])

            pycode = [
                "#!/usr/bin/env python",
                "import os",
                "import pickle",
                "i = os.environ['SLURM_ARRAY_TASK_ID']",
                f"with open('{pklfile}'.format(i), 'rb') as f:",
                "    pickle.load(f)()",
                "",
            ]
        else:
            await loop.run_in_executor(None, pklfile.write_bytes, job.pkl)

            pycode = [
                "#!/usr/bin/env python",
                "import pickle",
                f"with open('{pklfile}', 'rb') as f:",
                "    pickle.load(f)()",
                "",
            ]

        await loop.run_in_executor(None, pyfile.write_text, "\n".join(pycode))

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
