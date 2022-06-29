r"""Scheduling backends"""

import asyncio
import cloudpickle as pickle
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
from functools import lru_cache
from inspect import isawaitable
from pathlib import Path
from random import random
from tabulate import tabulate
from typing import *

from .utils import comma_separated, eprint, future, runpickle, trace, slugify
from .workflow import Job, cycles, prune as _prune


DIR = os.environ.get('DAWGZ_DIR', '.dawgz')


class Scheduler(ABC):
    r"""Abstract workflow scheduler"""

    backend = None

    def __init__(
        self,
        name: str = None,
        settings: Dict[str, Any] = {},
        **kwargs,
    ):
        super().__init__()

        self.name = name
        self.date = datetime.now().replace(microsecond=0)
        self.uuid = uuid.uuid4().hex

        self.path = Path(DIR).resolve() / self.uuid
        self.path.mkdir(parents=True)

        # Settings
        self.settings = settings.copy()
        self.settings.update(kwargs)

        # Jobs
        self.order = {}
        self.results = {}
        self.traces = {}

    def dump(self) -> None:
        with open(self.path / 'dump.pkl', 'wb') as f:
            pickle.dump(self, f)

        with open(self.path.parent / 'workflows.csv', 'a', newline='') as f:
            csv.writer(f).writerow((
                self.name,
                self.uuid,
                self.date,
                self.backend,
                len(self.order),
                len(self.traces),
            ))

    def tag(self, job: Job) -> str:
        if job in self.order:
            i = self.order[job]
        else:
            i = self.order[job] = len(self.order)

        return f'{i:04d}_{slugify(job.name)}'

    def state(self, job: Job, i: int = None) -> str:
        if job in self.traces:
            return 'FAILED'
        else:
            return 'COMPLETED'

    def output(self, job: Job, i: int = None) -> Any:
        result = self.results[job]

        if job.array is None:
            return result
        elif type(result) is dict:
            return result[i]
        else:
            return result

    @contextmanager
    def context(self) -> None:
        try:
            yield None
        finally:
            pass

    def wait(self, *jobs: Job) -> None:
        with self.context():
            asyncio.run(self._wait(*jobs))

    async def _wait(self, *jobs: Job) -> None:
        if jobs:
            await asyncio.wait(map(self.submit, jobs))
            await asyncio.wait(map(self.submit, self.order))

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
    async def satisfy(self, job: Job) -> None:
        pass

    @abstractmethod
    async def exec(self, job: Job) -> Any:
        pass


def schedule(
    *jobs: Job,
    backend: str,
    prune: bool = False,
    verbose: bool = True,
    **kwargs,
) -> Scheduler:
    for cycle in cycles(*jobs, backward=True):
        raise CyclicDependencyGraphError(' <- '.join(map(str, cycle)))

    if prune:
        jobs = _prune(*jobs)

    backends = {
        s.backend: s
        for s in (
            AsyncScheduler,
            DummyScheduler,
            SlurmScheduler,
        )
    }

    scheduler = backends.get(backend)(**kwargs)
    scheduler.wait(*jobs)
    scheduler.dump()

    if scheduler.traces and verbose:
        eprint(tabulate(scheduler.traces.items(), ('Job', 'Error'), showindex=True))

    return scheduler


class AsyncScheduler(Scheduler):
    r"""Asynchronous scheduler"""

    backend = 'async'

    def __init__(self, pools: int = None, **kwargs):
        super().__init__(**kwargs)

        self.pools = pools

    @contextmanager
    def context(self) -> None:
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

                if isinstance(result, JobFailedError) and status != 'success':
                    result = None
                elif not isinstance(result, Exception) and status == 'failure':
                    result = JobNotFailedError(f'{job}')

                if isinstance(result, Exception):
                    if job.waitfor == 'all':
                        raise DependencyNeverSatisfiedError(str(job)) from result
                elif job.waitfor == 'any':
                    break
            else:
                continue
            break
        else:
            if job.dependencies and job.waitfor == 'any':
                raise DependencyNeverSatisfiedError(str(job))

    async def exec(self, job: Job) -> Any:
        dump = pickle.dumps(job.f)
        call = lambda *args: self.remote(runpickle, dump, *args)

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
    r"""Dummy scheduler"""

    backend = 'dummy'

    async def exec(self, job: Job) -> None:
        print(f"START {job}")
        await asyncio.sleep(random())
        print(f"END   {job}")


class SlurmScheduler(Scheduler):
    r"""Slurm scheduler"""

    backend = 'slurm'
    translate = {
        'cpus': 'cpus-per-task',
        'gpus': 'gpus-per-node',
        'ram': 'mem',
        'memory': 'mem',
        'timelimit': 'time',
    }

    def __init__(
        self,
        shell: str = os.environ.get('SHELL', '/bin/sh'),
        env: List[str] = [],  # cd, virtualenv, conda, etc.
        **kwargs,
    ):
        super().__init__(**kwargs)

        assert shutil.which('sbatch') is not None, "sbatch executable not found"

        # Environment
        self.shell = shell
        self.env = env

    @lru_cache(None)
    def sacct(self, jobid: str) -> Dict[str, str]:
        text = subprocess.run(
            ['sacct', '-j', jobid, '-o', 'JobID,State', '-n', '-P', '-X'],
            capture_output=True,
            check=True,
            text=True,
        ).stdout

        return dict(line.split('|') for line in text.splitlines())

    def state(self, job: Job, i: int = None) -> str:
        if job in self.traces:
            return 'CANCELLED'

        jobid = self.results[job]
        table = self.sacct(jobid)

        if job.array is None:
            return table.get(jobid, None)
        elif i in job.array:
            jobid = f'{jobid}_{i}'

            if jobid in table:
                return table[jobid]

        states = set(table.values())

        if len(states) > 1:
            if i in job.array:
                return 'PENDING'
            else:
                return 'MIXED'
        else:
            return states.pop()

    def output(self, job: Job, i: int = None) -> str:
        tag = self.tag(job)

        if job.array is None:
            logfile = self.path / f'{tag}.log'
        else:
            logfile = self.path / f'{tag}_{i}.log'

        if logfile.exists():
            with open(logfile, newline='') as f:
                text = f.read()

            text = re.sub(r'\r+', r'\r', text)
            text = re.sub(r'.*\r(.+)', r'\1', text)

            return text
        else:
            return None

    async def satisfy(self, job: Job) -> str:
        results = await asyncio.gather(*map(self.submit, job.dependencies))

        for result in results:
            if isinstance(result, Exception):
                raise DependencyNeverSatisfiedError(str(job)) from result

    async def exec(self, job: Job) -> Any:
        # Submission script
        lines = [
            f"#!{self.shell}",
            f"#",
            f"#SBATCH --job-name=\"{job.name}\"",
        ]

        if job.array is not None:
            lines.append("#SBATCH --array=" + comma_separated(job.array))

        tag = self.tag(job)

        if job.array is None:
            logfile = self.path / f'{tag}.log'
        else:
            logfile = self.path / f'{tag}_%a.log'

        lines.append(f"#SBATCH --output={logfile}")

        ## Settings
        settings = self.settings.copy()
        settings.update(job.settings)

        assert 'clusters' not in settings, "multi-cluster operations not supported"

        if settings:
            lines.append("#")

        for key, value in settings.items():
            key = self.translate.get(key, key)

            if type(value) is bool:
                if value:
                    lines.append(f"#SBATCH --{key}")
            else:
                lines.append(f"#SBATCH --{key}={value}")

        ## Dependencies
        sep = '?' if job.waitfor == 'any' else ','
        types = {
            'success': 'afterok',
            'failure': 'afternotok',
            'any': 'afterany',
        }

        deps = [
            f'{types[status]}:{await self.submit(dep)}'
            for dep, status in job.dependencies.items()
        ]

        if deps:
            lines.extend([
                "#",
                "#SBATCH --dependency=" + sep.join(deps),
            ])

        lines.append("")

        ## Environment
        if self.env:
            lines.extend([*self.env, ""])

        ## Pickle job
        pklfile = self.path / f'{tag}.pkl'

        with open(pklfile, 'wb') as f:
            pickle.dump(job.f, f)

        args = '' if job.array is None else '$SLURM_ARRAY_TASK_ID'

        lines.extend([
            f"python << EOC",
            f"import pickle",
            f"with open(r'{pklfile}', 'rb') as f:",
            f"    pickle.load(f)({args})",
            f"EOC",
            f"",
        ])

        ## Save
        shfile = self.path / f'{tag}.sh'

        with open(shfile, 'w') as f:
            f.write('\n'.join(lines))

        # Submit script
        try:
            text = subprocess.run(
                ['sbatch', '--parsable', str(shfile)],
                capture_output=True,
                check=True,
                text=True,
            ).stdout

            jobid, *_ = text.strip('\n').split(';')  # ignore cluster name

            return jobid
        except Exception as e:
            if isinstance(e, subprocess.CalledProcessError):
                e = subprocess.SubprocessError(e.stderr.strip('\n'))

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
