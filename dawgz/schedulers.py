r"""Scheduling backends"""

import asyncio
import cloudpickle as pkl
import os
import shutil

from abc import ABC, abstractmethod
from datetime import datetime
from functools import partial
from pathlib import Path
from random import random
from subprocess import run
from typing import Any, Dict, List

from .utils import awaitable, catch, comma_separated, eprint, gather, to_thread, trace
from .workflow import Job, cycles, prune as _prune


class Scheduler(ABC):
    r"""Abstract workflow scheduler"""

    def __init__(self):
        super().__init__()

        self.table = {}
        self.submissions = {}

    def tag(self, job: Job) -> str:
        if job in self.table:
            i = self.table[job]
        else:
            i = self.table[job] = len(self.table)

        return f'{job.name}_{i:03d}'

    @property
    def results(self) -> Dict[Job, Any]:
        return {
            job: task.result()
            for job, task in self.submissions.items()
        }

    @property
    def errors(self) -> Dict[Job, Exception]:
        return {
            job: result
            for job, result in self.results.items()
            if isinstance(result, Exception)
        }

    async def wait(self, *jobs) -> None:
        await asyncio.wait(map(self.submit, jobs))
        await asyncio.wait(self.submissions.values())

    async def submit(self, job: Job) -> Any:
        if job in self.submissions:
            task = self.submissions[job]
        else:
            task = self.submissions[job] = asyncio.create_task(catch(self._submit(job)))

        return await task

    async def _submit(self, job: Job) -> Any:
        if job.satisfiable:
            await self.satisfy(job)
        else:
            raise DependencyNeverSatisfiedError(f'aborting {job}')

        _ = self.tag(job)

        return await self.exec(job)

    @abstractmethod
    async def satisfy(self, job: Job) -> None:
        pass

    @abstractmethod
    async def exec(self, job: Job) -> Any:
        pass


def schedule(
    *jobs,
    backend: str,
    prune: bool = False,
    warn: bool = True,
    **kwargs,
) -> Scheduler:
    for cycle in cycles(*jobs, backward=True):
        raise CyclicDependencyGraphError(' <- '.join(map(str, cycle)))

    if prune:
        jobs = _prune(*jobs)

    scheduler = {
        'async': AsyncScheduler,
        'dummy': DummyScheduler,
        'slurm': SlurmScheduler,
    }.get(backend)(**kwargs)

    asyncio.run(scheduler.wait(*jobs))

    if warn:
        traces = list(map(trace, scheduler.errors.values()))
        if traces:
            traces.insert(0, 'DAWGZWarning: errors occurred while scheduling')

            length = max(
                len(line)
                for trce in traces
                for line in trce.splitlines()
            )

            sep = '\n' + '-' * length + '\n'
            text = sep.join(traces)

            eprint(text, end=sep)

    return scheduler


class AsyncScheduler(Scheduler):
    r"""Asynchronous scheduler"""

    async def satisfy(self, job: Job) -> None:
        pending = {
            asyncio.create_task(gather(self.submit(dep), status))
            for dep, status in job.dependencies.items()
        }

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
                        raise DependencyNeverSatisfiedError(f'aborting {job}') from result
                elif job.waitfor == 'any':
                    break
            else:
                continue
            break
        else:
            if job.dependencies and job.waitfor == 'any':
                raise DependencyNeverSatisfiedError(f'aborting {job}')

    async def exec(self, job: Job) -> Any:
        try:
            if job.array is None:
                return await to_thread(job.fn)
            else:
                return await gather(*map(partial(to_thread, job.fn), job.array))
        except Exception as e:
            raise JobFailedError(f'{job}') from e


class DummyScheduler(AsyncScheduler):
    r"""Dummy scheduler"""

    async def exec(self, job: Job) -> None:
        print(f'START {job}')
        await asyncio.sleep(random())
        print(f'END   {job}')


class SlurmScheduler(Scheduler):
    r"""Slurm scheduler"""

    def __init__(
        self,
        name: str = None,
        path: str = '.dawgz',
        shell: str = os.environ.get('SHELL', '/bin/sh'),
        env: List[str] = [],  # cd, virtualenv, conda, etc.
        settings: Dict[str, Any] = {},
        **kwargs,
    ):
        super().__init__()

        assert shutil.which('sbatch') is not None, 'sbatch executable not found'

        if name is None:
            name = datetime.now().strftime('%y%m%d_%H%M%S')

        path = Path(path) / name
        assert not path.exists(), f'{path} already exists'
        path.mkdir(parents=True, exist_ok=True)

        self.name = name
        self.path = path.resolve()

        # Environment
        self.shell = shell
        self.env = env

        # Settings
        self.settings = settings.copy()
        self.settings.update(kwargs)

        self.translate = {
            'cpus': 'cpus-per-task',
            'gpus': 'gpus-per-task',
            'ram': 'mem',
            'memory': 'mem',
            'timelimit': 'time',
        }

    async def satisfy(self, job: Job) -> str:
        results = await gather(*map(self.submit, job.dependencies))

        for result in results:
            if isinstance(result, Exception):
                raise DependencyNeverSatisfiedError(f'aborting {job}') from result

    async def exec(self, job: Job) -> Any:
        # Submission script
        lines = [
            f'#!{self.shell}',
            '#',
            f'#SBATCH --job-name={job.name}',
        ]

        if job.array is not None:
            lines.append('#SBATCH --array=' + comma_separated(job.array))

        if job.array is None:
            logfile = self.path / f'{self.tag(job)}.log'
        else:
            logfile = self.path / f'{self.tag(job)}_%a.log'

        lines.extend([f'#SBATCH --output={logfile}', '#'])

        ## Settings
        settings = self.settings.copy()
        settings.update(job.settings)

        for key, value in settings.items():
            key = self.translate.get(key, key)

            if value is None:
                lines.append(f'#SBATCH --{key}')
            else:
                lines.append(f'#SBATCH --{key}={value}')

        if settings:
            lines.append('#')

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
            lines.extend(['#SBATCH --dependency=' + sep.join(deps), '#'])

        ## Convenience
        lines.extend([
            '#SBATCH --export=ALL',
            '#SBATCH --parsable',
            '#SBATCH --requeue',
            '',
        ])

        ## Environment
        if job.env:
            lines.extend([*job.env, ''])
        elif self.env:
            lines.extend([*self.env, ''])

        ## Pickle job
        pklfile = self.path / f'{self.tag(job)}.pkl'

        with open(pklfile, 'wb') as f:
            f.write(pkl.dumps(job.fn))

        args = '' if job.array is None else '$SLURM_ARRAY_TASK_ID'
        unpickle = [
            'python << EOC',
            'import pickle',
            f'with open(r\'{pklfile}\', \'rb\') as file:',
            f'    pickle.load(file)({args})',
            'EOC',
        ]

        lines.extend([*unpickle, ''])

        ## Save
        shfile = self.path / f'{self.tag(job)}.sh'

        with open(shfile, 'w') as f:
            f.write('\n'.join(lines))

        # Submit script
        try:
            text = run(['sbatch', str(shfile)], capture_output=True, check=True, text=True).stdout
            jobid, *_ = text.splitlines()

            return jobid
        except Exception as e:
            raise JobSubmissionError(f'could not submit {job}') from e


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
