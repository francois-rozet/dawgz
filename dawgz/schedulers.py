r"""Scheduling backends"""

import asyncio
import cloudpickle as pkl
import contextvars
import os

from abc import ABC, abstractmethod
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial
from pathlib import Path
from subprocess import run
from typing import Any, Callable, Dict, List

from .workflow import Job


class Scheduler(ABC):
    r"""Abstract workflow scheduler"""

    def __init__(self):
        self.submissions = {}

    async def gather(self, *jobs) -> List[Any]:
        return await asyncio.gather(*map(self.submit, jobs))

    async def submit(self, job: Job) -> Any:
        if job not in self.submissions:
            self.submissions[job] = asyncio.create_task(self._submit(job))

        return await self.submissions[job]

    @abstractmethod
    async def _submit(self, job: Job) -> Any:
        pass


def schedule(*jobs, backend: str = None, **kwargs) -> List[Any]:
    for cycle in Job.cycles(*jobs, backward=True):
        raise CyclicDependencyGraphError(' <- '.join(map(str, cycle)))

    for job in jobs:
        job.prune()

    scheduler = {
        'default': Default,
        'bash': Bash,
        'slurm': Slurm,
    }.get(backend, Default)(**kwargs)

    return asyncio.run(scheduler.gather(*jobs))


class CyclicDependencyGraphError(Exception):
    pass


class Default(Scheduler):
    r"""Default scheduler"""

    async def condition(self, job: Job, status: str) -> Any:
        result = await self.submit(job)

        if isinstance(result, Exception):
            if status == 'success':
                return result
            else:
                return None
        else:
            if status == 'failure':
                raise JobNotFailedException(f'{job}')
            else:
                return result

    async def _submit(self, job: Job) -> Any:
        # Wait for (all or any) dependencies to complete
        pending = {
            asyncio.create_task(self.condition(dep, status))
            for dep, status in job.dependencies.items()
        }

        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

            for task in done:
                result = task.result()

                if isinstance(result, Exception):
                    if job.waitfor == 'all':
                        raise DependencyNeverSatisfiedException(f'aborting job {job}') from result
                else:
                    if job.waitfor == 'any':
                        break
            else:
                continue
            break
        else:
            if job.dependencies and job.waitfor == 'any':
                raise DependencyNeverSatisfiedException(f'aborting job {job}')

        # Execute job
        try:
            if job.array is None:
                return await to_thread(job.fn)
            else:
                return await asyncio.gather(*(
                    to_thread(job.fn, i)
                    for i in job.array
                ))
        except Exception as error:
            return error


async def to_thread(func: Callable, /, *args, **kwargs) -> Any:
    r"""Asynchronously run function `func` in a separate thread.

    Any *args and **kwargs supplied for this function are directly passed
    to `func`. Also, the current `contextvars.Context` is propagated,
    allowing context variables from the main thread to be accessed in the
    separate thread.

    Return a coroutine that can be awaited to get the eventual result of `func`.

    References:
        https://github.com/python/cpython/blob/main/Lib/asyncio/threads.py
    """

    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    func_call = partial(ctx.run, func, *args, **kwargs)

    return await loop.run_in_executor(None, func_call)


class DependencyNeverSatisfiedException(Exception):
    pass


class JobNotFailedException(Exception):
    pass


class Bash(Scheduler):
    r"""Bash scheduler"""

    def __init__(
        self,
        *jobs,
        name: str = None,
        path: str = '.dawgz',
        env: List[str] = [],
    ):
        super().__init__(*jobs)

        if name is None:
            name = datetime.now().strftime('%y%m%d_%H%M%S')

        path = Path(path) / name
        path.mkdir(parents=True, exist_ok=True)

        self.name = name
        self.path = path.resolve()
        self.env = env

        self.table = {}

    def id(self, job: Job) -> str:
        if self.table.get(job.name, job) is job:
            identifier = job.name
        else:
            identifier = str(id(job))

        self.table[identifier] = job

        return identifier

    def scriptlines(self, job: Job, variables: Dict[str, str] = {}) -> List[str]:
        # Exit at first error
        lines = ['set -e', '']

        # Environment
        if self.env:
            lines.extend([*self.env, ''])

        # Variables
        if variables:
            for key, value in variables.items():
                lines.append(f'{key}={value}')
            lines.append('')

        # Pickle function
        pklfile = self.path / f'{self.id(job)}.pkl'

        with open(pklfile, 'wb') as f:
            f.write(pkl.dumps(job.fn))

        # Unpickle
        args = '' if job.array is None else '$i'
        unpickle = f'python -c "import pickle; pickle.load(open(r\'{pklfile}\', \'rb\'))({args})"'

        lines.extend([unpickle, ''])

        return lines

    async def _submit(self, job: Job) -> str:
        raise NotImplementedError()


class Slurm(Bash):
    r"""Slurm scheduler"""

    async def _submit(self, job: Job) -> str:
        # Wait for dependencies to be submitted
        jobids = await asyncio.gather(*[
            self.submit(dep)
            for dep in job.dependencies
        ])

        # Write submission file
        lines = [
            '#!/usr/bin/env bash',
            '#',
            f'#SBATCH --job-name={job.name}',
        ]

        if job.array is None:
            logfile = self.path / f'{self.id(job)}.log'
        else:
            array = job.array

            if type(array) is range:
                lines.append('#SBATCH --array=' + f'{array.start}-{array.stop-1}:{array.step}')
            else:
                lines.append('#SBATCH --array=' + ','.join(map(str, array)))

            logfile = self.path / f'{self.id(job)}_%a.log'

        lines.extend([f'#SBATCH --output={logfile}', '#'])

        ## Resources
        translate = {
            'cpus': 'cpus-per-task',
            'gpus': 'gpus-per-task',
            'ram': 'mem',
            'time': 'time',
        }

        for key, value in job.settings.items():
            key = translate.get(key, key)

            if value is None:
                lines.append(f'#SBATCH --{key}')
            else:
                lines.append(f'#SBATCH --{key}={value}')

        ## Dependencies
        separator = '?' if job.waitfor == 'any' else ','
        keywords = {
            'success': 'afterok',
            'failure': 'afternotok',
            'any': 'afterany',
        }

        deps = [
            f'{keywords[status]}:{jobid}'
            for jobid, (_, status) in zip(jobids, job.dependencies.items())
        ]

        if deps:
            lines.extend(['#', '#SBATCH --dependency=' + separator.join(deps)])

        ## Convenience
        lines.extend([
            '#',
            '#SBATCH --export=ALL',
            '#SBATCH --parsable',
            '#SBATCH --requeue',
            '',
        ])

        ## Script
        lines.extend(self.scriptlines(job, {'i': '$SLURM_ARRAY_TASK_ID'}))

        ## Save
        bashfile = self.path / f'{self.id(job)}.sh'

        with open(bashfile, 'w') as f:
            f.write('\n'.join(lines))

        # Submit job
        text = run(['sbatch', str(bashfile)], capture_output=True, check=True, text=True).stdout

        for jobid in text.splitlines():
            return jobid
