r"""Scheduling backends"""

import asyncio
import cloudpickle as pkl
import os
import shutil

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from subprocess import run
from typing import Any, Dict, List

from .utils import to_thread
from .workflow import Job, cycles, prune


def schedule(
    *jobs,
    backend: str = None,
    pruning: bool = True,
    **kwargs,
) -> List[Any]:
    for cycle in cycles(*jobs, backward=True):
        raise CyclicDependencyGraphError(' <- '.join(map(str, cycle)))

    if pruning:
        jobs = prune(*jobs)

    scheduler = {
        'local': LocalScheduler,
        'slurm': SlurmScheduler,
    }.get(backend, LocalScheduler)(**kwargs)

    return asyncio.run(scheduler.gather(*jobs))


class CyclicDependencyGraphError(Exception):
    pass


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


class LocalScheduler(Scheduler):
    r"""Local scheduler"""

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


class DependencyNeverSatisfiedException(Exception):
    pass


class JobNotFailedException(Exception):
    pass


class SlurmScheduler(Scheduler):
    r"""Slurm scheduler"""

    def __init__(
        self,
        name: str = None,
        path: str = '.dawgz',
        shell: str = '/bin/bash',
        env: List[str] = [],  # cd, virtualenv, conda, etc.
        settings: Dict[str, Any] = {},
        **kwargs,
    ):
        super().__init__()

        assert shutil.which('sbatch') is not None, 'sbatch executable not found'

        if name is None:
            name = datetime.now().strftime('%y%m%d_%H%M%S')

        path = Path(path) / name
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
            'timelimit': 'time',
        }

        # Identifier table
        self.table = {}

    def id(self, job: Job) -> str:
        if self.table.get(job.name, job) is job:
            identifier = job.name
        else:
            identifier = str(id(job))

        self.table[identifier] = job

        return identifier

    async def _submit(self, job: Job) -> str:
        # Wait for dependencies to be submitted
        jobids = await asyncio.gather(*[
            self.submit(dep)
            for dep in job.dependencies
        ])

        # Write submission file
        lines = [
            f'#!{self.shell}',
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

        ## Settings
        settings = self.settings.copy()
        settings.update(job.settings)

        for key, value in settings.items():
            key = self.translate.get(key, key)

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

        ## Exit at first error
        lines.extend(['set -e', ''])

        ## Environment
        if job.env:
            lines.extend([*job.env, ''])
        elif self.env:
            lines.extend([*self.env, ''])

        ## Pickle function
        pklfile = self.path / f'{self.id(job)}.pkl'

        with open(pklfile, 'wb') as f:
            f.write(pkl.dumps(job.fn))

        args = '' if job.array is None else '$SLURM_ARRAY_TASK_ID'
        unpickle = f'python -c "import pickle; pickle.load(open(r\'{pklfile}\', \'rb\'))({args})"'

        lines.extend([unpickle, ''])

        ## Save
        bashfile = self.path / f'{self.id(job)}.sh'

        with open(bashfile, 'w') as f:
            f.write('\n'.join(lines))

        # Submit job
        text = run(['sbatch', str(bashfile)], capture_output=True, check=True, text=True).stdout

        for jobid in text.splitlines():
            return jobid
