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

from .utils import comma_separated, print_traces, to_thread
from .workflow import Job, cycles, prune as _prune


class Scheduler(ABC):
    r"""Abstract workflow scheduler"""

    def __init__(self):
        super().__init__()

        self.submissions = {}

    async def gather(self, *jobs) -> Dict[Job, Any]:
        await asyncio.wait(map(self.submit, jobs))

        jobs = self.submissions.keys()
        results = await asyncio.gather(*self.submissions.values())

        return dict(zip(jobs, results))

    async def submit(self, job: Job) -> Any:
        if job in self.submissions:
            task = self.submissions[job]
        else:
            task = self.submissions[job] = asyncio.create_task(self._process(job))

        return await task

    async def _process(self, job: Job) -> Any:
        try:
            if job.unsatisfiable:
                raise DependencyNeverSatisfiedError(f'aborting {job}')

            return await self._submit(job)
        except Exception as e:
            return e

    @abstractmethod
    async def _submit(self, job: Job) -> Any:
        pass


def schedule(
    *jobs,
    backend: str,
    prune: bool = False,
    **kwargs,
) -> Scheduler:
    for cycle in cycles(*jobs, backward=True):
        raise CyclicDependencyGraphError(' <- '.join(map(str, cycle)))

    if prune:
        jobs = _prune(*jobs)

    scheduler = {
        'local': LocalScheduler,
        'slurm': SlurmScheduler,
    }.get(backend)(**kwargs)

    submissions = asyncio.run(scheduler.gather(*jobs))
    errors = filter(lambda x: isinstance(x, Exception), submissions.values())

    print_traces(errors)

    return scheduler


class LocalScheduler(Scheduler):
    r"""Local scheduler"""

    async def condition(self, job: Job, status: str) -> Any:
        result = await self.submit(job)

        if isinstance(result, Exception):
            if isinstance(result, JobFailedError):
                if status == 'success':
                    return result
                else:
                    return None
            else:
                return result
        else:
            if status == 'failure':
                return JobNotFailedError(f'{job}')
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
                        raise DependencyNeverSatisfiedError(f'aborting {job}') from result
                elif job.waitfor == 'any':
                    break
            else:
                continue
            break
        else:
            if job.dependencies and job.waitfor == 'any':
                raise DependencyNeverSatisfiedError(f'aborting {job}')

        # Execute job
        try:
            if job.array is None:
                return await to_thread(job.fn)
            else:
                return await asyncio.gather(*(
                    to_thread(job.fn, i)
                    for i in job.array
                ))
        except Exception as e:
            raise JobFailedError(f'{job}') from e


class SlurmScheduler(Scheduler):
    r"""Slurm scheduler"""

    def __init__(
        self,
        name: str = None,
        path: str = '.dawgz',
        shell: str = None,
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
        self.shell = os.environ['SHELL'] if shell is None else shell
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
        # Wait for all dependencies to be submitted
        jobids = await asyncio.gather(*[
            self.submit(dep)
            for dep in job.dependencies
        ])

        for jobid in jobids:
            if isinstance(jobid, Exception):
                raise DependencyNeverSatisfiedError(f'aborting {job}') from jobid

        # Write submission file
        lines = [
            f'#!{self.shell}',
            '#',
            f'#SBATCH --job-name={job.name}',
        ]

        if job.array is not None:
            lines.append('#SBATCH --array=' + comma_separated(job.array))

        if job.array is None:
            logfile = self.path / f'{self.id(job)}.log'
        else:
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
            f'{types[status]}:{jobid}'
            for status, jobid in zip(job.dependencies.values(), jobids)
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
        try:
            text = run(['sbatch', str(bashfile)], capture_output=True, check=True, text=True).stdout
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
