r"""Schedulers"""

import asyncio
import cloudpickle as pkl
import os

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from subprocess import check_output
from typing import Any

from .workflow import Job


class Scheduler(ABC):
    r"""Abstract workflow scheduler"""

    def __init__(self):
        self.submissions = {}

    def __call__(self, job: Job) -> Any:
        for cycle in job.cycles(backward=True):
            raise CyclicDependencyGraphError(' <- '.join(map(str, cycle)))

        job.prune()

        return asyncio.run(self.submit(job))

    async def submit(self, job: Job) -> Any:
        if job not in self.submissions:
            self.submissions[job] = asyncio.create_task(self._submit(job))

        return await self.submissions[job]

    @abstractmethod
    async def _submit(self, job: Job) -> Any:
        pass


class CyclicDependencyGraphError(Exception):
    pass


def scheduler(backend: None, **kwargs) -> Scheduler:
    return {
        'slurm': Slurm,
    }.get(backend, Default)(**kwargs)


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
                return await asyncio.to_thread(job.__call__)
            else:
                return await asyncio.gather(*[
                    asyncio.to_thread(job.__call__, i)
                    for i in job.array
                ])
        except Exception as error:
            return error


class DependencyNeverSatisfiedException(Exception):
    pass


class JobNotFailedException(Exception):
    pass


class Slurm(Scheduler):
    r"""Slurm scheduler"""

    def __init__(
        self,
        name: str = None,
        path: str = '.dawgz',
        env: list[str] = [],
    ):
        super().__init__()

        if name is None:
            name = datetime.now().strftime('%y%m%d_%H%M%S')

        path = Path(path) / name
        path.mkdir(parents=True, exist_ok=True)

        self.name = name
        self.path = path.resolve()
        self.env = env

    async def _submit(self, job: Job) -> str:
        # Wait for dependencies to be submitted
        jobids = await asyncio.gather(*[
            self.submit(dep)
            for dep in job.dependencies
        ])

        # Pickle job instance
        codefile = self.path / f'{job.name}.pkl'

        with open(codefile, 'wb') as f:
            f.write(pkl.dumps(job))

        # Write submission file
        lines = [
            '#!/usr/bin/env bash',
            '#',
            f'#SBATCH --job-name={job.name}',
        ]

        if job.array is None:
            logfile = codefile.with_suffix('.log')
        else:
            array = job.array

            if type(array) is range:
                lines.append('#SBATCH --array=' + f'{array.start}-{array.stop-1}:{array.step}')
            else:
                lines.append('#SBATCH --array=' + ','.join(map(str, array)))

            logfile = self.path / f'{job.name}_%a.log'

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
        ])

        ## Exit at first error
        lines.extend(['', 'set -o errexit', ''])

        ## Environment
        env = self.env + job.env
        if env:
            lines.extend([*env, ''])

        ## Python
        python = f'python -c "import pickle; pickle.load(open(r\'{codefile}\', \'rb\'))'

        if job.array is None:
            python += '()"'
        else:
            python += '($SLURM_ARRAY_TASK_ID)"'

        lines.extend([python, ''])

        ## File
        bashfile = codefile.with_suffix('.sh')

        with open(bashfile, 'w') as f:
            f.write('\n'.join(lines))

        # Submit job
        lines = check_output(['sbatch', str(bashfile)], text=True)
        for jobid in lines.splitlines():
            return jobid
