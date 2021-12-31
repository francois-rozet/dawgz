r"""Directed Acyclic Workflow Graph Scheduling"""

__version__ = '0.0.1'


import asyncio

from functools import cached_property, partial
from typing import Any, Callable, Iterator, Union


class Node(object):
    r"""Abstract graph node"""

    def __init__(self, name: str):
        super().__init__()

        self.name = name

        self._children = {}
        self._parents = {}

    def __repr__(self) -> str:
        return self.name

    def __str__(self) -> str:
        return repr(self)

    def has_child(self, node: 'Node', edge: Any = None) -> None:
        self._children[node] = edge
        node._parents[self] = edge

    def has_parent(self, node: 'Node', edge: Any = None) -> None:
        node.has_child(self, edge)

    @property
    def children(self) -> list['Node']:
        return list(self._children)

    @property
    def parents(self) -> list['Node']:
        return list(self._parents)

    def cycles(self, backward: bool = True) -> Iterator[list['Node']]:
        path = [self]
        tree = {self: self.parents if backward else self.children}

        while path:
            branches = tree[path[-1]]

            if len(branches) > 0:
                node = branches.pop()
            else:
                tree[path.pop()] = None
                continue

            if node in tree:  # node has been visited
                if tree[node] is not None:  # node is in current path
                    yield path + [node]
                continue

            path.append(node)
            tree[node] = node.parents if backward else node.children

    def dfs(self, backward: bool = False) -> Iterator['Node']:
        queue = [self]
        visited = set()

        while queue:
            node = queue.pop()

            if node in visited:
                continue

            visited.add(node)
            queue.extend(node.parents if backward else node.children)

            yield node


def job(f: Callable = None, /, **kwargs):
    if f is None:
        return partial(job, **kwargs)
    return Job(f, **kwargs)


class Job(Node):
    r"""Job node"""

    def __init__(
        self,
        f: Callable,
        name: str = None,
        array: Union[int, list[int], range] = None,
        cpus: int = 1,
        gpus: int = 0,
        ram: str = '2GB',
        time: str = '1-00:00:00',
        env: list[str] = [],
        **kwargs,
    ):
        super().__init__(f.__name__ if name is None else name)

        self.f = f

        if type(array) is int:
            array = range(array)
        self.array = array

        # Slurm settings
        self.settings = kwargs.copy()
        self.settings['job-name'] = name
        self.settings['cpus-per-task'] = f'{cpus}'
        self.settings['gpus-per-task'] = f'{gpus}'
        self.settings['mem'] = ram
        self.settings['time'] = time

        if type(array) is range:
            self.settings['array'] = f'{array.start}-{array.stop-1}:{array.step}'
        elif type(array) is list:
            self.settings['array'] = ','.join(map(str, array))

        # Environment
        self.environment = env

        # Dependencies
        self._wait = 'all'

        # Backend
        self._backend = None

    def __repr__(self) -> str:
        if 'array' in self.settings:
            return self.name + '[' + self.settings['array'] + ']'
        return self.name

    @property
    def dependencies(self):
        return self._parents

    def after(self, job: 'Job', condition: str = 'success'):
        assert not self.frozen
        assert condition in ['success', 'failure', 'any']

        self.has_parent(job, condition)

    @property
    def waitfor(self) -> str:
        return self._wait

    @waitfor.setter
    def waitfor(self, mode: str = 'all') -> None:
        assert not self.frozen
        assert mode in ['all', 'any']

        self._wait = mode

    @property
    def backend(self) -> str:
        return self._backend

    @backend.setter
    def backend(self, backend: str) -> None:
        assert not self.frozen
        assert backend in [None, 'slurm']

        if self._backend == backend:
            return

        self._backend = backend

        for job in self.dependencies:
            job.backend = backend

    @property
    def frozen(self):
        return 'task' in self.__dict__

    @cached_property
    def task(self) -> asyncio.Task:
        return asyncio.create_task(run(self))

    async def _routine(self) -> Any:
        return await self.task

    def __call__(self) -> Any:
        for node in self.dfs(backward=True):
            if self.backend != node.backend:
                raise DependencyBackendConflictError(f'jobs {self} and {node} have different backends')

        for cycle in self.cycles(backward=True):
            raise CyclicDependencyGraphError(' <- '.join(map(str, cycle)))

        return asyncio.run(self._routine())


async def transform(job: Job, condition: str = 'success') -> Any:
    try:
        result = await job.task
    except Exception as e:
        if condition == 'success':
            raise e
        else:
            return None
    else:
        if condition == 'failure':
            raise JobNotFailedException(f'{job}')
        else:
            return result


async def run(self: Job) -> Any:
    pending = {
        asyncio.create_task(transform(job, condition))
        for job, condition in self.dependencies.items()
    }

    while pending:
        done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

        for task in done:
            try:
                task.result()
            except Exception as e:
                if self.waitfor == 'all':
                    raise DependencyNeverSatisfiedException(f'aborting job {self}') from e
            else:
                if self.waitfor == 'any':
                    break
        else:
            continue
        break
    else:
        if self.waitfor == 'any':
            raise DependencyNeverSatisfiedException(f'aborting job {self}')

    if self.array is None:
        return await asyncio.to_thread(self.f)

    return await asyncio.gather(*[
        asyncio.to_thread(self.f, i)
        for i in self.array
    ])


class DependencyNeverSatisfiedException(Exception):
    pass


class JobNotFailedException(Exception):
    pass


class DependencyBackendConflictError(Exception):
    pass


class CyclicDependencyGraphError(Exception):
    pass
