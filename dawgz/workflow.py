r"""Workflow graph components"""

from __future__ import annotations
from functools import cached_property
from typing import *

from .utils import accepts, comma_separated, contextualize, every


class Node(object):
    r"""Abstract graph node"""

    def __init__(self, name: str):
        super().__init__()

        self.name = name
        self.children = {}
        self.parents = {}

    def __repr__(self) -> str:
        return self.name

    def __str__(self) -> str:
        return repr(self)

    def add_child(self, node: Node, edge: Any = None) -> None:
        self.children[node] = edge
        node.parents[self] = edge

    def add_parent(self, node: Node, edge: Any = None) -> None:
        node.add_child(self, edge)

    def rm_child(self, node: Node) -> None:
        del self.children[node]
        del node.parents[self]

    def rm_parent(self, node: Node) -> None:
        node.rm_child(self)


class Job(Node):
    r"""Job node"""

    def __init__(
        self,
        f: Callable,
        name: str = None,
        array: Union[int, Iterable[int]] = None,
        env: List[str] = [],
        settings: Dict[str, Any] = {},
        **kwargs,
    ):
        super().__init__(f.__name__ if name is None else name)

        if array is None:
            assert accepts(f), "job should not expect arguments"
        else:
            if type(array) is int:
                array = range(array)
            array = set(array)

            assert len(array) > 0, "array should not be empty"
            assert accepts(f, 0), "job array should expect one argument"

        self.f = f
        self.array = array

        # Context
        self.context = {}

        # Environment
        self.env = env

        # Settings
        self.settings = settings.copy()
        self.settings.update(kwargs)

        # Dependencies
        self._waitfor = 'all'
        self.unsatisfied = set()

        # Conditions
        self.preconditions = []
        self.postconditions = []

        # Pruning
        self.unsatisfiable = False

    @property
    def fn(self) -> Callable:
        name, f, post = self.name, self.f, self.postcondition

        contextualize(f, **self.context)
        contextualize(post, **self.context)

        def call(*args) -> Any:
            result = f(*args)

            if not post(*args):
                raise PostconditionNotSatisfiedError(f"{name}{list(args) if args else ''}")

            return result

        return call

    def __call__(self, *args) -> Any:
        return self.fn(*args)

    def __repr__(self) -> str:
        if self.array is None:
            return self.name
        else:
            return self.name + '[' + comma_separated(self.array) + ']'

    @property
    def dependencies(self) -> Dict[Job, str]:
        return self.parents

    def after(self, *deps, status: str = 'success') -> None:
        assert status in ['success', 'failure', 'any']

        for dep in deps:
            self.add_parent(dep, status)

    def detach(self, *deps) -> None:
        for dep in deps:
            self.rm_parent(dep)

    @property
    def waitfor(self) -> str:
        return self._waitfor

    @waitfor.setter
    def waitfor(self, mode: str = 'all') -> None:
        assert mode in ['all', 'any']

        self._waitfor = mode

    def ensure(self, condition: Callable) -> None:
        if self.array is None:
            assert accepts(condition), "postcondition should not expect arguments"
        else:
            assert accepts(condition, 0), "postcondition should expect one argument"

        self.postconditions.append(condition)

    @property
    def postcondition(self) -> Callable:
        return every(self.postconditions)

    @cached_property
    def done(self) -> bool:
        if not self.postconditions:
            return False

        if self.array is None:
            return self.postcondition()
        else:
            return all(map(self.postcondition, self.array))

    @property
    def satisfiable(self) -> bool:
        if self.unsatisfied:
            if self.waitfor == 'all':
                return False
            elif self.waitfor == 'any' and not self.dependencies:
                return False

        return True


def dfs(*nodes, backward: bool = False) -> Iterator[Node]:
    queue = list(nodes)
    visited = set()

    while queue:
        node = queue.pop()

        if node in visited:
            continue
        else:
            yield node

        queue.extend(node.parents if backward else node.children)
        visited.add(node)


def leafs(*nodes) -> Set[Node]:
    return {
        node for node in dfs(*nodes, backward=False)
        if not node.children
    }


def roots(*nodes) -> Set[Node]:
    return {
        node for node in dfs(*nodes, backward=True)
        if not node.parents
    }


def cycles(*nodes, backward: bool = False) -> Iterator[List[Node]]:
    queue = [list(nodes)]
    path = []
    pathset = set()
    visited = set()

    while queue:
        branch = queue[-1]

        if not branch:
            if not path:
                break

            queue.pop()
            pathset.remove(path.pop())
            continue

        node = branch.pop()

        if node in visited:
            if node in pathset:
                yield path + [node]
            continue

        queue.append(list(node.parents if backward else node.children))
        path.append(node)
        pathset.add(node)
        visited.add(node)


def prune(*jobs) -> Set[Job]:
    for job in dfs(*jobs, backward=True):
        if job.done:
            job.detach(*job.dependencies)
        elif job.array is not None and job.postconditions:
            job.array = {
                i for i in job.array
                if not job.postcondition(i)
            }

        satisfied, unsatisfied, pending = [], [], []

        for dep, status in job.dependencies.items():
            if dep.done:
                if status == 'failure':  # first-order unsatisfiability
                    unsatisfied.append(dep)
                else:
                    satisfied.append(dep)
            else:
                pending.append(dep)

        job.detach(*satisfied, *unsatisfied)

        if job.waitfor == 'any' and satisfied:
            job.detach(*pending)
            job.unsatisfied.clear()
        else:
            job.unsatisfied.update(unsatisfied)

    return {
        job for job in jobs
        if not job.done
    }


class PostconditionNotSatisfiedError(Exception):
    pass
