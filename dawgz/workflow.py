r"""Workflow graph components"""

from __future__ import annotations

from functools import cached_property
from typing import Any, Callable, Dict, Iterable, Iterator, List, Set, Union

# isort: split
from .utils import accepts, comma_separated, every, pickle


class Node(object):
    r"""Abstract graph node"""

    def __init__(self):
        super().__init__()

        self.children = {}
        self.parents = {}

    def add_child(self, node: Node, edge: Any = None):
        self.children[node] = edge
        node.parents[self] = edge

    def add_parent(self, node: Node, edge: Any = None):
        node.add_child(self, edge)

    def rm_child(self, node: Node):
        del self.children[node]
        del node.parents[self]

    def rm_parent(self, node: Node):
        node.rm_child(self)


class Job(Node):
    r"""Job node"""

    def __init__(
        self,
        f: Callable,
        *,
        name: str = None,
        array: Union[int, Iterable[int]] = None,
        array_throttle: int = None,
        settings: Dict[str, Any] = {},  # noqa: B006
        **kwargs,
    ):
        super().__init__()

        assert callable(f), "job should be callable"

        if array is None:
            assert accepts(f), "job should not expect arguments"
        else:
            if type(array) is int:
                array = range(array)
            array = set(array)

            assert len(array) > 0, "array should not be empty"
            assert accepts(f, 0), "job array should expect an argument"

        self._f = pickle.dumps(f)
        self.name = f.__name__ if name is None else name
        self.array = array
        self.array_throttle = array_throttle

        # Settings
        self.settings = settings.copy()
        self.settings.update(kwargs)

        # Dependencies
        self._waitfor = "all"
        self.unsatisfied = set()

        # Conditions
        self._postconditions = []

    def __getstate__(self) -> Dict:
        state = self.__dict__.copy()

        for key in ["_f", "_postconditions"]:
            state.pop(key, None)

        return state

    @property
    def f(self) -> Callable:
        return pickle.loads(self._f)

    @property
    def run(self) -> Callable:
        name = self.name
        f = self.f
        cond = every(self.postconditions)

        def fun(*args) -> Any:
            result = f(*args)

            if not cond(*args):
                raise PostconditionNotSatisfiedError(f"{name}{list(args) if args else ''}")

            return result

        return fun

    def __call__(self, *args) -> Any:
        return self.run(*args)

    def __str__(self) -> str:
        if self.array is None:
            return self.name
        else:
            return self.name + "[" + comma_separated(self.array) + "]"

    @property
    def dependencies(self) -> Dict[Job, str]:
        return self.parents

    def after(self, *deps: Job, status: str = "success"):
        assert status in ["success", "failure", "any"]

        for dep in deps:
            self.add_parent(dep, status)

    def detach(self, *deps: Job):
        for dep in deps:
            self.rm_parent(dep)

    @property
    def waitfor(self) -> str:
        return self._waitfor

    @waitfor.setter
    def waitfor(self, mode: str = "all"):
        assert mode in ["all", "any"]

        self._waitfor = mode

    def ensure(self, condition: Callable):
        if self.array is None:
            assert accepts(condition), "postcondition should not expect arguments"
        else:
            assert accepts(condition, 0), "postcondition should expect an argument"

        self._postconditions.append(pickle.dumps(condition))

    @property
    def postconditions(self) -> List[Callable]:
        return list(map(pickle.loads, self._postconditions))

    @cached_property
    def done(self) -> bool:
        if not self.postconditions:
            return False

        condition = every(self.postconditions)

        if self.array is None:
            return condition()
        else:
            return all(map(condition, self.array))

    @property
    def satisfiable(self) -> bool:
        if self.unsatisfied:
            if self.waitfor == "all":
                return False
            elif self.waitfor == "any" and not self.dependencies:
                return False

        return True


def dfs(*nodes: Node, backward: bool = False) -> Iterator[Node]:
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


def leafs(*nodes: Node) -> Set[Node]:
    return {node for node in dfs(*nodes, backward=False) if not node.children}


def roots(*nodes: Node) -> Set[Node]:
    return {node for node in dfs(*nodes, backward=True) if not node.parents}


def cycles(*nodes: Node, backward: bool = False) -> Iterator[List[Node]]:
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


def prune(*jobs: Job) -> Set[Job]:
    for job in dfs(*jobs, backward=True):
        if job.done:
            job.detach(*job.dependencies)
        elif job.array is not None and job.postconditions:
            condition = every(job.postconditions)
            job.array = {i for i in job.array if not condition(i)}

        satisfied, unsatisfied, pending = [], [], []

        for dep, status in job.dependencies.items():
            if dep.done:
                if status == "failure":  # first-order unsatisfiability
                    unsatisfied.append(dep)
                else:
                    satisfied.append(dep)
            else:
                pending.append(dep)

        job.detach(*satisfied, *unsatisfied)

        if job.waitfor == "any" and satisfied:
            job.detach(*pending)
            job.unsatisfied.clear()
        else:
            job.unsatisfied.update(unsatisfied)

    return {job for job in jobs if not job.done}


class PostconditionNotSatisfiedError(Exception):
    pass
