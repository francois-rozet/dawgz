r"""Workflow graph components"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from functools import partial
from typing import (
    Any,
    Literal,
    TypeVar,
)

from .utils import pickle


class Node:
    r"""Abstract graph node"""

    def __init__(self) -> None:
        self.children = {}
        self.parents = {}

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
    r"""Job node."""

    def __init__(
        self,
        fun: Callable,
        /,
        args: Sequence[Any] = (),
        kwargs: dict[str, Any] = {},  # noqa: B006
        *,
        name: str | None = None,
        interpreter: str | None = None,
        settings: dict[str, Any] = {},  # noqa: B006
    ) -> None:
        super().__init__()

        self.dump = pickle.dumps((fun, args, kwargs))

        # String repr
        if name is None:
            name = fun.__name__
        else:
            assert name.replace("_", "").isalnum(), (
                f"function name can only contain underscore and alphanumeric characters, got '{name}'"
            )

        self.fun_name = name
        self.args_repr = [f"{a}" for a in args] + [f"{k}={v}" for k, v in kwargs.items()]

        # Settings
        self.interpreter = interpreter
        self.settings = settings

        # Status
        self.status: str = "pending"

        # Dependencies
        self.wait_mode: str = "all"
        self.satisfied: dict[Job, str] = {}
        self.unsatisfied: dict[Job, str] = {}

    def __repr__(self) -> str:
        job_repr = f"{self.fun_name}(" + ", ".join(self.args_repr) + ")"

        if len(job_repr) > 28:
            return job_repr[:24] + " ..."
        else:
            return job_repr

    def __getstate__(self) -> dict:
        state = self.__dict__.copy()
        state.pop("dump")
        return state

    @property
    def run(self) -> Callable[[], Any]:
        fun, args, kwargs = pickle.loads(self.dump)
        return partial(fun, *args, **kwargs)

    def mark(self, status: Literal["success", "failure", "cancelled", "pending"]) -> Job:
        r"""Sets the completion status of a job.

        Arguments:
            status: The completion status. The default status is `"pending"`.
        """
        assert status in ["success", "failure", "cancelled", "pending"]
        self.status = status
        return self

    @property
    def dependencies(self) -> dict[Job, str]:
        return self.parents

    def after(self, *deps: Job, status: Literal["success", "failure", "any"] = "success") -> Job:
        r"""Adds dependencies to a job.

        Arguments:
            deps: A set of job dependencies.
            status: The desired dependency status.
        """
        assert status in ["success", "failure", "any"]
        for dep in deps:
            self.add_parent(dep, status)
        return self

    def detach(self, *deps: Job) -> None:
        for dep in deps:
            self.rm_parent(dep)

    def waitfor(self, mode: Literal["all", "any"]) -> Job:
        r"""Sets the waiting mode of a job.

        Arguments:
            mode: The dependency waiting mode. The default mode is `"all"`.
        """
        assert mode in ["all", "any"]
        self.wait_mode = mode
        return self

    @property
    def satisfy_status(self) -> Literal["ready", "never", "wait"]:
        if self.wait_mode == "all" and self.unsatisfied:
            return "never"
        elif self.wait_mode == "all" and not self.dependencies:
            return "ready"
        elif self.wait_mode == "any" and self.satisfied:
            return "ready"
        elif self.wait_mode == "any" and not self.dependencies:
            return "never"
        else:
            return "wait"


N = TypeVar("N", bound=Node)


def dfs(*nodes: N, backward: bool = False) -> Iterator[N]:
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


def leafs(*nodes: N) -> set[N]:
    return {node for node in dfs(*nodes, backward=False) if not node.children}


def roots(*nodes: N) -> set[N]:
    return {node for node in dfs(*nodes, backward=True) if not node.parents}


def cycles(*nodes: Node, backward: bool = False) -> Iterator[list[Node]]:
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


def prune(*jobs: Job) -> set[Job]:
    for job in dfs(*jobs, backward=True):
        if job.status != "pending":
            job.detach(*job.dependencies)

        for dep, status in job.dependencies.items():
            if dep.status == "pending":
                pass
            elif dep.status == "cancelled":
                job.unsatisfied[dep] = status
            elif status == "any" or dep.status == status:
                job.satisfied[dep] = status
            else:
                job.unsatisfied[dep] = status

        job.detach(*job.satisfied, *job.unsatisfied)

    return {job for job in jobs if job.status == "pending"}
