r"""Workflow graph components"""

from functools import cached_property
from typing import Any, Callable, Dict, Iterator, List, Set, Tuple, Union


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

    def add_child(self, node: 'Node', edge: Any = None) -> None:
        self._children[node] = edge
        node._parents[self] = edge

    def add_parent(self, node: 'Node', edge: Any = None) -> None:
        node.add_child(self, edge)

    def rm_child(self, node: 'Node') -> None:
        del self._children[node]

    def rm_parent(self, node: 'Node') -> None:
        node.rm_child(self)

    @property
    def children(self) -> List['Node']:
        return list(self._children)

    @property
    def parents(self) -> List['Node']:
        return list(self._parents)

    @staticmethod
    def dfs(*roots, backward: bool = False) -> Set['Node']:
        queue = list(roots)
        visited = set()

        while queue:
            node = queue.pop()

            if node in visited:
                continue

            queue.extend(node.parents if backward else node.children)
            visited.add(node)

        return visited

    @staticmethod
    def cycles(*roots, backward: bool = False) -> Iterator[List['Node']]:
        queue = [list(roots)]
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

            queue.append(node.parents if backward else node.children)
            path.append(node)
            pathset.add(node)
            visited.add(node)


class Job(Node):
    r"""Job node"""

    def __init__(
        self,
        f: Callable,
        name: str = None,
        array: Union[int, Set[int], range] = None,
        **kwargs,
    ):
        super().__init__(f.__name__ if name is None else name)

        self.f = f

        if type(array) is int:
            array = range(array)
        self.array = array

        # Settings
        self.settings = {
            'cpus': 1,
            'gpus': 0,
            'ram': '2GB',
            'time': '1-00:00:00',
        }
        self.settings.update(kwargs)

        # Dependencies
        self._waitfor = 'all'

        # Postcondition
        self.postcondition = None
        self.pruned = False

    @property
    def fn(self) -> Callable:
        name, f, post = self.name, self.f, self.postcondition

        if self.done:
            f = lambda *args: None

        def call(*args) -> Any:
            result = f(*args)

            if post is not None:
                assert post(*args), f'job {name} does not satisfy its postcondition'

            return result

        return call

    def __call__(self, *args) -> Any:
        return self.fn(*args)

    def __repr__(self) -> str:
        if self.array is not None:
            array = self.array

            if type(array) is range:
                array = f'[{array.start}:{array.stop}:{array.step}]'
            else:
                array = '[' + ','.join(map(str, array)) + ']'
        else:
            array = ''

        return self.name + array

    @property
    def dependencies(self) -> Dict['Job', str]:
        return self._parents

    def after(self, *deps, status: str = 'success') -> None:
        assert status in ['success', 'failure', 'any']

        for dep in deps:
            self.add_parent(dep, status)

    @property
    def waitfor(self) -> str:
        return self._waitfor

    @waitfor.setter
    def waitfor(self, mode: str = 'all') -> None:
        assert mode in ['all', 'any']

        self._waitfor = mode

    def ensure(self, condition: Callable) -> None:
        self.postcondition = condition

    @cached_property
    def done(self) -> bool:
        if self.postcondition is not None:
            if self.array is None:
                return self.postcondition()
            else:
                return all(map(self.postcondition, self.array))

        return False

    def prune(self) -> None:
        if self.pruned:
            return
        self.pruned = True

        done = {
            dep for dep, status in self.dependencies.items()
            if status == 'success' and dep.done
        }

        if self.waitfor == 'any' and done:
            for dep in self.dependencies:
                self.rm_parent(dep)
        elif self.waitfor == 'all':
            for dep in done:
                self.rm_parent(dep)

        for dep in self.dependencies:
            dep.prune()

        if self.array is not None and self.postcondition is not None:
            pending = {
                i for i in self.array
                if not self.postcondition(i)
            }

            if len(pending) < len(self.array):
                self.array = pending
