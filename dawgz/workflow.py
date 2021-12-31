r"""Workflow graph components"""

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

    def cycles(self, backward: bool = False) -> Iterator[list['Node']]:
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


class Job(Node):
    r"""Job node"""

    def __init__(
        self,
        f: Callable,
        name: str = None,
        array: Union[int, list[int], range] = None,
        env: list[str] = [],
        **kwargs,
    ):
        super().__init__(f.__name__ if name is None else name)

        self.f = f

        if type(array) is int:
            array = range(array)
        self.array = array

        # Environment commands
        self.env = env

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

    def __call__(self, *args, **kwargs) -> Any:
        return self.f(*args, **kwargs)

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
    def dependencies(self) -> dict['Job', str]:
        return self._parents

    def after(self, *deps, status: str = 'success') -> None:
        assert status in ['success', 'failure', 'any']

        for dep in deps:
            self.has_parent(dep, status)

    @property
    def waitfor(self) -> str:
        return self._waitfor

    @waitfor.setter
    def waitfor(self, mode: str = 'all') -> None:
        assert mode in ['all', 'any']

        self._waitfor = mode
