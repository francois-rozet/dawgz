r"""Miscellaneous helpers"""

import asyncio
import cloudpickle as pickle
import inspect
import sys
import traceback

from types import FunctionType
from typing import *


def accepts(f: Callable, /, *args, **kwargs) -> bool:
    r"""Checks whether a function `f` accepts arguments without errors."""

    try:
        inspect.signature(f).bind(*args, **kwargs)
    except TypeError as e:
        return False
    else:
        return True


def comma_separated(integers: Iterable[int]) -> str:
    r"""Formats integers as a comma separated list of intervals."""

    integers = sorted(list(integers))
    intervals = []

    i = j = integers[0]

    for k in integers[1:]:
        if k > j + 1:
            intervals.append((i, j))
            i = j = k
        else:
            j = k
    else:
        intervals.append((i, j))

    fmt = lambda i, j: f'{i}' if i == j else f'{i}-{j}'

    return ','.join(map(fmt, *zip(*intervals)))


def contextualize(f: FunctionType, /, **context) -> FunctionType:
    r"""Contextualizes a function."""

    f.__globals__.update(context)

    for i, var in enumerate(f.__code__.co_freevars):
        if var in context:
            f.__closure__[i].cell_contents = context[var]

    return f


def eprint(*args, **kwargs) -> None:
    r"""Prints to the standard error stream."""

    print(*args, file=sys.stderr, **kwargs)


def every(conditions: List[Callable]) -> Callable:
    r"""Combines a list of conditions into a single condition."""

    return lambda *args: all(c(*args) for c in conditions)


def future(obj: Any, return_exceptions: bool = False) -> asyncio.Future:
    r"""Transforms any object to an awaitable future."""

    if inspect.isawaitable(obj):
        if return_exceptions:
            fut = asyncio.Future()

            def callback(self):
                result = self.exception()
                if result is None:
                    result = self.result()

                fut.set_result(result)

            asyncio.ensure_future(obj).add_done_callback(callback)
        else:
            fut = asyncio.ensure_future(obj)
    else:
        fut = asyncio.Future()
        fut.set_result(obj)

    return fut


def runpickle(f: bytes, /, *args, **kwargs) -> Any:
    r"""Runs a pickled function `f` with arguments."""

    return pickle.loads(f)(*args, **kwargs)


def trace(error: Exception) -> str:
    r"""Returns the trace of an error."""

    lines = traceback.format_exception(
        type(error),
        error,
        error.__traceback__,
    )

    return ''.join(lines).strip('\n')
