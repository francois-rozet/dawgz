r"""Miscellaneous helpers"""

import asyncio
import contextvars
import traceback

from functools import partial, lru_cache
from inspect import signature
from typing import Any, Callable, Coroutine, Iterable


@lru_cache(maxsize=None, typed=True)
def accepts(f: Callable, *args, **kwargs) -> bool:
    r"""Checks whether function `f` accepts the supplied
    *args and **kwargs without errors."""

    try:
        signature(f).bind(*args, **kwargs)
    except TypeError as e:
        return False
    else:
        return True


async def catch(coroutine: Coroutine) -> Any:
    r"""Catches possible exceptions in coroutine."""

    try:
        return await coroutine
    except Exception as e:
        return e


def comma_separated(integers: Iterable[int]) -> str:
    r"""Formats integers as a comma separated list of intervals."""

    integers = sorted(list(integers))
    intervals = []

    i = j = integers[0]

    for k in integers[1:]:
        if  k > j + 1:
            intervals.append((i, j))
            i = j = k
        else:
            j = k
    else:
        intervals.append((i, j))

    fmt = lambda i, j: f'{i}' if i == j else f'{i}-{j}'

    return ','.join(map(fmt, *zip(*intervals)))


def every(conditions: List[Callable]) -> Callable:
    r"""Combines a list of conditions into a single condition."""

    return lambda *args: all(c(*args) for c in conditions)


def print_traces(errors: Iterable[Exception]) -> None:
    r"""Prints the traces of a sequence of exceptions,
    delimited by dashed lines."""

    traces = []

    for e in errors:
        try:
            raise e
        except:
            traces.append(traceback.format_exc())

    if traces:
        sep = '-' * 80 + '\n'
        print(sep + sep.join(traces) + sep, end='')


async def to_thread(f: Callable, /, *args, **kwargs) -> Any:
    r"""Asynchronously run function `f` in a separate thread.

    Any *args and **kwargs supplied for this function are directly passed
    to `f`. Also, the current `contextvars.Context` is propagated,
    allowing context variables from the main thread to be accessed in the
    separate thread.

    Return a coroutine that can be awaited to get the eventual result of `f`.

    References:
        https://github.com/python/cpython/blob/main/Lib/asyncio/threads.py
    """

    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    func_call = partial(ctx.run, f, *args, **kwargs)

    return await loop.run_in_executor(None, func_call)
