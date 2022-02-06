r"""Miscellaneous helpers"""

import asyncio
import contextvars
import inspect
import sys
import traceback

from functools import partial, lru_cache
from typing import Any, Callable, Coroutine, Iterable, List


@lru_cache(maxsize=None, typed=True)
def accepts(f: Callable, *args, **kwargs) -> bool:
    r"""Checks whether function `f` accepts the supplied
    *args and **kwargs without errors."""

    try:
        inspect.signature(f).bind(*args, **kwargs)
    except TypeError as e:
        return False
    else:
        return True


async def awaitable(x: Any) -> Any:
    r"""Transforms any object to an awaitable."""

    if inspect.isawaitable(x):
        return await x
    else:
        return x


async def catch(coroutine: Coroutine) -> Any:
    r"""Catches possible exceptions in coroutine."""

    return (await asyncio.gather(coroutine, return_exceptions=True))[0]


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


def eprint(*args, **kwargs) -> None:
    r"""Prints to the standard error stream."""

    print(*args, file=sys.stderr, **kwargs)


def every(conditions: List[Callable]) -> Callable:
    r"""Combines a list of conditions into a single condition."""

    return lambda *args: all(c(*args) for c in conditions)


async def gather(*args, **kwargs) -> List[Any]:
    r"""Replica of `asyncio.gather` that accepts non-awaitable arguments."""

    return await asyncio.gather(*map(awaitable, args), **kwargs)


async def to_thread(f: Callable, /, *args, **kwargs) -> Any:
    r"""Asynchronously runs function `f` in a separate thread.

    Any *args and **kwargs supplied for this function are directly passed
    to `f`. Also, the current `contextvars.Context` is propagated,
    allowing context variables from the main thread to be accessed in the
    separate thread.

    Returns a coroutine that can be awaited to get the eventual result of `f`.

    References:
        https://github.com/python/cpython/blob/main/Lib/asyncio/threads.py
    """

    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    func_call = partial(ctx.run, f, *args, **kwargs)

    return await loop.run_in_executor(None, func_call)


def trace(error: Exception) -> str:
    r"""Returns the trace of an error."""

    lines = traceback.format_exception(
        type(error),
        error,
        error.__traceback__,
    )

    return ''.join(lines).strip('\n')
