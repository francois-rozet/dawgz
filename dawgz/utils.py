r"""Miscellaneous helpers"""

import asyncio
import contextvars
import traceback

from functools import partial, lru_cache
from inspect import signature
from typing import Any, Callable, Iterable


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
