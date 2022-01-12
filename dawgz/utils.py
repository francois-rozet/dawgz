r"""Miscellaneous helpers"""

import asyncio
import contextvars

from functools import partial, lru_cache
from inspect import signature
from typing import Any, Callable


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


@lru_cache(typed=True)
def accepts(f: Callable, *args, **kwargs) -> bool:
    r"""Checks whether function `f` accepts the supplied
    *args and **kwargs without errors."""

    try:
        signature(f).bind(*args, **kwargs)
    except TypeError as e:
        return False
    else:
        return True
