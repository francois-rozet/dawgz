r"""Miscellaneous helpers"""

import asyncio
import contextvars

from functools import partial
from typing import Any, Callable


async def to_thread(func: Callable, /, *args, **kwargs) -> Any:
    r"""Asynchronously run function `func` in a separate thread.

    Any *args and **kwargs supplied for this function are directly passed
    to `func`. Also, the current `contextvars.Context` is propagated,
    allowing context variables from the main thread to be accessed in the
    separate thread.

    Return a coroutine that can be awaited to get the eventual result of `func`.

    References:
        https://github.com/python/cpython/blob/main/Lib/asyncio/threads.py
    """

    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    func_call = partial(ctx.run, func, *args, **kwargs)

    return await loop.run_in_executor(None, func_call)
