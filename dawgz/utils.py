r"""Miscellaneous helpers"""

import asyncio
import cloudpickle as pickle
import inspect
import mmap
import re
import struct
import sys
import traceback
import uuid

from typing import IO, Any, overload
from wonderwords import RandomWord

BYTES_HEADER = b"BYTES_LIST"
BYTES_U64 = struct.Struct("<Q")


def bytes_dump(file: IO[bytes], items: list[bytes]) -> None:
    r"""Writes a list of bytes to a file."""

    file.write(BYTES_HEADER)
    file.write(BYTES_U64.pack(len(items)))
    file.write(b"".join(map(BYTES_U64.pack, map(len, items))))
    file.write(b"".join(items))


@overload
def bytes_load(file: IO[bytes], i: int) -> bytes: ...


@overload
def bytes_load(file: IO[bytes], i: None) -> list[bytes]: ...


def bytes_load(file: IO[bytes], i: int | None = None) -> bytes | list[bytes]:
    r"""Reads a bytes list from disk."""

    with mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as buffer:
        offset = len(BYTES_HEADER)

        if buffer[:offset] != BYTES_HEADER:
            raise ValueError("Unknown file format.")

        length = BYTES_U64.unpack_from(buffer, offset)[0]
        offset += BYTES_U64.size

        sizes = [None] * length
        for j in range(length):
            sizes[j] = BYTES_U64.unpack_from(buffer, offset)[0]
            offset += BYTES_U64.size

        offsets = [offset + sum(sizes[:j]) for j in range(length)]

        if i is None:
            return [buffer[offsets[j] : offsets[j] + sizes[j]] for j in range(length)]
        else:
            return buffer[offsets[i] : offsets[i] + sizes[i]]


def cat(text: str, width: int) -> str:
    r"""Formats text as it would be displayed in a terminal."""

    lines = []

    for line in text.split("\n"):
        s = ""

        for carriage in reversed(line.split("\r")):
            if len(carriage) > len(s):
                s = s + carriage[len(s) :]

        line = s

        if line and width > 0:
            for i in range(0, len(line), width):
                lines.append(line[i : i + width])
        else:
            lines.append(line)  # keep empty lines

    return "\n".join(lines)


def eprint(*args, **kwargs) -> None:
    r"""Prints to the standard error stream."""

    print(*args, file=sys.stderr, **kwargs)


def future(obj: Any, return_exceptions: bool = False) -> asyncio.Future:
    r"""Transforms any object to an awaitable future."""

    if inspect.isawaitable(obj):
        if return_exceptions:
            fut = asyncio.Future()

            def callback(self: Any) -> None:
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


def human_uuid() -> str:
    r"""Returns a human-readable UUID."""

    adjective = RandomWord().word(
        word_min_length=6,
        word_max_length=8,
        include_categories=["adjectives"],
        exclude_with_spaces=True,
    )

    noun = RandomWord().word(
        word_min_length=14 - len(adjective),
        word_max_length=14 - len(adjective),
        include_categories=["nouns"],
        exclude_with_spaces=True,
    )

    hex = uuid.uuid4().hex[:8]

    return f"{adjective}_{noun}_{hex}"


def runpickle(f: bytes, /, *args, **kwargs) -> Any:
    r"""Runs a pickled function `f` with arguments."""

    return pickle.loads(f)(*args, **kwargs)


def slugify(text: str) -> str:
    r"""Slugifies text."""

    slug = "".join(char if char.isalnum() else "_" for char in text)
    slug = "_".join(slug.split("_"))

    return slug


def trace(error: Exception) -> str:
    r"""Returns the trace of an error."""

    lines = traceback.format_exception(
        type(error),
        error,
        error.__traceback__,
    )

    lines = [line for line in lines if not re.search(r"futures\/\w+\.py", line)]

    return "".join(lines).strip("\n")
