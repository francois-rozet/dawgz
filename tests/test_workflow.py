"""Tests for dawgz.workflow."""

import pytest

from enum import IntEnum
from pathlib import Path
from typing import Any

from dawgz.workflow import Job, JobArray

SCALARS = (bool, bytes, float, int, str, type(None))
CONTAINERS = (dict, frozenset, list, set, tuple)


def assert_builtin(x: Any, path: str, seen: set[int]) -> None:
    r"""Asserts that `x` is a built-in scalar, or a built-in container thereof.

    Types are compared exactly, as subclasses (e.g. `Path`, `IntEnum`) are not built-in and
    may not survive serialization. Jobs are allowed, as they are the nodes of the graph, and
    are inspected recursively.
    """

    if isinstance(x, Job):
        assert_state_builtin(x, path=path, seen=seen)
    elif type(x) in SCALARS:
        pass
    elif type(x) in CONTAINERS:
        if isinstance(x, dict):
            for key, value in x.items():
                assert_builtin(key, path=f"{path}[{key!s}:key]", seen=seen)
                assert_builtin(value, path=f"{path}[{key!s}]", seen=seen)
        else:
            for i, element in enumerate(x):
                assert_builtin(element, path=f"{path}[{i}]", seen=seen)
    elif isinstance(x, (*SCALARS, *CONTAINERS)):
        raise AssertionError(f"{path} is a '{type(x).__name__}' instance, not a built-in type")
    else:
        raise AssertionError(f"{path} is not built-in, got '{type(x).__name__}'")


def assert_state_builtin(job: Job, path: str = "job", seen: set[int] | None = None) -> None:
    r"""Asserts that all elements of a job's state are built-in.

    The graph is cyclic (parents and children reference each other), so jobs are visited once.
    """

    if seen is None:
        seen = set()

    if id(job) in seen:
        return
    else:
        seen.add(id(job))

    for key, value in job.__getstate__().items():
        assert_builtin(value, path=f"{path}.{key}", seen=seen)


########
# Jobs #
########


def hello(x: object = None) -> None:
    print(x)


############
# Fixtures #
############


class Text(str):
    r"""A `str` subclass, i.e. not a built-in type."""


class Number(int):
    r"""An `int` subclass, i.e. not a built-in type."""


class Partition(IntEnum):
    r"""An `IntEnum`, i.e. not a built-in type."""

    GPU = 8


#########
# Tests #
#########


def test_defaults() -> None:
    job = Job(hello)

    assert job.shell == "/bin/bash"
    assert job.interpreter == "python"
    assert job.env == []
    assert job.settings == {}

    assert_state_builtin(job)


def test_settings_types() -> None:
    job = Job(
        hello,
        settings={"cpus": 4, "ram": "16GB", "time": 3.14, "exclusive": True},
    )

    assert job.settings == {"cpus": 4, "ram": "16GB", "time": 3.14, "exclusive": True}

    assert_state_builtin(job)


@pytest.mark.parametrize(
    "shell, interpreter",
    [
        ("/bin/sh", "python"),
        (Path("/bin/sh"), Path("python")),
        (Text("/bin/sh"), Text("python")),
    ],
)
def test_shell_interpreter_cast(shell: Any, interpreter: Any) -> None:
    job = Job(hello, shell=shell, interpreter=interpreter)

    assert job.shell == "/bin/sh"
    assert job.interpreter == "python"

    assert_state_builtin(job)


def test_env_cast() -> None:
    job = Job(hello, env=[Text("module load python"), Path("/opt/setup.sh")])

    assert job.env == ["module load python", "/opt/setup.sh"]

    assert_state_builtin(job)


def test_settings_enum_cast() -> None:
    job = Job(hello, settings={"cpus": Partition.GPU})

    assert job.settings == {"cpus": 8}
    assert type(job.settings["cpus"]) is int

    assert_state_builtin(job)


def test_settings_bool_not_cast_to_int() -> None:
    job = Job(hello, settings={"exclusive": True})

    assert job.settings == {"exclusive": True}
    assert type(job.settings["exclusive"]) is bool

    assert_state_builtin(job)


def test_settings_key_cast() -> None:
    job = Job(hello, settings={Text("ram"): "16GB"})

    assert type(next(iter(job.settings))) is str

    assert_state_builtin(job)


def test_settings_cast() -> None:
    job = Job(
        hello,
        settings={
            "cpus": Number(4),
            "ram": Text("16GB"),
            "logdir": Path("/tmp/logs"),
        },
    )

    assert job.settings == {"cpus": 4, "ram": "16GB", "logdir": "/tmp/logs"}
    assert type(job.settings["cpus"]) is int

    assert_state_builtin(job)


def test_args_repr() -> None:
    job = Job(hello, args=(Path("/tmp"),), kwargs={})

    assert_state_builtin(job)


def test_dependencies() -> None:
    a = Job(hello, name="a")
    b = Job(hello, name="b")
    c = Job(hello, name="c").after(a).after(b, status="any")

    assert_state_builtin(c)

    c.mark("failure")
    a.mark("success")

    assert_state_builtin(c)


def test_array() -> None:
    jobs = [Job(hello, args=(i,), settings={"cpus": Number(2)}) for i in range(3)]
    array = JobArray(*jobs, throttle=2)

    assert array.settings == {"cpus": 2}

    assert_state_builtin(array)


def test_array_of_arrays_dependencies() -> None:
    first = JobArray(*[Job(hello, args=(i,)) for i in range(2)], name="first")
    second = JobArray(*[Job(hello, args=(i,)) for i in range(2)], name="second")
    second.after(first)

    assert_state_builtin(second)


def test_assert_builtin_detects_non_builtin() -> None:
    job = Job(hello)
    job.settings = {"logdir": Path("/tmp")}

    with pytest.raises(AssertionError, match="not built-in"):
        assert_state_builtin(job)


def test_assert_builtin_detects_subclass() -> None:
    job = Job(hello)
    job.shell = Text("/bin/sh")

    with pytest.raises(AssertionError, match="'Text' instance"):
        assert_state_builtin(job)
