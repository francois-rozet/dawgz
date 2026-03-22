"""Tests for the 'async' backend."""

import pytest

from pathlib import Path
from typing import Any

import dawgz

from dawgz.schedulers import CyclicDependencyGraphError

########
# Jobs #
########


@dawgz.job
def identity(x: Any) -> Any:
    return x


@dawgz.job
def fail() -> None:
    raise RuntimeError("intentional failure")


@dawgz.job
def mutate_list(items: list) -> list:
    items.append("mutated")
    return items


############
# Fixtures #
############


@pytest.fixture(autouse=True)
def redirect_dawgz_dir(tmp_path: Path) -> None:
    dawgz.set_dawgz_dir(tmp_path / ".dawgz")


@pytest.fixture(params=[None, 4], ids=["threads", "processes"])
def pools(request: pytest.FixtureRequest) -> int | None:
    return request.param


#########
# Tests #
#########


def test_single_job(pools: int | None) -> None:
    job = identity("x")
    scheduler = dawgz.schedule(job, backend="async", quiet=True, pools=pools)
    assert scheduler.results[job] == "x"
    assert job not in scheduler.traces


def test_failing_job(pools: int | None) -> None:
    job = fail()
    scheduler = dawgz.schedule(job, backend="async", quiet=True, pools=pools)
    assert "RuntimeError" in scheduler.traces[job]
    assert job not in scheduler.results


def test_mutation(pools: int | None) -> None:
    items = ["a", "b", "c"]
    job = mutate_list(items)
    items.append("d")
    scheduler = dawgz.schedule(job, backend="async", quiet=True, pools=pools)
    assert "d" not in scheduler.results[job]
    assert "mutated" in scheduler.results[job]
    assert "mutated" not in items


def test_linear_chain(pools: int | None) -> None:
    a, b, c = "abc"
    a_job = identity(a)
    b_job = identity(b).after(a_job)
    c_job = identity(c).after(b_job)
    scheduler = dawgz.schedule(c_job, backend="async", quiet=True, pools=pools)
    assert scheduler.results[a_job] == a
    assert scheduler.results[b_job] == b
    assert scheduler.results[c_job] == c


def test_fan_out(pools: int | None) -> None:
    a, b, c, d = "abcd"
    a_job = identity(a)
    b_job = identity(b).after(a_job)
    c_job = identity(c).after(a_job)
    d_job = identity(d).after(a_job)
    scheduler = dawgz.schedule(b_job, c_job, d_job, backend="async", quiet=True, pools=pools)
    assert scheduler.results[a_job] == a
    assert scheduler.results[b_job] == b
    assert scheduler.results[c_job] == c
    assert scheduler.results[d_job] == d


def test_fan_in(pools: int | None) -> None:
    a, b, c, d = "abcd"
    a_job = identity(a)
    b_job = identity(b)
    c_job = identity(c)
    d_job = identity(d).after(a_job, b_job, c_job)
    scheduler = dawgz.schedule(d_job, backend="async", quiet=True, pools=pools)
    assert scheduler.results[a_job] == a
    assert scheduler.results[b_job] == b
    assert scheduler.results[c_job] == c
    assert scheduler.results[d_job] == d


def test_diamond(pools: int | None) -> None:
    a, b, c, d = "abcd"
    a_job = identity(a)
    b_job = identity(b).after(a_job)
    c_job = identity(c).after(a_job)
    d_job = identity(d).after(b_job, c_job)
    scheduler = dawgz.schedule(d_job, backend="async", quiet=True, pools=pools)
    assert scheduler.results[a_job] == a
    assert scheduler.results[b_job] == b
    assert scheduler.results[c_job] == c
    assert scheduler.results[d_job] == d


def test_cyclic_dependency(pools: int | None) -> None:
    a_job = identity(None)
    b_job = identity(None)
    a_job.after(b_job)
    b_job.after(a_job)
    with pytest.raises(CyclicDependencyGraphError):
        dawgz.schedule(a_job, backend="async", quiet=True, pools=pools)


def test_blocked_by_failed_dep(pools: int | None) -> None:
    a_job = fail()
    b_job = identity(None).after(a_job)
    scheduler = dawgz.schedule(b_job, backend="async", quiet=True, pools=pools)
    assert "RuntimeError" in scheduler.traces[a_job]
    assert "DependencyNeverSatisfiedError" in scheduler.traces[b_job]


def test_triggered_by_failed_dep(pools: int | None) -> None:
    a_job = fail()
    b_job = identity("b").after(a_job, status="failure")
    scheduler = dawgz.schedule(b_job, backend="async", quiet=True, pools=pools)
    assert "RuntimeError" in scheduler.traces[a_job]
    assert scheduler.results[b_job] == "b"


def test_waitfor_any_one_succeeds(pools: int | None) -> None:
    a_job = fail()
    b_job = identity("b")
    c_job = identity("c").after(a_job, b_job).waitfor("any")
    scheduler = dawgz.schedule(c_job, backend="async", quiet=True, pools=pools)
    assert "RuntimeError" in scheduler.traces[a_job]
    assert scheduler.results[b_job] == "b"
    assert scheduler.results[c_job] == "c"


def test_waitfor_any_all_fail(pools: int | None) -> None:
    a_job = fail()
    b_job = fail()
    c_job = identity("c").after(a_job, b_job).waitfor("any")
    scheduler = dawgz.schedule(c_job, backend="async", quiet=True, pools=pools)
    assert "RuntimeError" in scheduler.traces[a_job]
    assert "RuntimeError" in scheduler.traces[b_job]
    assert "DependencyNeverSatisfiedError" in scheduler.traces[c_job]


def test_mark_success(pools: int | None) -> None:
    a_job = fail().mark("success")
    b_job = identity("b").after(a_job)
    scheduler = dawgz.schedule(b_job, backend="async", quiet=True, pools=pools)
    assert a_job not in scheduler.results
    assert a_job not in scheduler.traces
    assert scheduler.results[b_job] == "b"


def test_mark_failure(pools: int | None) -> None:
    a_job = identity("a").mark("failure")
    b_job = identity("b").after(a_job, status="failure")
    scheduler = dawgz.schedule(b_job, backend="async", quiet=True, pools=pools)
    assert a_job not in scheduler.results
    assert a_job not in scheduler.traces
    assert scheduler.results[b_job] == "b"


def test_job_array(pools: int | None) -> None:
    xs = "abc"
    array = dawgz.array(*map(identity, xs))
    scheduler = dawgz.schedule(array, backend="async", quiet=True, pools=pools)
    assert all(scheduler.results[array][i] == x for i, x in enumerate(xs))


def test_job_array_with_dep(tmp_path: Path, pools: int | None) -> None:
    a_job = identity("a")
    xs = "abc"
    array = dawgz.array(*map(identity, xs)).after(a_job)
    scheduler = dawgz.schedule(array, backend="async", quiet=True, pools=pools)
    assert scheduler.results[a_job] == "a"
    assert all(scheduler.results[array][i] == x for i, x in enumerate(xs))
