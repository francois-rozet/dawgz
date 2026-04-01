"""Tests for the 'async' backend."""

import pytest

from pathlib import Path

import dawgz

########
# Jobs #
########


@dawgz.job
def echo(x: object) -> None:
    print(repr(x))


@dawgz.job
def fail() -> None:
    raise RuntimeError("intentional failure")


############
# Fixtures #
############


@pytest.fixture(autouse=True)
def redirect_dawgz_dir(tmp_path: Path) -> None:
    dawgz.set_dawgz_dir(tmp_path / ".dawgz")


#########
# Tests #
#########


def test_not_callable() -> None:
    with pytest.raises(TypeError, match="not a callable"):
        dawgz.job("callable")()


def test_missing_args() -> None:
    with pytest.raises(TypeError, match="missing"):
        echo()


def test_too_many_args() -> None:
    with pytest.raises(TypeError, match="too many"):
        echo(1, 2)


def test_unexpected_kwargs() -> None:
    with pytest.raises(TypeError, match="unexpected"):
        echo(1, y=2)


def test_single_job() -> None:
    x = "a"
    job = echo(x)
    scheduler = dawgz.schedule(job, backend="async")

    assert scheduler.logs(job) == repr(x)
    assert scheduler.state(job) == "COMPLETED"
    assert job not in scheduler.traces


def test_failing_job() -> None:
    job = fail()
    scheduler = dawgz.schedule(job, backend="async")

    assert "RuntimeError" in scheduler.logs(job)
    assert "JobFailedError" in scheduler.traces[job]
    assert scheduler.state(job) == "FAILED"


def test_mutation() -> None:
    @dawgz.job
    def mutate_list() -> None:
        xs.append("e")
        print(xs)

    xs = ["a", "b", "c"]
    job = mutate_list()
    xs.append("d")

    scheduler = dawgz.schedule(job, backend="async")

    assert "'d'" not in scheduler.logs(job)
    assert "'e'" in scheduler.logs(job)
    assert "e" not in xs


def test_chain() -> None:
    xs = ["a", "b", "c", "d"]
    jobs = [echo(x) for x in xs]
    jobs[1].after(jobs[0])
    jobs[2].after(jobs[1])
    jobs[3].after(jobs[2])

    scheduler = dawgz.schedule(jobs[-1], backend="async")

    for x, job in zip(xs, jobs, strict=True):
        assert scheduler.logs(job) == repr(x)
        assert scheduler.state(job) == "COMPLETED"


def test_diamond() -> None:
    xs = ["a", "b", "c", "d"]
    jobs = [echo(x) for x in xs]
    jobs[1].after(jobs[0])
    jobs[2].after(jobs[0])
    jobs[3].after(jobs[1], jobs[2])

    scheduler = dawgz.schedule(jobs[-1], backend="async")

    for x, job in zip(xs, jobs, strict=True):
        assert scheduler.logs(job) == repr(x)
        assert scheduler.state(job) == "COMPLETED"


def test_cyclic_dependency() -> None:
    xs = ["a", "b", "c"]
    jobs = [echo(x) for x in xs]
    jobs[1].after(jobs[0])
    jobs[2].after(jobs[1])
    jobs[0].after(jobs[2])

    with pytest.raises(dawgz.schedulers.CyclicDependencyGraphError):
        dawgz.schedule(*jobs, backend="async")


def test_blocked_by_failed_dep() -> None:
    a_job = fail()
    b_job = echo("b").after(a_job)

    scheduler = dawgz.schedule(b_job, backend="async")

    assert "RuntimeError" in scheduler.logs(a_job)
    assert "JobNeverSatisfiedError" in scheduler.logs(b_job)
    assert scheduler.state(a_job) == "FAILED"
    assert scheduler.state(b_job) == "CANCELLED"


def test_triggered_by_failed_dep() -> None:
    a_job = fail()
    b_job = echo("b").after(a_job, status="failure")

    scheduler = dawgz.schedule(b_job, backend="async")

    assert "RuntimeError" in scheduler.logs(a_job)
    assert scheduler.logs(b_job) == "'b'"
    assert scheduler.state(a_job) == "FAILED"
    assert scheduler.state(b_job) == "COMPLETED"


def test_waitfor_any_one_succeeds() -> None:
    a_job = fail()
    b_job = echo("b")
    c_job = echo("c").after(a_job, b_job).waitfor("any")

    scheduler = dawgz.schedule(c_job, backend="async")

    assert "RuntimeError" in scheduler.logs(a_job)
    assert scheduler.logs(b_job) == "'b'"
    assert scheduler.logs(c_job) == "'c'"
    assert scheduler.state(a_job) == "FAILED"
    assert scheduler.state(b_job) == "COMPLETED"
    assert scheduler.state(c_job) == "COMPLETED"


def test_waitfor_any_all_fail() -> None:
    a_job = fail()
    b_job = fail()
    c_job = echo("c").after(a_job, b_job).waitfor("any")

    scheduler = dawgz.schedule(c_job, backend="async")

    assert "RuntimeError" in scheduler.logs(a_job)
    assert "RuntimeError" in scheduler.logs(b_job)
    assert "JobNeverSatisfiedError" in scheduler.logs(c_job)
    assert scheduler.state(a_job) == "FAILED"
    assert scheduler.state(b_job) == "FAILED"
    assert scheduler.state(c_job) == "CANCELLED"


def test_triggered_by_success_status() -> None:
    a_job = fail().mark("success")
    b_job = echo("b").after(a_job)

    scheduler = dawgz.schedule(b_job, backend="async")

    assert a_job not in scheduler.order
    assert a_job not in scheduler.traces
    assert scheduler.logs(b_job) == "'b'"
    assert scheduler.state(a_job) == "UNKNOWN"
    assert scheduler.state(b_job) == "COMPLETED"


def test_blocked_by_failure_status() -> None:
    a_job = echo("a").mark("failure")
    b_job = echo("b").after(a_job)

    scheduler = dawgz.schedule(b_job, backend="async")

    assert a_job not in scheduler.order
    assert a_job not in scheduler.traces
    assert "JobNeverSatisfiedError" in scheduler.logs(b_job)
    assert scheduler.state(a_job) == "UNKNOWN"
    assert scheduler.state(b_job) == "CANCELLED"


def test_triggered_by_failure_status() -> None:
    a_job = echo("a").mark("failure")
    b_job = echo("b").after(a_job, status="failure")

    scheduler = dawgz.schedule(b_job, backend="async")

    assert a_job not in scheduler.order
    assert a_job not in scheduler.traces
    assert scheduler.logs(b_job) == "'b'"
    assert scheduler.state(a_job) == "UNKNOWN"
    assert scheduler.state(b_job) == "COMPLETED"


def test_array() -> None:
    xs = ["a", "b", "c"]
    jobs = [echo(x) for x in xs]
    array = dawgz.array(*jobs)

    scheduler = dawgz.schedule(array, backend="async")

    assert scheduler.state(array) == "COMPLETED"

    for i, x in enumerate(xs):
        assert scheduler.logs(array, i) == repr(x)


def test_array_one_fails() -> None:
    xs = ["a", "b", "c"]
    jobs = [echo(x) for x in xs] + [fail()]
    array = dawgz.array(*jobs)

    scheduler = dawgz.schedule(array, backend="async")

    assert scheduler.state(array) == "FAILED"

    for i, x in enumerate(xs):
        assert scheduler.logs(array, i) == repr(x)

    assert "RuntimeError" in scheduler.logs(array, len(xs))


def test_triggered_by_array() -> None:
    xs = ["a", "b", "c"]
    jobs = [echo(x) for x in xs]
    array = dawgz.array(*jobs)
    y_job = echo("y").after(array)

    scheduler = dawgz.schedule(y_job, backend="async")

    assert scheduler.state(array) == "COMPLETED"
    assert scheduler.logs(y_job) == "'y'"
    assert scheduler.state(y_job) == "COMPLETED"


def test_blocked_by_failed_array() -> None:
    xs = ["a", "b", "c"]
    jobs = [echo(x) for x in xs] + [fail()]
    array = dawgz.array(*jobs)
    y_job = echo("y").after(array)

    scheduler = dawgz.schedule(y_job, backend="async")

    assert scheduler.state(array) == "FAILED"
    assert "JobNeverSatisfiedError" in scheduler.logs(y_job)
    assert scheduler.state(y_job) == "CANCELLED"
