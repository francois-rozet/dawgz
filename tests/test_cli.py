"""Tests for the dawgz CLI."""

import pytest
import sys

from pathlib import Path

import dawgz

from dawgz.__main__ import main

########
# Jobs #
########


@dawgz.job
def noop() -> int:
    return 42


@dawgz.job
def failing(arg: str, kwarg: str) -> None:
    raise RuntimeError("intentional failure")


############
# Fixtures #
############


@pytest.fixture(autouse=True)
def redirect_dawgz_dir(tmp_path: Path) -> None:
    dawgz.set_dawgz_dir(tmp_path / ".dawgz")


@pytest.fixture()
def dummy_workflow() -> dawgz.Scheduler:
    return dawgz.schedule(
        noop(), failing("a", kwarg="k"), name="dummy", backend="async", quiet=True
    )


#########
# Tests #
#########


def test_main_no_workflows(capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["dawgz"])
    main()
    out = capsys.readouterr().out
    assert "Name" in out
    assert "Backend" in out


def test_main_workflows(
    dummy_workflow: dawgz.Scheduler, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "argv", ["dawgz"])
    main()
    out = capsys.readouterr().out
    assert "dummy" in out
    assert "async" in out


def test_main_workflow(
    dummy_workflow: dawgz.Scheduler, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "argv", ["dawgz", "0"])
    main()
    out = capsys.readouterr().out
    assert "noop()" in out
    assert "COMPLETED" in out
    assert "failing('a', kwarg='k')" in out
    assert "FAILED" in out


def test_main_job(
    dummy_workflow: dawgz.Scheduler, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "argv", ["dawgz", "-1", "0"])
    main()
    out = capsys.readouterr().out
    assert "noop()" in out
    assert "COMPLETED" in out
    assert "42" in out


def test_main_job_failing(
    dummy_workflow: dawgz.Scheduler, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "argv", ["dawgz", "-1", "-1"])
    main()
    out = capsys.readouterr().out
    assert "failing('a', kwarg='k')" in out
    assert "FAILED" in out
    assert "RuntimeError" in out


def test_main_invalid_workflow_index(
    dummy_workflow: dawgz.Scheduler, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "argv", ["dawgz", "99"])
    with pytest.raises(IndexError):
        main()


def test_main_invalid_job_index(
    dummy_workflow: dawgz.Scheduler, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "argv", ["dawgz", "0", "99"])
    with pytest.raises(IndexError):
        main()
