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
    print("42")


@dawgz.job
def failing(arg: str, kwarg: str) -> None:
    raise RuntimeError("intentional failure")


@dawgz.job
def echo(msg: str) -> None:
    print(msg)


############
# Fixtures #
############


@pytest.fixture(autouse=True)
def redirect_dawgz_dir(tmp_path: Path) -> None:
    dawgz.set_dawgz_dir(tmp_path / ".dawgz")


@pytest.fixture()
def dummy_workflow() -> dawgz.Scheduler:
    return dawgz.schedule(
        noop(),
        failing("a", kwarg="k"),
        echo("[bracket] hello"),
        name="dummy",
        backend="async",
        quiet=True,
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
    assert "noop" in out
    assert "COMPLETED" in out
    assert "failing" in out
    assert "FAILED" in out
    assert "echo" in out
    assert "COMPLETED" in out


def test_main_job(
    dummy_workflow: dawgz.Scheduler, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "argv", ["dawgz", "0", "0"])
    main()
    out = capsys.readouterr().out
    assert "noop" in out
    assert "COMPLETED" in out
    assert "42" in out


def test_main_job_failing(
    dummy_workflow: dawgz.Scheduler, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "argv", ["dawgz", "-1", "1"])
    main()
    out = capsys.readouterr().out
    assert "failing" in out
    assert "FAILED" in out
    assert "RuntimeError" in out


def test_main_job_source(
    dummy_workflow: dawgz.Scheduler, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "argv", ["dawgz", "-1", "-2", "--source"])
    main()
    out = capsys.readouterr().out
    assert "failing" in out
    assert "FAILED" in out
    assert "def failing" in out


def test_main_job_input(
    dummy_workflow: dawgz.Scheduler, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "argv", ["dawgz", "0", "-2", "--input"])
    main()
    out = capsys.readouterr().out
    assert "failing" in out
    assert "FAILED" in out
    assert "failing('a', kwarg='k')" in out


def test_main_job_logs_preserve_brackets(
    dummy_workflow: dawgz.Scheduler, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "argv", ["dawgz", "-1", "-1"])
    main()
    out = capsys.readouterr().out
    assert "echo" in out
    assert "[bracket] hello" in out


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
