"""Tests for the dawgz CLI."""

import pytest
import sys

from datetime import datetime, timedelta
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


def test_main_workflows_date_filter(
    capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "dawgz.__main__.list_workflows",
        lambda: [
            ["before", "id-1", "2025-01-01 12:00:00", "async", "1", "0"],
            ["matching", "id-2", "2025-01-02 00:00:00", "async", "1", "0"],
            ["after", "id-3", "2025-01-03 00:00:00", "async", "1", "0"],
        ],
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "dawgz",
            "--since",
            "2025-01-02",
            "--before",
            "2025-01-03",
        ],
    )

    main()

    out = capsys.readouterr().out
    assert "matching" in out
    assert "before" not in out
    assert "after" not in out


def test_main_workflows_relative_date_filter(
    capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = datetime.now()

    monkeypatch.setattr(
        "dawgz.__main__.list_workflows",
        lambda: [
            ["old", "id-1", str(now - timedelta(days=10)), "async", "1", "0"],
            ["recent", "id-2", str(now - timedelta(days=2)), "async", "1", "0"],
            ["fresh", "id-3", str(now - timedelta(minutes=30)), "async", "1", "0"],
        ],
    )
    monkeypatch.setattr(sys, "argv", ["dawgz", "--since", "3d", "--before", "1h"])

    main()

    out = capsys.readouterr().out
    assert "recent" in out
    assert "old" not in out
    assert "fresh" not in out


def test_main_workflows_mixed_absolute_and_relative(
    capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = datetime.now()

    monkeypatch.setattr(
        "dawgz.__main__.list_workflows",
        lambda: [
            ["ancient", "id-1", "2020-01-01 00:00:00", "async", "1", "0"],
            ["recent", "id-2", str(now - timedelta(days=1)), "async", "1", "0"],
        ],
    )
    monkeypatch.setattr(sys, "argv", ["dawgz", "--since", "2020-01-02", "--before", "0h"])

    main()

    out = capsys.readouterr().out
    assert "recent" in out
    assert "ancient" not in out


def test_main_workflows_invalid_timestamp(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["dawgz", "--since", "yesterday"])

    with pytest.raises(SystemExit):
        main()


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


def test_main_workflows_states_counts_arrays(
    capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    @dawgz.job
    def fan(i: int) -> None:
        print(i)

    array = dawgz.array(fan(0), fan(1), fan(2), name="fan")
    scheduler = dawgz.schedule(noop(), array, name="arrays", backend="dummy", quiet=True)

    monkeypatch.setattr(sys, "argv", ["dawgz", "--fetch-states"])
    main()
    out = capsys.readouterr().out

    # 2 jobs, but 4 states (the array counts once per index)
    assert "arrays" in out
    assert scheduler.state()["COMPLETED"] == 4
    assert "4 COMPLETED" in out


def test_main_workflows_states(
    dummy_workflow: dawgz.Scheduler, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "argv", ["dawgz", "--fetch-states"])
    main()
    out = capsys.readouterr().out

    assert "States" in out
    assert "2 COMPLETED" in out
    assert "1 FAILED" in out


def test_main_workflows_without_states_flag(
    dummy_workflow: dawgz.Scheduler, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "argv", ["dawgz"])
    main()
    out = capsys.readouterr().out

    assert "States" not in out
    assert "COMPLETED" not in out


def test_main_workflows_states_respects_date_filter(
    dummy_workflow: dawgz.Scheduler, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "argv", ["dawgz", "--since", "2100-01-01", "--fetch-states"])

    main()

    out = capsys.readouterr().out
    assert "States" in out
    assert dummy_workflow.uid not in out


def test_main_workflow_has_no_workflow_table(
    dummy_workflow: dawgz.Scheduler, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "argv", ["dawgz", "0"])
    main()
    out = capsys.readouterr().out

    assert dummy_workflow.uid not in out


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


def test_main_job_raw(
    dummy_workflow: dawgz.Scheduler, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "argv", ["dawgz", "0", "0", "--raw"])
    main()
    out = capsys.readouterr().out
    assert "42" in out
    assert "noop" not in out
    assert "State" not in out


def test_main_invalid_workflow_index(
    dummy_workflow: dawgz.Scheduler, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "argv", ["dawgz", "99"])
    with pytest.raises(SystemExit, match="Workflow index 99 is out of range"):
        main()


def test_main_invalid_job_index(
    dummy_workflow: dawgz.Scheduler, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "argv", ["dawgz", "0", "99"])
    with pytest.raises(SystemExit, match="Job index 99 is out of range"):
        main()


def test_main_invalid_workflow_index_negative(
    dummy_workflow: dawgz.Scheduler, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "argv", ["dawgz", "-99"])
    with pytest.raises(SystemExit, match="Expected an index between -1 and 0"):
        main()


def test_main_invalid_job_index_negative(
    dummy_workflow: dawgz.Scheduler, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "argv", ["dawgz", "0", "-99"])
    with pytest.raises(SystemExit, match="Expected an index between -3 and 2"):
        main()


def test_main_valid_negative_indices_still_work(
    dummy_workflow: dawgz.Scheduler, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "argv", ["dawgz", "-1", "-1"])

    main()

    out = capsys.readouterr().out
    assert "echo" in out


def test_main_index_without_workflows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["dawgz", "0"])
    with pytest.raises(SystemExit, match="No workflow to index"):
        main()


def test_main_invalid_array_index(
    capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    @dawgz.job
    def fan(i: int) -> None:
        print(i)

    dawgz.schedule(
        dawgz.array(fan(0), fan(1), fan(2), name="fan"),
        name="arrays",
        backend="async",
        quiet=True,
    )

    monkeypatch.setattr(sys, "argv", ["dawgz", "0", "0", "99"])
    with pytest.raises(SystemExit, match="Job array index 99 is out of range"):
        main()


def test_main_valid_array_index(
    capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    @dawgz.job
    def fan(i: int) -> None:
        print(f"index {i}")

    dawgz.schedule(
        dawgz.array(fan(0), fan(1), fan(2), name="fan"),
        name="arrays",
        backend="async",
        quiet=True,
    )

    monkeypatch.setattr(sys, "argv", ["dawgz", "0", "0", "2"])

    main()

    out = capsys.readouterr().out
    assert "index 2" in out


def test_main_missing_workflow_files(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "dawgz.__main__.list_workflows",
        lambda: [["ghost", "gone-uid", "2025-01-01 00:00:00", "async", "1", "0"]],
    )
    monkeypatch.setattr(sys, "argv", ["dawgz", "0"])

    with pytest.raises(SystemExit, match="files of workflow 'gone-uid' are missing"):
        main()


def test_main_cancel_invalid_workflow_index(
    dummy_workflow: dawgz.Scheduler, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "argv", ["dawgz", "99", "--cancel"])
    with pytest.raises(SystemExit, match="Workflow index 99 is out of range"):
        main()
