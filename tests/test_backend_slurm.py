"""Tests for the 'slurm' backend."""

import os
import pytest
import re
import subprocess

from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock, patch

import dawgz

########
# Jobs #
########


@dawgz.job
def echo(x: object) -> None:
    print(repr(x))


############
# Fixtures #
############


@pytest.fixture(autouse=True)
def redirect_dawgz_dir(tmp_path: Path) -> None:
    dawgz.set_dawgz_dir(tmp_path / ".dawgz")


@pytest.fixture(autouse=True)
def squeue() -> dict[str, str]:
    return {}


@pytest.fixture(autouse=True)
def mock_subprocess_run(squeue: dict) -> Generator[MagicMock]:
    def run(cmd: list[str], **ignore) -> subprocess.CompletedProcess[str]:
        if cmd[0] == "sbatch":
            with open(cmd[-1]) as shfile:
                match = re.search(r"--array=(\d+)-(\d+)", shfile.read())
                if match:
                    array = range(int(match.group(1)), int(match.group(2)) + 1)
                else:
                    array = None

            jobid = f"{len(squeue):03d}"

            if array is None:
                squeue[jobid] = "PENDING"
            else:
                for i in array:
                    squeue[f"{jobid}_{i}"] = "PENDING"

            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout=f"{jobid}\n", stderr=""
            )
        elif cmd[0] == "scancel":
            prefixes = cmd[2:]

            for jobid in squeue:
                if any(jobid.startswith(prefix) for prefix in prefixes):
                    squeue[jobid] = "CANCELLED"

            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="terminating job\n", stderr=""
            )
        elif cmd[0] == "sacct":
            prefix = cmd[2]
            lines = []

            for jobid, status in squeue.items():
                if jobid.startswith(prefix):
                    lines.append(f"{jobid}|{status}")

            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="\n".join(lines) + "\n", stderr=""
            )
        else:
            raise NotImplementedError(f"Unknown command {cmd[0]}")

    with patch("subprocess.run", side_effect=run) as m:
        yield m


@pytest.fixture(autouse=True)
def mock_srun(tmp_path: Path) -> Generator[dict[str, str]]:
    srun = tmp_path / "srun"
    srun.write_text('#!/bin/bash\nexec "$@"\n')
    srun.chmod(0o755)

    mock_env = {**os.environ, "PATH": f"{tmp_path}:" + os.environ["PATH"]}

    with patch("os.environ", mock_env) as env:
        yield env


def sbatch(shfile: Path, run: object = subprocess.run) -> None:
    content = shfile.read_text()

    # Find output log path
    match = re.search(r"#SBATCH --output=(.+)", content)
    logfile = match.group(1)

    # Find array range
    match = re.search(r"#SBATCH --array=(\d+)-(\d+)", content)

    if match:
        array = range(int(match.group(1)), int(match.group(2)) + 1)
    else:
        array = None

    # Run
    if array is None:
        result = run(
            ["/bin/bash", str(shfile)],
            capture_output=True,
            text=True,
            env=os.environ,
        )

        with open(logfile, mode="w") as f:
            f.write(result.stdout)
    else:
        for i in array:
            result = run(
                ["/bin/bash", str(shfile)],
                capture_output=True,
                text=True,
                env={**os.environ, "SLURM_ARRAY_TASK_ID": str(i)},
            )

            with open(logfile.replace("%a", str(i)), mode="w") as f:
                f.write(result.stdout)


#########
# Tests #
#########


def test_shfile() -> None:
    def hello() -> None:
        pass

    job = dawgz.job(
        hello,
        name="hello",
        shell="/bin/zsh",
        interpreter="python3",
        env=["export FOO=bar", "module load cuda"],
        settings={
            "cpus": 4,
            "ram": "16GB",
            "partition": "gpu",
            "tasks_per_node": 8,
            "gpus_per_node": 8,
        },
    )()

    scheduler = dawgz.schedule(job, backend="slurm")

    tag = scheduler.tag(job)
    shfile = scheduler.path / f"{tag}.sh"
    content = shfile.read_text()

    # Shell
    assert content.startswith("#!/bin/zsh\n")

    # Name
    assert f"--job-name={tag}" in content

    # Env
    assert "export FOO=bar\n" in content
    assert "module load cuda\n" in content

    # Interpreter
    assert "srun python3 " in content

    # Settings
    assert "SBATCH --nodes=1" in content
    assert "SBATCH --ntasks-per-node=8" in content
    assert "SBATCH --cpus-per-task=4" in content
    assert "SBATCH --gpus-per-node=8" in content
    assert "SBATCH --mem=16GB" in content
    assert "SBATCH --partition=gpu" in content


@pytest.mark.parametrize("wait_mode", ["all", "any"])
@pytest.mark.parametrize("status", ["success", "failure"])
def test_dependencies(wait_mode: str, status: str) -> None:
    a_job = echo("a")
    b_job = echo("b")
    c_job = echo("c").after(a_job, b_job, status=status).waitfor(wait_mode)

    scheduler = dawgz.schedule(c_job, backend="slurm")

    a_jobid = scheduler.results[a_job]
    b_jobid = scheduler.results[b_job]
    c_tag = scheduler.tag(c_job)

    shfile = scheduler.path / f"{c_tag}.sh"
    content = shfile.read_text()

    sep = "," if wait_mode == "all" else "?"
    after = "afterok" if status == "success" else "afternotok"

    assert f"--dependency={after}:{a_jobid}{sep}{after}:{b_jobid}" in content


def test_dependency_submission_failure() -> None:
    a_job = echo("a")
    b_job = echo("b").after(a_job)

    def failing_sbatch(cmd: list[str], **ignore) -> subprocess.CompletedProcess[str]:
        if cmd[0] == "sbatch":
            raise subprocess.CalledProcessError(cmd=cmd, returncode=1, stderr="submission error")
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

    with patch("subprocess.run", side_effect=failing_sbatch):
        scheduler = dawgz.schedule(b_job, backend="slurm")

    assert "JobSubmissionError" in scheduler.logs(a_job)
    assert "JobNeverSatisfiedError" in scheduler.logs(b_job)


def test_single_job() -> None:
    x = "a"
    job = echo(x)
    scheduler = dawgz.schedule(job, backend="slurm")

    tag = scheduler.tag(job)
    shfile = scheduler.path / f"{tag}.sh"

    sbatch(shfile)

    assert scheduler.logs(job) == repr(x)


def test_job_array() -> None:
    xs = ["a", "b", "c"]
    jobs = [echo(x) for x in xs]
    array = dawgz.array(*jobs)

    scheduler = dawgz.schedule(array, backend="slurm")

    tag = scheduler.tag(array)
    shfile = scheduler.path / f"{tag}.sh"

    sbatch(shfile)

    for i, x in enumerate(xs):
        assert scheduler.logs(array, i) == repr(x)


def test_export_env_var() -> None:
    def get_env() -> None:
        print(os.environ["DAWGZ_VAR"])

    job = dawgz.job(
        get_env,
        env=[
            "export DAWGZ_VAR='hello, world'",
        ],
    )()

    scheduler = dawgz.schedule(job, backend="slurm")

    tag = scheduler.tag(job)
    shfile = scheduler.path / f"{tag}.sh"

    sbatch(shfile)

    assert scheduler.logs(job) == "hello, world"


def test_cancel() -> None:
    @dawgz.job
    def f() -> None:
        pass

    job = f()

    scheduler = dawgz.schedule(job, backend="slurm")
    assert scheduler.state(job) == "PENDING"

    scheduler.cancel(job)
    assert scheduler.state(job) == "PENDING"

    dawgz.schedulers.SACCT_CACHE.clear()
    assert scheduler.state(job) == "CANCELLED"
