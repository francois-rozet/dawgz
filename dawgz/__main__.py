r"""Module's main"""

import argparse
import csv
import rich.box
import rich.console
import rich.table
import shutil

from datetime import datetime, timedelta
from typing import Literal

from dawgz import Scheduler, get_dawgz_dir
from dawgz.schedulers.core import format_states
from dawgz.utils import parse_timestamp

DESCRIPTION = """\
Inspect the workflows scheduled with DAWGZ.

Without arguments, list the scheduled workflows. Give a workflow index to list its
jobs, and a job index (optionally followed by a job array index) to report the output
of that job. Indices are those displayed in the tables and can be negative.
"""

EXAMPLES = """\
examples:
  dawgz                     list the scheduled workflows
  dawgz --since 3d          list the workflows submitted in the last 3 days
  dawgz --fetch-states      list the workflows and the states of their jobs
  dawgz 1                   list the jobs of workflow 1
  dawgz -1                  list the jobs of the last workflow
  dawgz 1 2                 report the logs of job 2 of workflow 1
  dawgz 1 2 --raw           report the logs of job 2 without table
  dawgz 1 2 3               report the logs of index 3 of job array 2
  dawgz 1 2 --source        report the source of job 2 instead of its logs
  dawgz 1 --cancel          cancel all the jobs of workflow 1
"""


def list_workflows() -> list[list[str]]:
    record = get_dawgz_dir() / "workflows.csv"

    if record.exists():
        with open(record) as f:
            return list(csv.reader(f))
    else:
        return []


def report(
    workflow: int | None = None,
    job: int | None = None,
    i: int | None = None,
    entry: Literal["source", "settings", "input", "logs"] = "logs",
    raw: bool = False,
    since: datetime | timedelta | None = None,
    before: datetime | timedelta | None = None,
    fetch_states: bool = False,
) -> None:
    workflows = list_workflows()

    now = datetime.now()
    if isinstance(since, timedelta):
        since = now - since
    if isinstance(before, timedelta):
        before = now - before

    if workflow is None:
        table = rich.table.Table(box=rich.box.ROUNDED)
        table.add_column("", justify="right", no_wrap=True, min_width=2)
        table.add_column("Name", justify="left", no_wrap=True)
        table.add_column("ID", justify="left", no_wrap=False)
        table.add_column("Date", justify="left", no_wrap=True)
        table.add_column("Backend", justify="left", no_wrap=True)
        table.add_column("Jobs", justify="right", no_wrap=True)
        table.add_column("Errors", justify="right", no_wrap=True)

        if fetch_states:
            table.add_column("States", justify="left", no_wrap=True)

        for j, row in enumerate(workflows):
            _, uid, submitted, *_ = row

            submitted = datetime.fromisoformat(submitted)
            if since is not None and submitted < since:
                continue
            if before is not None and submitted >= before:
                continue

            if fetch_states:
                try:
                    states = Scheduler.load(get_dawgz_dir() / uid).state()
                except Exception:
                    states = "UNKNOWN"

                table.add_row(str(j), *row, format_states(states))
            else:
                table.add_row(str(j), *row)

        renderables = [table]
    else:
        _, uid, *_ = workflows[workflow]
        scheduler = Scheduler.load(get_dawgz_dir() / uid)

        if job is None:
            renderables = scheduler.report()
        else:
            renderables = scheduler.report(job, i, entry=entry, raw=raw)

    if raw:
        width = 1_000_000
    else:
        width = shutil.get_terminal_size(fallback=(1_000_000, 0)).columns

    try:
        console = rich.console.Console(highlight=False, width=width)
        for r in renderables:
            console.print(r)
    except BrokenPipeError:
        pass


def cancel(
    workflow: int,
    job: int | None = None,
    i: int | None = None,
) -> None:
    workflows = list_workflows()

    row = workflows[workflow]
    uuid = row[1]
    scheduler = Scheduler.load(get_dawgz_dir() / uuid)

    if job is None:
        message = scheduler.cancel()
    else:
        message = scheduler.cancel(job, i)

    if message:
        print(message)


def main() -> None:
    # Parser
    parser = argparse.ArgumentParser(
        description=DESCRIPTION,
        epilog=EXAMPLES,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("workflow", default=None, nargs="?", type=int, help="workflow index")
    parser.add_argument("job", default=None, nargs="?", type=int, help="job index")
    parser.add_argument("i", default=None, nargs="?", type=int, help="job array index")
    parser.add_argument("--raw", action="store_true", help="report job logs without table")
    parser.add_argument(
        "--since",
        type=parse_timestamp,
        metavar="TIMESTAMP",
        help="list workflows submitted on or after this date-time or duration ago",
    )
    parser.add_argument(
        "--before",
        type=parse_timestamp,
        metavar="TIMESTAMP",
        help="list workflows submitted before this date-time or duration ago",
    )
    parser.add_argument(
        "--fetch-states",
        action="store_true",
        help="fetch and report the job states of each workflow",
    )

    group = parser.add_mutually_exclusive_group()
    group.set_defaults(entry="logs")
    group.add_argument(
        "-c", "--cancel", default=False, action="store_true", help="cancel workflow or job"
    )

    for entry in ["source", "settings", "input", "logs"]:
        group.add_argument(
            f"--{entry}",
            dest="entry",
            action="store_const",
            const=entry,
            help=f"report job {entry} in table",
        )

    args = parser.parse_args()

    # Action
    if args.cancel:
        cancel(args.workflow, args.job, args.i)
    else:
        report(
            args.workflow,
            args.job,
            args.i,
            args.entry,
            args.raw,
            args.since,
            args.before,
            args.fetch_states,
        )


if __name__ == "__main__":
    main()
