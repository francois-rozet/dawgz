r"""Module's main"""

import argparse
import csv
import rich.box
import rich.console
import rich.table
import shutil

from typing import Literal

from dawgz import Scheduler, get_dawgz_dir


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
) -> None:
    workflows = list_workflows()

    if workflow is None:
        table = rich.table.Table(box=rich.box.ROUNDED)
        table.add_column("", justify="right", no_wrap=True, min_width=2)
        table.add_column("Name", justify="left", no_wrap=True)
        table.add_column("ID", justify="left", no_wrap=False)
        table.add_column("Date", justify="left", no_wrap=True)
        table.add_column("Backend", justify="left", no_wrap=True)
        table.add_column("Jobs", justify="right", no_wrap=True)
        table.add_column("Errors", justify="right", no_wrap=True)

        for j, row in enumerate(workflows):
            table.add_row(str(j), *row)

        renderables = [table]
    else:
        row = workflows[workflow]
        uid = row[1]
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
    parser = argparse.ArgumentParser(description="DAWGZ's CLI")

    parser.add_argument("workflow", default=None, nargs="?", type=int, help="workflow index")
    parser.add_argument("job", default=None, nargs="?", type=int, help="job index")
    parser.add_argument("i", default=None, nargs="?", type=int, help="job array index")
    parser.add_argument("--raw", action="store_true", help="report job logs without table")

    group = parser.add_mutually_exclusive_group()
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
        report(args.workflow, args.job, args.i, args.entry or "logs", args.raw)


if __name__ == "__main__":
    main()
