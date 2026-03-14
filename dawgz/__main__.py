r"""Module's main"""

import argparse
import csv

from tabulate import tabulate

from .schedulers import DIR, Scheduler


def table(workflows: list[list[str]], workflow: int | None = None, job: int | None = None) -> None:
    if workflow is None:
        headers = ("Name", "ID", "Date", "Backend", "Jobs", "Errors")
        table = tabulate(workflows, headers, showindex=True)
    else:
        row = workflows[workflow]
        uid = row[1]
        scheduler = Scheduler.load(DIR / uid)

        if job is None:
            table = scheduler.report()
        else:
            jobs = list(scheduler.order)
            job = jobs[job]

            table = scheduler.report(job)

    try:
        print(table)
    except BrokenPipeError:
        pass


def cancel(workflows: list[list[str]], workflow: int, job: int | None = None) -> None:
    row = workflows[workflow]
    uuid = row[1]
    scheduler = Scheduler.load(DIR / uuid)

    if job is None:
        message = scheduler.cancel()
    else:
        jobs = list(scheduler.order)
        job = jobs[job]

        message = scheduler.cancel(job)

    if message:
        print(message)


def main() -> None:
    # Parser
    parser = argparse.ArgumentParser(description="DAWGZ's CLI")

    parser.add_argument("workflow", default=None, nargs="?", type=int, help="workflow index")
    parser.add_argument("job", default=None, nargs="?", type=int, help="job index")

    parser.add_argument("-c", "--cancel", default=False, action="store_true")

    args = parser.parse_args()

    # Workflows
    record = DIR / "workflows.csv"

    if record.exists():
        with open(record) as f:
            workflows = list(csv.reader(f))
    else:
        workflows = []

    # Action
    if args.cancel:
        cancel(workflows, args.workflow, args.job)
    else:
        table(workflows, args.workflow, args.job)


if __name__ == "__main__":
    main()
