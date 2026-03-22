r"""Module's main"""

import argparse
import csv

from tabulate import tabulate

from dawgz import Scheduler, get_dawgz_dir


def list_workflows() -> list[list[str]]:
    record = get_dawgz_dir() / "workflows.csv"

    if record.exists():
        with open(record) as f:
            return list(csv.reader(f))
    else:
        return []


def table(
    workflow: int | None = None,
    job: int | None = None,
    i: int | None = None,
) -> None:
    workflows = list_workflows()

    if workflow is None:
        headers = ("Name", "ID", "Date", "Backend", "Jobs", "Errors")
        table = tabulate(workflows, headers, showindex=True)
    else:
        row = workflows[workflow]
        uid = row[1]
        scheduler = Scheduler.load(get_dawgz_dir() / uid)

        if job is None:
            table = scheduler.report()
        else:
            jobs = list(scheduler.order)
            job = jobs[job]

            table = scheduler.report(job, i)

    try:
        print(table)
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
        jobs = list(scheduler.order)
        job = jobs[job]

        message = scheduler.cancel(job, i)

    if message:
        print(message)


def main() -> None:
    # Parser
    parser = argparse.ArgumentParser(description="DAWGZ's CLI")

    parser.add_argument("workflow", default=None, nargs="?", type=int, help="workflow index")
    parser.add_argument("job", default=None, nargs="?", type=int, help="job index")
    parser.add_argument("i", default=None, nargs="?", type=int, help="job array index")

    parser.add_argument("-c", "--cancel", default=False, action="store_true")

    args = parser.parse_args()

    # Action
    if args.cancel:
        cancel(args.workflow, args.job, args.i)
    else:
        table(args.workflow, args.job, args.i)


if __name__ == "__main__":
    main()
