r"""Module's main"""

import cloudpickle as pickle
import csv

from pathlib import Path
from tabulate import tabulate
from typing import *

from .schedulers import DIR


def table(workflow: int = None, job: int = None) -> str:
    path = Path(DIR)
    record = path / 'workflows.csv'

    if record.exists():
        with open(record) as f:
            workflows = list(csv.reader(f))
    else:
        workflows = []

    if workflow is None:
        headers = ('Name', 'ID', 'Date', 'Backend', 'Jobs', 'Errors')
        rows = [(w[0], w[1][:8], *w[2:]) for w in workflows]

        return tabulate(rows, headers, showindex=True)
    else:
        uuid = workflows[workflow][1]

        with open(path / uuid / 'dump.pkl', 'rb') as f:
            scheduler = pickle.load(f)

        if job is None:
            return scheduler.report()
        else:
            jobs = list(scheduler.order)
            job = jobs[job]

            return scheduler.report(job)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="DAWGZ's CLI")

    parser.add_argument('workflow', default=None, nargs='?', type=int, help="workflow index")
    parser.add_argument('job', default=None, nargs='?', type=int, help="job index")

    args = parser.parse_args()

    print(table(args.workflow, args.job))


if __name__ == '__main__':
    main()
