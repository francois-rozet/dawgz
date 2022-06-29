r"""Command-line interface"""

import cloudpickle as pickle
import csv

from datetime import datetime
from pathlib import Path
from tabulate import tabulate
from typing import *

from .schedulers import DIR


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="DAWGZ's CLI")

    parser.add_argument('workflow', default=None, nargs='?', type=int, help="workflow index")
    parser.add_argument('job', default=None, nargs='?', type=int, help="job index")

    args = parser.parse_args()

    print(table(args.workflow, args.job))


def table(workflow: int = None, job: int = None) -> str:
    # Workflows
    path = Path(DIR)

    if (path / 'workflows.csv').exists():
        with open(path / 'workflows.csv') as f:
            workflows = list(csv.reader(f))
    else:
        workflows = []

    if workflow is None:
        headers = ('Name', 'ID', 'Date', 'Backend', 'Jobs', 'Errors')
        rows = [(w[0], w[1][:8], *w[2:]) for w in workflows]

        return tabulate(rows, headers, showindex=True)

    # Jobs
    _, uuid, _, backend = workflows[workflow][:4]

    with open(path / uuid / 'dump.pkl', 'rb') as f:
        workflow = pickle.load(f)

    jobs = list(workflow.order)

    if job is None:
        headers = ('Name', 'ID', 'State')
        rows = []

        for job in jobs:
            if backend == 'slurm' and job not in workflow.traces:
                jobid = workflow.results[job]
            else:
                jobid = None

            rows.append((str(job), jobid, workflow.state(job)))

        return tabulate(rows, headers, showindex=True)

    # Outputs
    job = jobs[job]

    headers = ('Name', 'State', 'Output')
    array = [None]

    if job in workflow.traces:
        rows = [[str(job), workflow.state(job), workflow.traces[job]]]
    elif job.array is None:
        rows = [[str(job), workflow.state(job), workflow.output(job)]]
    else:
        array = sorted(job.array)
        rows = [
            [f'{job.name}[{i}]', workflow.state(job, i), workflow.output(job, i)]
            for i in array
        ]

    return tabulate(rows, headers, showindex=array)
