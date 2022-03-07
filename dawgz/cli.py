r"""Command-line interface"""

import cloudpickle as pickle
import json

from datetime import datetime
from pathlib import Path
from tabulate import tabulate
from typing import *

from .schedulers import DIR


def main() -> str:
    import argparse
    import os
    import sys

    sys.path.append(os.getcwd())

    parser = argparse.ArgumentParser(description="DAWGZ's CLI")

    parser.add_argument('workflow', default=None, nargs='?', type=int, help="workflow index")
    parser.add_argument('job', default=None, nargs='?', type=int, help="job index")
    parser.add_argument('indices', default=[], nargs='*', type=int, help="array indices")

    args = parser.parse_args()

    return table(args.workflow, args.job, args.indices)


def table(workflow: int = None, job: int = None, indices: List[int] = []) -> str:
    path = Path(DIR)
    path.mkdir(parents=True, exist_ok=True)

    # Workflows
    workflows = sorted(path.iterdir(), key=lambda p: p.stat().st_mtime)

    if workflow is None:
        headers = ['Name', 'ID', 'Date', 'Backend', 'Jobs', 'Errors']
        rows = []

        for index, workflow in enumerate(workflows):
            with open(workflow / 'info.json') as f:
                info = json.load(f)

            info['date'] = datetime.fromisoformat(info['date'])

            rows.append([info.get(h.lower(), None) for h in headers])

        return tabulate(rows, headers, showindex=True)

    # Jobs
    with open(workflows[workflow] / 'graph.pkl', 'rb') as f:
        workflow = pickle.load(f)

    jobs = list(workflow.order)

    if job is None:
        headers = ['Name', 'ID', 'State']
        rows = []

        for job in jobs:
            rows.append([str(job), None, workflow.state(job)])

            if workflow.backend == 'slurm':
                if job not in workflow.traces:
                    rows[-1][1] = workflow.results[job]

        return tabulate(rows, headers, showindex=True)

    # Outputs
    job = jobs[job]

    headers = ['Name', 'State', 'Output']
    array = [None]

    if job in workflow.traces:
        rows = [[str(job), workflow.state(job), workflow.traces[job]]]
    elif job.array is None:
        rows = [[str(job), workflow.state(job), workflow.output(job)]]
    else:
        if indices:
            array = set(indices).intersection(job.array)
        else:
            array = job.array

        array = sorted(array)
        rows = [
            [f'{job.name}[{i}]', workflow.state(job, i), workflow.output(job, i)]
            for i in array
        ]

    return tabulate(rows, headers, showindex=array)


if __name__ == '__main__':
    main()
