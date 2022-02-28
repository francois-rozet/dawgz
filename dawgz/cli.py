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

    args = parser.parse_args()

    return table(args.workflow, args.job)


def table(workflow: int = None, job: int = None) -> str:
    path = Path(DIR)
    path.mkdir(parents=True, exist_ok=True)

    # Workflows
    workflows = sorted(path.iterdir(), key=lambda p: p.stat().st_mtime)

    if workflow is None:
        headers = ['Name', 'Date', 'Backend', 'Jobs', 'Errors']
        rows = []

        for index, workflow in enumerate(workflows):
            with open(workflow / 'info.json') as f:
                info = json.load(f)

            info['date'] = datetime.fromisoformat(info['date'])

            rows.append([info[h.lower()] for h in headers])

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
    indices = [None]

    if job in workflow.traces:
        rows = [[str(job), workflow.state(job), workflow.traces[job]]]
    elif job.array is None:
        rows = [[str(job), workflow.state(job), workflow.output(job)]]
    else:
        indices = sorted(job.array)
        rows = [
            [f'{job.name}[{i}]', workflow.state(job, i), workflow.output(job, i)]
            for i in indices
        ]

    return tabulate(rows, headers, showindex=indices)


if __name__ == '__main__':
    main()
