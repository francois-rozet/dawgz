from dawgz import after, job, schedule, leafs

@job(cpus='2', memory='4GB', array=5)
def generate(i: int):
    print(f'Generating data block {i}.')

@after(generate)
@job(cpus='1', memory='2GB', array=5)
def postprocess(i: int):
    print(f'Postprocessing data block {i}.')

def do_experiment(parameter):
    r"""This method allocates a `fit` and `make_plot` job
    based on the specified parameter."""

    @after(postprocess)
    @job(name=f'fit_{parameter}')  # By default, the name is equal to the function name
    def fit():
        print(f'Fit {parameter}.')

    @after(fit)
    @job(name=f'plt_{parameter}')  # Simplifies the identification of the logfile
    def make_plot():
        print(f'Plot {parameter}.')

# Programmatically build workflow
for parameter in [0.1, 0.2, 0.3, 0.4, 0.5]:
    do_experiment(parameter)

jobs = leafs(generate)  # Find all terminal nodes of workflow graph
schedule(*jobs, backend='local')
