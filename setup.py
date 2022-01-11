#!/usr/bin/env python

import setuptools
import dawgz

with open('README.md', 'r') as f:
    readme = f.read()

with open('requirements.txt', 'r') as f:
    required = f.read().splitlines()

setuptools.setup(
    name='dawgz',
    version=dawgz.__version__,
    description='Directed Acyclic Workflow Graph Scheduling',
    keywords='workflow-engine hpc slurm hpc-tools reproducible-science acyclic workflow graph scheduling',
    long_description=readme,
    long_description_content_type='text/markdown',
    author='François Rozet, Joeri Hermans, Arnaud Delaunoy',
    author_email='francois.rozet@outlook.com',
    url='https://github.com/francois-rozet/dawgz',
    install_requires=required,
    license='MIT license',
    packages=setuptools.find_packages(),
    project_urls={
        'Documentation': 'https://github.com/francois-rozet/dawgz',
        'Source': 'https://github.com/francois-rozet/dawgz',
        'Tracker': 'https://github.com/francois-rozet/dawgz/issues',
    },
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.10'
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    python_requires='>=3.8',
)
