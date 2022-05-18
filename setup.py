#!/usr/bin/env python

import setuptools

with open('README.md', 'r') as f:
    readme = f.read()

with open('requirements.txt', 'r') as f:
    required = f.read().splitlines()

setuptools.setup(
    name='dawgz',
    version='0.3.6',
    packages=setuptools.find_packages(),
    description='Directed Acyclic Workflow Graph Scheduling',
    keywords='acyclic workflow graph scheduling reproducible-science slurm hpc hpc-tools',
    long_description=readme,
    long_description_content_type='text/markdown',
    author='François Rozet, Joeri Hermans',
    author_email='francois.rozet@outlook.com',
    license='MIT license',
    url='https://github.com/francois-rozet/dawgz',
    project_urls={
        'Documentation': 'https://github.com/francois-rozet/dawgz',
        'Source': 'https://github.com/francois-rozet/dawgz',
        'Tracker': 'https://github.com/francois-rozet/dawgz/issues',
    },
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
    ],
    install_requires=required,
    python_requires='>=3.8',
    entry_points = {
        'console_scripts': [
            'dawgz = dawgz.cli:main',
        ],
    },
)
