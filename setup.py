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
    long_description=readme,
    long_description_content_type='text/markdown',
    keywords='acyclic workflow graph scheduling',
    author='François Rozet',
    author_email='francois.rozet@outlook.com',
    url='https://github.com/francois-rozet/dawgz',
    install_requires=required,
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.9',
)
