#! /usr/bin/env python

from setuptools import setup

setup(
    name='lofn',
    packages=['lofn', 'lofn.base'],
    version='0.4.0',
    description='Lightweight Orchestration For Now: Wrapper for serial tools '
                'using Spark and Docker to parallelize',
    author='Michael T. Neylon',
    author_email='neylon_michael_t@lilly.com',
    url='https://github.com/michaeltneylon/lofn',
    download_url='https://github.com/michaeltneylon/lofn/archive/0.4.0.tar.gz',
    license='Apache License 2.0',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Topic :: System :: Distributed Computing'
    ]
)
