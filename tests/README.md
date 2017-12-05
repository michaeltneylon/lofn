# Tests

Write tests for lofn and run them when developing.

## Setup

Install the version of lofn you are testing to run them.
It may be useful to use a virtualenv for test installations.

**Note on virtualenvs:**
If you are using a virtualenv, it may be safer
to run pytest as a python module, `python -m pytest [...]` to ensure the correct environment.

Some dependencies need to be installed to run the tests:

`pip install -r requirements.txt`

## System

First test that your system is ready to run lofn.
Inside the tests dir, run:

`python -m unittest system`

## lofn tests

These tests are using pytest to get spark fixtures, so you need to make sure to install pyspark into your
PYTHONPATH since we won't execute these with `spark-submit`

To simplify this, `findspark` module does all of this for us, so that is being used
in the test. make sure to install requirements.

Spark can be deployed to various masters which have different architecture. The tests are separated into two files for
testing on `standalone` or `yarn` deployment modes.

## Standalone

To run tests for standalone mode use:

`pytest local_tests.py`

## YARN

Run tests in YARN using:

If lofn is installed on each node in the cluster:

`pytest yarn_tests.py`

To support automated testing, we use Spark's PyFiles feature to ship the package to each executor in a YARN cluster.
To use this, specify an egg or zipball of the lofn package with `--pyfile`. After an installation using the setup.py
file, an egg is created in the dist folder, so the command to run the test with that would look like:

`pytest yarn_tests.py --pyfile ../dist/lofn*egg`

## Code Coverage

Using the pytest-cov plugin for code coverage, we can run all of our tests along with a report of code coverage.

Run the following command inside the 'tests' directory to execute tests along with code coverage analysis:

`pytest --cov lofn yarn_tests.py`

To see which lines are not covered:

`pytest --cov lofn --cov-report term-missing yarn_tests.py`
