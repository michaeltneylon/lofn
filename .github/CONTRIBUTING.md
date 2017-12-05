# How to Contribute

We welcome contributions in the form of pull requests or issues. Open an
issue for a bug report or enhancement proposal.
In order to create a pull request, make a fork of the repository and follow the
guidelines on this page.

## Testing

See the tests directory README on how to run the tests. There are two main test
suites that need to run successfully before a pull request can be merged. There
is a local, standalone Spark deployment test set and another set for a Spark
on YARN cluster. Both require Docker and Spark, and the latter requires a YARN
cluster.

If you are adding new functionality or features, please add tests to cover them.

If you have found a bug, please write a test for the bug that then passes with
 your changes.

### YARN Testing

An AWS EMR setup is included in the docs. Follow the setup to install Docker,
but do not install lofn from stable. Instead, follow the tests README
instructions on how to run the tests using the lofn egg from your fork.

## Coding Conventions

Follow PEP8 standards in your code. Run pylint to make sure your contributions
meet these standards before they can be merged.

`pylint lofn`

## Submitting Changes

Create an issue with the bug report or proposed enhancements and reference the
issue in your pull request.

Open a pull request with a clear list of changes. Helpful, atomic commit
messages will aid in our understanding of your changes and expedite merging.
If the change is fairly large, a multi-line commit message is ideal:

```
$ git commit -m 'brief summary
>
> changes described in some more detail here'
```
