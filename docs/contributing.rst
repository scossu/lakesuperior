Contributing to Lakesuperior
============================

Lakesuperior has been so far a single person’s off-hours project (with much
valuable input from several sides). In order to turn into anything close
to a Beta release and eventually to a production-ready implementation, it
needs some community love.

Contributions are welcome in all forms, including ideas, issue reports,
or even just spinning up the software and providing some feedback.
Lakesuperior is meant to live as a community project.

.. _dev_setup:

Development Setup
-----------------

To set up the software for developing code, documentation, or tests::

    mkdir lsup # or whatever you may want to call it
    cd lsup
    python3 -m venv venv
    source bin/activate
    git clone --recurse-submodules https://github.com/scossu/lakesuperior.git src
    cd src
    pip install -e .

This will allow to alter the code without having to re-run ``pip install``
after changes (unless one is changing the Cython modules; see below).

Modifying Cython Modules
------------------------

Cython files must be recompiled into C files and then into binary files every
time they are changed. To recompile Lakesuperior modules, run::

    python setup.py --build_ext --inplace

For a faster compilation while testing, the environment variable ``CFLAGS``
can set to ``-O0`` to turn off compiler optimization. The runtime code may run
slower so this is not recommended for performance benchmarking.

Refer to the `Cython documentation
<http://docs.cython.org/en/latest/src/userguide/source_files_and_compilation.html>`__
for a detailed description of the Cython compilation process.

Contribution Guidelines
-----------------------

You can contribute by (from least to most involved):

- Installing the repository and reporting any issues
- Testing on other platforms (OS X, Windows, other Linux distros)
- Loading some real-world data set and sharing interesting results
- Amending incorrect documentation or adding missing one
- Adding test coverage (**HOT**)
- Browsing the list of open issues and picking a ticket that you may find
  interesting and within your reach
- Suggesting new functionality or improvements and/or implementing them

Please open a ticket and discuss the issue you are raising before opening a PR.

Documentation is critical. If you implement new modules, class or methods, or
modify them, please document them thoroughly and verify that the API docs are
displaying and linking correctly.

Likewise, please add mindful testing to new fatures or bug fixes.

Development is done on the ``development`` branch. If you have any suggested
addition to the code, please fork the repo, create a new branch for your topic
and open a pull request against development. In case you find a critical bug,
a hotfix can be proposed against master if agreed in the related issue
discussion.

Last but not least, read carefully the `Code of
Conduct <../../code_of_conduct.md>`__.
