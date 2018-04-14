Command Line Reference
======================

LAKEsuperior comes with some command-line tools aimed at several purposes.

If LAKEsuperior is installed via ``pip``, all tools can be invoked as normal
commands (i.e. they are in the virtualenv ``PATH``). 

The tools are currently not directly available on Docker instances (*TODO add
instructions and/or code changes to access them*).

``fcrepo``
----------

This is the main server command. It has no parameters. The command spawns
Gunicorn workers (as many as set up in the configuration) and can be sent in
the background, or started via init script.

The tool must be run in the same virtual environment LAKEsuperior
was installed in (if it was)—i.e.::

    source <virtualenv root>/bin/activate

must be run before running the server.

In the case an init script is used, ``coilmq`` (belonging to a 3rd party
package) needs to be launched as well; unless a message broker is already set
up, or if messaging is disabled in the configuration.

``lsup-admin``
--------------

``lsup-admin`` is the principal repository management tool. It is
self-documented, so this is just a redundant overview::

    $ lsup-admin
    Usage: lsup-admin [OPTIONS] COMMAND [ARGS]...

    Options:
      --help  Show this message and exit.

    Commands:
      bootstrap     Bootstrap binary and graph stores.
      check_fixity  [STUB] Check fixity of a resource.
      check_refint  Check referential integrity.
      cleanup       [STUB] Clean up orphan database items.
      migrate       Migrate an LDP repository to LAKEsuperior.
      stats         Print repository statistics.

All entries marked ``[STUB]`` are not yet implemented, however the
``lsup_admin <command> --help`` command will issue a description of what
the command is meant to do. Check the
`issues page <https://github.com/scossu/lakesuperior/issues>`__ for what's on
the radar.

All of the above commands are also available via, and based upon, the
native Python API.

``lsup-benchmark``
------------------

``lsup-benchmark`` is used to run performance tests in a predictable way.

The command has no options but prompts the user for a few settings
interactively (N.B. this may change in favor of parameters).

The benchmark tool is able to create RDF sources, or non-RDF, or an equal mix
of them, via POST or PUT, in the currently running LAKEsuperior server. It
runs single-threaded.

The RDF sources are randomly generated graphs of consistent size and
complexity. They include a mix of in-repository references, literals, and
external URIs. Each graph has 200 triples.

The non-RDF sources are randomly generated 1024x1024 pixel PNG images.

You are warmly encouraged to run the script and share the performance results (
*TODO add template for posting results*).

``profiler``
------------

This command launches a single-threaded HTTP server (Flask) on port 5000 that
logs profiling information. This is useful for analyzing application
performance.

For more information, consult the `Python profilers guide
<https://docs.python.org/3/library/profile.html>`__.

Do not launch this while a WSGI server (``fcrepo``) is already running, because
that also launches a Flask server on port 5000.
