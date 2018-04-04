LAKEsuperior Command Line Reference
===================================

The LAKEsuperior command line tool is used for maintenance and
administration purposes.

The script should be in your executable path if you install LAKEsuperior with
``pip``. The tool is self-documented, so this is just a redundant overview::

    $ lsup_admin
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

*TODO: Add instructions to access from Docker.*

All entries marked ``[STUB]`` are not yet implemented, however the
``lsup_admin <command> --help`` command will issue a description of what
the command is meant to do. Check the
`issues page <https://github.com/scossu/lakesuperior/issues>`__ for what's on
the radar.

All of the above commands are also available via, and based upon, the
native Python API.
