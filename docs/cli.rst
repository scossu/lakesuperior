LAKEsuperior Command Line Reference
===================================

The LAKEsuperior command line tool is used for maintenance and
administration purposes.

The script is invoked from the main install directory. The tool is
self-documented, so this is just a redundant overview:

::

    $ ./lsup_admin
    Usage: lsup-admin [OPTIONS] COMMAND [ARGS]...

    Options:
      --help  Show this message and exit.

      bootstrap     Bootstrap binary and graph stores.
      check_fixity  [STUB] Check fixity of a resource.
      check_refint  [STUB] Check referential integrity.
      cleanup       [STUB] Clean up orphan database items.
      copy          [STUB] Copy (backup) repository data.
      dump          [STUB] Dump repository to disk.
      load          [STUB] Load serialized repository data.
      stats         Print repository statistics.

All entries marked ``[STUB]`` are not yet implemented, however the
``lsup_admin <command> --help`` command will issue a description of what
the command is meant to do. Please see the `TODO <TODO>`__ document for
a rough road map.

All of the above commands are also available via, and based upon, the
native Python API.
