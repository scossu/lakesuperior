Migration, Backup & Restore
===========================

All Lakesuperior data is by default fully contained in a folder. This
means that only the data, configurations and code folders are needed for
it to run. No Postgres, Redis, or such. Data and configuration folders
can be moved around as needed.

Migration Tool
--------------

Migration is the process of importing and converting data from a
different Fedora or LDP implementation into a new Lakesuperior instance.
This process uses the HTTP/LDP API of the original repository. A
command-line utility is available as part of the ``lsup-admin`` suite to
assist in such operation.

A repository can be migrated with a one-line command such as:

::

    lsup-admin migrate http://source-repo.edu/rest /local/dest/folder

For more options, enter

::

    lsup-admin migrate --help

The script will crawl through the resources and crawl through outbound
links within them. In order to do this, resources are added as raw
triples, i.e. no consistency checks are made.

This script will create a full dataset in the specified destination
folder, complete with a default configuration that allows to start the
Lakesuperior server immediately after the migration is complete.

Two approaches to migration are possible:

1. By providing a starting point on the source repository. E.g. if the
   repository you want to migrate is at ``http://repo.edu/rest/prod``
   you can add the ``-s /prod`` option to the script to avoid migrating
   irrelevant branches. Note that the script will still reach outside of
   the starting point if resources are referencing other resources
   outside of it.
2. By providing a file containing a list of resources to migrate. This
   is useful if a source repository cannot produce a full list (e.g. the
   root node has more children than the server can handle) but a list of
   individual resources is available via an external index (Solr,
   triplestore, etc.). The resources can be indicated by their fully
   qualified URIs or paths relative to the repository root. (*TODO
   latter option needs testing*)

Consistency check can (and should) be run after the migration::

    lsup-admin check_refint

This is critical to ensure that all resources in the repository are referencing
to other repository resources that are actually existing.

This feature has been added in alpha9.

*TODO: The output of ``check_refint`` is somewhat crude. Improvements can be
made to output integrity violations to a machine-readable log and integrate
with the migration tool.*

Backup And Restore
------------------

A back up of a LAKEshore repository consists in copying the RDF and
non-RDF data folders. These folders are indicated in the application
configuration. The default commands provided by your OS (``cp``,
``rsync``, ``tar`` etc. for Unix) are all is needed.
