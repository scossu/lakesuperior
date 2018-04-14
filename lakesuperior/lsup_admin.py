import click
import click_log
import csv
import json
import logging
import sys

from os import getcwd, path

import arrow

from lakesuperior import env
from lakesuperior.api import admin as admin_api
from lakesuperior.config_parser import config
from lakesuperior.globals import AppGlobals
from lakesuperior.store.ldp_rs.lmdb_store import TxnManager

__doc__="""
Utility to perform core maintenance tasks via console command-line.

The command-line tool is self-documented. Type::

    lsup-admin --help

for a list of tools and options.
"""

logger = logging.getLogger(__name__)
click_log.basic_config(logger)


@click.group()
def admin():
    pass

@click.command()
def bootstrap():
    """
    Bootstrap binary and graph stores.

    This script will parse configuration files and initialize a filesystem and
    triplestore with an empty FCREPO repository.
    It is used in test suites and on a first run.

    Additional scaffolding files may be parsed to create initial contents.
    """
    import lakesuperior.env_setup

    rdfly = env.app_globals.rdfly
    nonrdfly = env.app_globals.nonrdfly

    click.echo(
            click.style(
                'WARNING: This operation will WIPE ALL YOUR DATA.\n',
                bold=True, fg='red')
            + 'Are you sure? (Please type `yes` to continue) > ', nl=False)
    choice = input().lower()
    if choice != 'yes':
        click.echo('Aborting.')
        sys.exit(1)

    click.echo('Initializing graph store at {}'.format(rdfly.store.path))
    with TxnManager(env.app_globals.rdf_store, write=True) as txn:
        rdfly.bootstrap()
        rdfly.store.close()
    click.echo('Graph store initialized.')

    click.echo('Initializing binary store at {}'.format(nonrdfly.root))
    nonrdfly.bootstrap()
    click.echo('Binary store initialized.')
    click.echo('\nRepository successfully set up. Go to town.')
    click.echo('If the HTTP server is running, it must be restarted.')


@click.command()
@click.option(
    '--human', '-h', is_flag=True, flag_value=True,
    help='Print a human-readable string. By default, JSON is printed.')
def stats(human=False):
    """
    Print repository statistics.

    @param human (bool) Whether to output the data in human-readable
    format.
    """
    stat_data = admin_api.stats()
    if human:
        click.echo(
            'This option is not supported yet. Sorry.\nUse the `/admin/stats`'
            ' endpoint in the web UI for a pretty printout.')
    else:
        click.echo(json.dumps(stat_data))


@click.command()
def check_fixity(uid):
    """
    [STUB] Check fixity of a resource.
    """
    pass


@click.option(
    '--config-folder', '-c', default=None, help='Alternative configuration '
    'folder to look up. If not set, the location set in the environment or '
    'the default configuration is used.')
@click.option(
    '--output', '-o', default=None, help='Output file. If not specified, a '
    'timestamp-named file will be generated automatically.')
@click.command()
def check_refint(config_folder=None, output=None):
    """
    Check referential integrity.

    This command scans the graph store to verify that all references to
    resources within the repository are effectively pointing to existing
    resources. For repositories set up with the `referential_integrity` option
    (the default), this is a pre-condition for a consistent data set.

    If inconsistencies are found, a report is generated in CSV format with the
    following columns: `s`, `p`, `o` (respectively the terms of the
    triple containing the dangling relationship) and `missing` which
    indicates which term is the missing URI (currently always set to `o`).

    Note: this check can be run regardless of whether the repository enforces
    referential integrity.
    """
    if config_folder:
        env.app_globals = AppGlobals(parse_config(config_dir))
    else:
        import lakesuperior.env_setup

    check_results = admin_api.integrity_check()

    click.echo('Integrity check results:')
    if len(check_results):
        click.echo(click.style('Inconsistencies found!', fg='red', bold=True))
        if not output:
            output = path.join(getcwd(), 'refint_report-{}.csv'.format(
                arrow.utcnow().format('YYYY-MM-DDTHH:mm:ss.S')))
        elif not output.endswith('.csv'):
            output += '.csv'

        with open(output, 'w', newline='') as fh:
            writer = csv.writer(fh)
            writer.writerow(('s', 'p', 'o', 'missing'))
            for trp in check_results:
                # ``o`` is always hardcoded for now.
                writer.writerow([t.n3() for t in trp[0]] + ['o'])

        click.echo('Report generated at {}'.format(output))
    else:
        click.echo(click.style('Clean. ', fg='green', bold=True)
                + 'No inconsistency found. No report generated.')


@click.command()
def cleanup():
    """
    [STUB] Clean up orphan database items.
    """
    pass


@click.command()
@click.argument('src')
@click.argument('dest')
@click.option(
    '--start', '-s', show_default=True,
    help='Starting point for looking for resources in the repository.\n'
    'The default `/` value starts at the root, i.e. migrates the whole '
    'repository.')
@click.option(
    '--list-file', '-l', help='Path to a local file containing URIs to be '
    'used as starting points, one per line. Use this alternatively to `-s`. '
    'The URIs can be relative to the repository root (e.g. `/a/b/c`) or fully '
    'qualified (e.g. `https://example.edu/fcrepo/rest/a/b/c`).')
@click.option(
    '--zero-binaries', '-z', is_flag=True,
    help='If set, binaries are created as zero-byte files in the proper '
    'folder structure rather than having their full content copied.')
@click.option(
    '--skip-errors', '-e', is_flag=True,
    help='If set, when the application encounters an error while retrieving '
    'a resource from the source repository, it will log the error rather than '
    'quitting. Other exceptions caused by the application will terminate the '
    'process as usual.')
@click_log.simple_verbosity_option(logger)
def migrate(src, dest, start, list_file, zero_binaries, skip_errors):
    """
    Migrate an LDP repository to LAKEsuperior.

    This utility creates a fully functional LAKEshore repository from an
    existing repository. The source repo can be LAKEsuperior or
    another LDP-compatible implementation.

    A folder will be created in the location indicated by ``dest``. If the
    folder exists already, it will be deleted and recreated. The folder will be
    populated with the RDF and binary data directories and a default
    configuration directory. The new repository can be immediately started
    from this location.
    """
    logger.info('Migrating {} into a new repository on {}.'.format(
            src, dest))
    entries = admin_api.migrate(
            src, dest, start_pts=start, list_file=list_file,
            zero_binaries=zero_binaries, skip_errors=skip_errors)
    logger.info('Migrated {} resources.'.format(entries))
    logger.info("""Migration complete. To start the new repository, from the
    directory you launched this script run:

    FCREPO_CONFIG_DIR="{}/etc" ./fcrepo

    Make sure that the default port is not being used by another repository.
    """.format(dest))


admin.add_command(bootstrap)
admin.add_command(check_fixity)
admin.add_command(check_refint)
admin.add_command(cleanup)
admin.add_command(migrate)
admin.add_command(stats)

if __name__ == '__main__':
    admin()
