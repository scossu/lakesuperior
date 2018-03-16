import logging

import click_log
from contextlib import ExitStack
from shutil import rmtree

import lmdb
import requests

from rdflib import Graph, URIRef

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.env import env
from lakesuperior.store.ldp_rs.lmdb_store import TxnManager

__doc__ = '''
Admin API.

This module contains maintenance utilities and stats.
'''

logger = logging.getLogger(__name__)
app_globals = env.app_globals


def stats():
    '''
    Get repository statistics.

    @return dict Store statistics, resource statistics.
    '''
    repo_stats = {'rsrc_stats': env.app_globals.rdfly.count_rsrc()}
    with TxnManager(env.app_globals.rdf_store) as txn:
        repo_stats['store_stats'] = env.app_globals.rdf_store.stats()

    return repo_stats


@click_log.simple_verbosity_option(logger)
def dump(
        src, dest, start=('/',), binary_handling='include',
        compact_uris=False):
    '''
    Dump a whole LDP repository or parts of it to disk.

    @param src (rdflib.term.URIRef) Webroot of source repository. This must
    correspond to the LDP root node (for Fedora it can be e.g.
    `http://localhost:8080fcrepo/rest/`) and is used to determine if URIs
    retrieved are managed by this repository.
    @param dest (rdflib.URIRef) Base URI of the destination. This can be any
    container in a LAKEsuperior server. If the resource exists, it must be an
    LDP container. If it does not exist, it will be created.
    @param start (tuple|list) List of starting points to retrieve resources
    from. It would typically be the repository root in case of a full dump
    or one or more resources in the repository for a partial one.
    @param binary_handling (string) One of 'include', 'truncate' or 'split'.
    @param compact_uris (bool) NOT IMPLEMENTED. Whether the process should
    attempt to compact URIs generated with broken up path segments. If the UID
    matches a pattern such as `/12/34/56/123456...` it is converted to
    `/123456...`. This would remove a lot of cruft caused by the pairtree
    segments. Note that this will change the publicly exposed URIs. If
    durability is a concern, a rewrite directive can be added to the HTTP
    server that proxies the WSGI endpoint.
    '''
    # 1. Retrieve list of resources.
    if not isinstance(start, list) and not isinstance(start, tuple):
        start = (start,)
    _gather_resources(src, start)


def _gather_resources(webroot, start_pts):
    '''
    Gather all resources recursively and save them to temporary store.

    Resource UIDs (without the repository webroot) are saved as unique keys
    in a temporary store.

    @param webroot (string) Base URI of the repository.
    @param start_pts (tuple|list) Starting points to gather.
    '''
    dbpath = '/var/tmp/fcrepo_migration_data'
    rmtree(dbpath, ignore_errors=True)
    with lmdb.open(
            dbpath, 1024 ** 4, metasync=False, readahead=False,
            meminit=False) as db:
        #import pdb; pdb.set_trace()
        for start in start_pts:
            if not start.startswith('/'):
                raise ValueError(
                        'Starting point {} does not begin with a slash.'
                        .format(start))

            _gather_refs(db, webroot, start)


@click_log.simple_verbosity_option(logger)
def _gather_refs(db, base, path):
    '''
    Get the UID of a resource and its relationships recursively.

    This method recurses into itself each time a reference to a resource
    managed by the repository is encountered.

    @param base (string) Base URL of repository. This is used to determine
    whether encountered URI terms are repository-managed.
    @param base (string) Path, relative to base URL, of the resource to gather.
    '''
    pfx = base.rstrip('/')
    # Public URI of source repo.
    uri = pfx + path
    # Internal URI of destination.
    iuri = uri.replace(pfx, nsc['fcres'])
    ibase = base.replace(pfx, nsc['fcres'])

    rsp = requests.head(uri)
    rsp.raise_for_status()

    # Determine LDP type.
    ldp_type = 'ldp_nr'
    for link in requests.utils.parse_header_links(rsp.headers.get('link')):
        if (
                link.get('rel') == 'type'
                and link.get('url') == str(nsc['ldp'].RDFSource)):
            ldp_type = 'ldp_rs'
            break

    if ldp_type == 'ldp_rs':
        # Get the whole RDF document now because we have to know all outbound
        # links.
        get_uri = uri
    else:
        get_uri = uri + '/fcr:metadata'

    get_req = requests.get(get_uri)
    get_req.raise_for_status()
    data = get_req.content.replace(base.encode('utf-8'), ibase.encode('utf-8'))
    logger.debug('Localized data: {}'.format(data.decode('utf-8')))
    gr = Graph(identifier=iuri).parse(data=data, format='turtle')

    # First store the resource, so when we recurse, a resource referring back
    # to this resource will skip it as already existing and avoid an infinite
    # loop.
    #
    # The RDF data stream inserted is the turtle-serialized bytestring as it
    # comes from the request.
    with db.begin(write=True) as txn:
        with txn.cursor() as cur:
            if not cur.set_key(iuri.encode('utf-8')):
                cur.put(uri.encode('utf-8'), data)

    # Now, crawl through outbound links.
    # LDP-NR fcr:metadata must be checked too.
    for pred, obj in gr.predicate_objects():
        if (
                isinstance(obj, URIRef)
                and obj.startswith(iuri)
                and pred != nsc['fcrepo'].hasParent):
            with db.begin() as txn:
                with txn.cursor() as cur:
                    # Avoid ∞
                    if cur.set_key(obj.encode('utf-8')):
                        continue
            _gather_refs(db, base, obj.replace(ibase, ''))
