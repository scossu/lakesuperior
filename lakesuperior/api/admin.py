import logging
import os

import click_log
from contextlib import ExitStack
from shutil import rmtree

import lmdb
import requests

from rdflib import Graph, URIRef

import lakesuperior.env_setup

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.env import env
from lakesuperior.store.ldp_rs.lmdb_store import TxnManager
from lakesuperior.store.ldp_nr.default_layout import DefaultLayout as FileLayout

__doc__ = '''
Admin API.

This module contains maintenance utilities and stats.
'''

logger = logging.getLogger(__name__)
app_globals = env.app_globals

_ignore_list = (
    nsc['fcrepo'].hasParent,
    nsc['fcrepo'].hasTransactionProvider,
)


def stats():
    '''
    Get repository statistics.

    @return dict Store statistics, resource statistics.
    '''
    repo_stats = {'rsrc_stats': env.app_globals.rdfly.count_rsrc()}
    with TxnManager(env.app_globals.rdf_store) as txn:
        repo_stats['store_stats'] = env.app_globals.rdf_store.stats()

    return repo_stats


def dump(
        src, dest, start=('/',), binary_handling='include',
        compact_uris=False):
    '''
    Dump a whole LDP repository or parts of it to disk.

    @param src (rdflib.term.URIRef) Webroot of source repository. This must
    correspond to the LDP root node (for Fedora it can be e.g.
    `http://localhost:8080fcrepo/rest/`) and is used to determine if URIs
    retrieved are managed by this repository.
    @param dest (str) Local path of the destination. If the location exists it
    must be a writable directory. It will be deleted and recreated. If it does
    not exist, it will be created along with its parents if missing.
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
    start_pts = (
            (start,)
            if not isinstance(start, list) and not isinstance(start, tuple)
            else start)

    dbpath = '{}/ldprs_store'.format(dest)
    rmtree(dbpath, ignore_errors=True)
    os.makedirs(dbpath)
    fpath = '{}/ldpnr_store'.format(dest)
    rmtree(fpath, ignore_errors=True)
    os.makedirs(fpath)

    with lmdb.open(
            dbpath, 1024 ** 4, metasync=False, readahead=False,
            meminit=False) as db:
        for start in start_pts:
            if not start.startswith('/'):
                raise ValueError(
                        'Starting point {} does not begin with a slash.'
                        .format(start))

            _gather_refs(db, src, start, dest)
        entries = db.stat()['entries']
        logger.info('Dumped {} resources.'.format(entries))

    return entries


def _gather_refs(db, base, path, dest):
    '''
    Get the UID of a resource and its relationships recursively.

    This method recurses into itself each time a reference to a resource
    managed by the repository is encountered.

    @param base (string) Base URL of repository. This is used to determine
    whether encountered URI terms are repository-managed.
    @param path (string) Path, relative to base URL, of the resource to gather.
    @param dest (string) Local path for RDF database and non-RDF files.
    '''
    pfx = base.rstrip('/')
    # Public URI of source repo.
    uri = pfx + path
    # Internal URI of destination.
    iuri = URIRef(uri.replace(pfx, nsc['fcres']))
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

    # Get the whole RDF document now because we have to know all outbound
    # links.
    get_uri = uri if ldp_type == 'ldp_rs' else '{}/fcr:metadata'.format(uri)
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

    # Grab binary.
    if ldp_type == 'ldp_nr':
        bin_resp = requests.get('{}/fcr:content'.format(uri))
        bin_resp.raise_for_status()

        # @FIXME Use a more robust checking mechanism. Maybe offer the option
        # to verify the content checksum.
        cnt_hash = gr.value(iuri, nsc['premis'].hasMessageDigest).replace(
                'urn:sha1:', '')
        fpath = FileLayout.local_path('{}/ldpnr_store'.format(dest), cnt_hash)
        os.makedirs(os.path.dirname(fpath), exist_ok=True)
        with open(fpath, 'wb') as f:
            f.write(bin_resp.content)

    # Now, crawl through outbound links.
    # LDP-NR fcr:metadata must be checked too.
    for pred, obj in gr.predicate_objects():
        if (
                isinstance(obj, URIRef)
                and obj.startswith(iuri)
                and pred not in _ignore_list):
            with db.begin() as txn:
                with txn.cursor() as cur:
                    # Avoid ∞
                    if cur.set_key(obj.encode('utf-8')):
                        continue
            _gather_refs(db, base, obj.replace(ibase, ''), dest)
