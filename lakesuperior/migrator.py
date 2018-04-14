import logging
import shutil

from contextlib import ContextDecorator
from os import makedirs, path
from urllib.parse import urldefrag

import requests
import yaml

from rdflib import Graph, URIRef

from lakesuperior import env, basedir
from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.exceptions import InvalidResourceError
from lakesuperior.globals import AppGlobals, ROOT_UID
from lakesuperior.config_parser import parse_config
from lakesuperior.store.ldp_rs.lmdb_store import TxnManager


logger = logging.getLogger(__name__)


class StoreWrapper(ContextDecorator):
    """
    Open and close a store.
    """
    def __init__(self, store):
        self.store = store

    def __enter__(self):
        self.store.open(env.app_globals.rdfly.config)

    def __exit__(self, *exc):
        self.store.close()


class Migrator:
    """
    Class to handle a database migration.

    This class holds state of progress and shared variables as it crawls
    through linked resources in an LDP server.

    Since a repository migration can be a very long operation but it is
    impossible to know the number of the resources to gather by LDP interaction
    alone, a progress ticker outputs the number of processed resources at
    regular intervals.
    """

    db_params = {
        'map_size': 1024 ** 4,
        'metasync': False,
        'readahead': False,
        'meminit': False,
    }
    """
    LMDB database parameters.

    See :meth:`lmdb.Environment.__init__`
    """

    ignored_preds = (
        nsc['fcrepo'].hasParent,
        nsc['fcrepo'].hasTransactionProvider,
        nsc['fcrepo'].hasFixityService,
    )
    """List of predicates to ignore when looking for links."""


    def __init__(
            self, src, dest, clear=False, zero_binaries=False,
            compact_uris=False, skip_errors=False):
        """
        Set up base paths and clean up existing directories.

        :param rdflib.URIRef src: Webroot of source repository. This must
            correspond to the LDP root node (for Fedora it can be e.g.
            ``http://localhost:8080fcrepo/rest/``) and is used to determine if
            URIs retrieved are managed by this repository.
        :param str dest: Destination repository path. If the location exists
            it must be a writable directory. It will be deleted and recreated.
            If it does not exist, it will be created along with its parents if
            missing.
        :param bool clear: Whether to clear any pre-existing data at the
            locations indicated.
        :param bool zero_binaries: Whether to create zero-byte binary files
            rather than copy the sources.
        :param bool compact_uris: NOT IMPLEMENTED. Whether the process should
            attempt to compact URIs generated with broken up path segments. If
            the UID matches a pattern such as ``/12/34/56/123456...`` it is
            converted to ``/123456...``. This would remove a lot of cruft
            caused by the pairtree segments. Note that this will change the
            publicly exposed URIs. If durability is a concern, a rewrite
            directive can be added to the HTTP server that proxies the WSGI
            endpoint.
        """
        # Set up repo folder structure and copy default configuration to
        # destination file.
        self.dbpath = '{}/data/ldprs_store'.format(dest)
        self.fpath = '{}/data/ldpnr_store'.format(dest)
        self.config_dir = '{}/etc'.format(dest)

        if clear:
            shutil.rmtree(dest, ignore_errors=True)
        if not path.isdir(self.config_dir):
            shutil.copytree(
                '{}/etc.defaults'.format(basedir), self.config_dir)

        # Modify and overwrite destination configuration.
        orig_config = parse_config(self.config_dir)
        orig_config['application']['store']['ldp_rs']['location'] = self.dbpath
        orig_config['application']['store']['ldp_nr']['path'] = self.fpath

        if clear:
            with open('{}/application.yml'.format(self.config_dir), 'w') \
                    as config_file:
                config_file.write(yaml.dump(orig_config['application']))

        env.app_globals = AppGlobals(parse_config(self.config_dir))

        self.rdfly = env.app_globals.rdfly
        self.nonrdfly = env.app_globals.nonrdfly

        if clear:
            with TxnManager(env.app_globals.rdf_store, write=True) as txn:
                self.rdfly.bootstrap()
                self.rdfly.store.close()
            env.app_globals.nonrdfly.bootstrap()

        self.src = src.rstrip('/')
        self.zero_binaries = zero_binaries
        self.skip_errors = skip_errors



    def migrate(self, start_pts=None, list_file=None):
        """
        Migrate the database.

        This method creates a fully functional and configured LAKEsuperior
        data set contained in a folder from an LDP repository.

        :param start_pts: List of starting points to retrieve
            resources from. It would typically be the repository root in case
            of a full dump or one or more resources in the repository for a
            partial one.
        :type start_pts: tuple or list
        :param str list_file: path to a local file containing a list of URIs,
            one per line.
        """
        from lakesuperior.api import resource as rsrc_api
        self._ct = 0
        with StoreWrapper(self.rdfly.store):
            if start_pts:
                for start in start_pts:
                    if not start.startswith('/'):
                        raise ValueError(
                            'Starting point {} does not begin with a slash.'
                            .format(start))

                    if not rsrc_api.exists(start):
                        # Create the full hierarchy with link to the parents.
                        rsrc_api.create_or_replace(start)
                    # Then populate the new resource and crawl for more
                    # relationships.
                    self._crawl(start)
            elif list_file:
                with open(list_file, 'r') as fp:
                    for uri in fp:
                        uid = uri.strip().replace(self.src, '')
                        if not rsrc_api.exists(uid):
                            try:
                                rsrc_api.create_or_replace(uid)
                            except InvalidResourceError:
                                pass
                        self._crawl(uid)
        logger.info('Dumped {} resources.'.format(self._ct))

        return self._ct


    def _crawl(self, uid):
        """
        Get the contents of a resource and its relationships recursively.

        This method recurses into itself each time a reference to a resource
        managed by the repository is encountered.

        :param str uid: The path relative to the source server webroot
            pointing to the resource to crawl, effectively the resource UID.
        """
        ibase = str(nsc['fcres'])
        # Public URI of source repo.
        uri = self.src + uid
        # Internal URI of destination.
        iuri = ibase + uid

        try:
            rsp = requests.head(uri)
        except:
            logger.warn('Error retrieving resource {}'.format(uri))
            return
        if rsp:
            if not self.skip_errors:
                rsp.raise_for_status()
            elif rsp.status_code > 399:
                print('Error retrieving resource {} headers: {} {}'.format(
                    uri, rsp.status_code, rsp.text))

        # Determine LDP type.
        ldp_type = 'ldp_nr'
        try:
            for link in requests.utils.parse_header_links(
                    rsp.headers.get('link')):
                if (
                        link.get('rel') == 'type'
                        and (
                            link.get('url') == str(nsc['ldp'].RDFSource)
                            or link.get('url') == str(nsc['ldp'].Container))
                ):
                    # Resource is an LDP-RS.
                    ldp_type = 'ldp_rs'
                    break
        except TypeError:
            ldp_type = 'ldp_rs'
            #raise ValueError('URI {} is not an LDP resource.'.format(uri))

        # Get the whole RDF document now because we have to know all outbound
        # links.
        get_uri = (
                uri if ldp_type == 'ldp_rs' else '{}/fcr:metadata'.format(uri))
        try:
            get_rsp = requests.get(get_uri)
        except:
            logger.warn('Error retrieving resource {}'.format(get_uri))
            return
        if get_rsp:
            if not self.skip_errors:
                get_rsp.raise_for_status()
            elif get_rsp.status_code > 399:
                print('Error retrieving resource {} body: {} {}'.format(
                    uri, get_rsp.status_code, get_rsp.text))

        data = get_rsp.content.replace(
                self.src.encode('utf-8'), ibase.encode('utf-8'))
        gr = Graph(identifier=iuri).parse(data=data, format='turtle')

        # Store raw graph data. No checks.
        with TxnManager(self.rdfly.store, True):
            self.rdfly.modify_rsrc(uid, add_trp=set(gr))

        # Grab binary and set new resource parameters.
        if ldp_type == 'ldp_nr':
            provided_imr = gr.resource(URIRef(iuri))
            if self.zero_binaries:
                data = b''
            else:
                bin_rsp = requests.get(uri)
                if not self.skip_errors:
                    bin_rsp.raise_for_status()
                elif bin_rsp.status_code > 399:
                    print('Error retrieving resource {} body: {} {}'.format(
                        uri, bin_rsp.status_code, bin_rsp.text))
                data = bin_rsp.content
            #import pdb; pdb.set_trace()
            uuid = str(gr.value(
                URIRef(iuri), nsc['premis'].hasMessageDigest)).split(':')[-1]
            fpath = self.nonrdfly.local_path(
                    self.nonrdfly.config['path'], uuid)
            makedirs(path.dirname(fpath), exist_ok=True)
            with open(fpath, 'wb') as fh:
                fh.write(data)

        self._ct += 1
        if self._ct % 10 == 0:
            print('{} resources processed so far.'.format(self._ct))

        # Now, crawl through outbound links.
        # LDP-NR fcr:metadata must be checked too.
        for pred, obj in gr.predicate_objects():
            #import pdb; pdb.set_trace()
            obj_uid = obj.replace(ibase, '')
            with TxnManager(self.rdfly.store, True):
                conditions = bool(
                    isinstance(obj, URIRef)
                    and obj.startswith(iuri)
                    # Avoid ∞ loop with fragment URIs.
                    and str(urldefrag(obj).url) != str(iuri)
                    # Avoid ∞ loop with circular references.
                    and not self.rdfly.ask_rsrc_exists(obj_uid)
                    and pred not in self.ignored_preds
                )
            if conditions:
                print('Object {} will be crawled.'.format(obj_uid))
                self._crawl(urldefrag(obj_uid).url)
