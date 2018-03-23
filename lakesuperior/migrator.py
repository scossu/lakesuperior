import logging
import shutil

from io import BytesIO
from contextlib import ContextDecorator
from os import path
from urllib.parse import urldefrag

import lmdb
import requests
import yaml

from rdflib import Graph, URIRef

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.env import env
from lakesuperior.globals import AppGlobals
from lakesuperior.config_parser import parse_config
from lakesuperior.store.ldp_rs.lmdb_store import TxnManager


logger = logging.getLogger(__name__)


class StoreWrapper(ContextDecorator):
    '''
    Open and close a store.
    '''
    def __init__(self, store):
        self.store = store

    def __enter__(self):
        self.store.open(
                env.config['application']['store']['ldp_rs'])

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

    """
    LMDB database parameters.

    See :meth:`lmdb.Environment.__init__`
    """
    db_params = {
        'map_size': 1024 ** 4,
        'metasync': False,
        'readahead': False,
        'meminit': False,
    }

    """List of predicates to ignore when looking for links."""
    ignored_preds = (
        nsc['fcrepo'].hasParent,
        nsc['fcrepo'].hasTransactionProvider,
        nsc['fcrepo'].hasFixityService,
    )


    def __init__(
            self, src, dest, start_pts, zero_binaries=False,
            compact_uris=False):
        """
        Set up base paths and clean up existing directories.

        :param src: (URIRef) Webroot of source repository. This must
        correspond to the LDP root node (for Fedora it can be e.g.
        ``http://localhost:8080fcrepo/rest/``) and is used to determine if URIs
        retrieved are managed by this repository.
        :param dest: (str) Destination repository path. If the location exists
        it must be a writable directory. It will be deleted and recreated. If
        it does not exist, it will be created along with its parents if
        missing.
        :param start_pts: (tuple|list) List of starting points to retrieve
        resources from. It would typically be the repository root in case of a
        full dump or one or more resources in the repository for a partial one.
        :param binary_handling: (string) One of ``include``, ``truncate`` or
        ``split``.
        :param compact_uris: (bool) NOT IMPLEMENTED. Whether the process should
        attempt to compact URIs generated with broken up path segments. If the
        UID matches a pattern such as `/12/34/56/123456...` it is converted to
        `/123456...`. This would remove a lot of cruft caused by the pairtree
        segments. Note that this will change the publicly exposed URIs. If
        durability is a concern, a rewrite directive can be added to the HTTP
        server that proxies the WSGI endpoint.
        """
        # Set up repo folder structure and copy default configuration to
        # destination file.
        cur_dir = path.dirname(path.dirname(path.abspath(__file__)))
        self.dbpath = '{}/data/ldprs_store'.format(dest)
        self.fpath = '{}/data/ldpnr_store'.format(dest)
        self.config_dir = '{}/etc'.format(dest)

        shutil.rmtree(dest, ignore_errors=True)
        shutil.copytree(
                '{}/etc.defaults'.format(cur_dir), self.config_dir)

        # Modify and overwrite destination configuration.
        orig_config = parse_config(self.config_dir)
        orig_config['application']['store']['ldp_rs']['location'] = self.dbpath
        orig_config['application']['store']['ldp_nr']['path'] = self.fpath
        # This sets a "hidden" configuration property that bypasses all server
        # management on resource load: referential integrity, server-managed
        # triples, etc. This will be removed at the end of the migration.
        orig_config['application']['store']['ldp_rs']['disable_checks'] = True

        with open('{}/application.yml'.format(self.config_dir), 'w') \
                as config_file:
            config_file.write(yaml.dump(orig_config['application']))

        env.config = parse_config(self.config_dir)
        env.app_globals = AppGlobals(env.config)

        with TxnManager(env.app_globals.rdf_store, write=True) as txn:
            env.app_globals.rdfly.bootstrap()
            env.app_globals.rdfly.store.close()
        env.app_globals.nonrdfly.bootstrap()

        self.src = src.rstrip('/')
        self.start_pts = start_pts
        self.zero_binaries = zero_binaries

        from lakesuperior.api import resource as rsrc_api
        self.rsrc_api = rsrc_api
        print('Environment: {}'.format(env))
        print('Resource API Environment: {}'.format(self.rsrc_api.env))



    def migrate(self):
        """
        Migrate the database.

        This method creates a fully functional and configured LAKEsuperior
        environment contained in a folder from an LDP repository.
        """
        self._ct = 0
        with StoreWrapper(env.app_globals.rdfly.store):
            for start in self.start_pts:
                if not start.startswith('/'):
                    raise ValueError(
                            'Starting point {} does not begin with a slash.'
                            .format(start))

                self._crawl(start)
        self._remove_temp_options()
        logger.info('Dumped {} resources.'.format(self._ct))

        return self._ct


    def _crawl(self, uid):
        """
        Get the contents of a resource and its relationships recursively.

        This method recurses into itself each time a reference to a resource
        managed by the repository is encountered.

        @param uid (string) The path relative to the source server webroot
        pointing to the resource to crawl, effectively the resource UID.
        """
        ibase = str(nsc['fcres'])
        # Public URI of source repo.
        uri = self.src + uid
        # Internal URI of destination.
        iuri = ibase + uid

        rsp = requests.head(uri)
        rsp.raise_for_status()

        # Determine LDP type.
        ldp_type = 'ldp_nr'
        try:
            for link in requests.utils.parse_header_links(
                    rsp.headers.get('link')):
                if (
                        link.get('rel') == 'type'
                        and (
                            link.get('url') == str(nsc['ldp'].RDFSource)
<<<<<<< HEAD
                            or link.get('url') == str(nsc['ldp'].Container)
                        ):
=======
                            or link.get('url') == str(nsc['ldp'].Container))
                ):
>>>>>>> f3821f6... Add conditions to avoid loops.
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
        get_req = requests.get(get_uri)
        get_req.raise_for_status()

        data = get_req.content.replace(
                self.src.encode('utf-8'), ibase.encode('utf-8'))
        #logger.debug('Localized data: {}'.format(data.decode('utf-8')))
        gr = Graph(identifier=iuri).parse(data=data, format='turtle')

        # Grab binary and set new resource parameters.
        if ldp_type == 'ldp_nr':
            provided_imr = gr.resource(URIRef(iuri))
            if self.zero_binaries:
                data = b'\x00'
                mimetype = str(provided_imr.value(
                        nsc['ebucore'].hasMimeType,
                        default='application/octet-stream'))
            else:
                bin_resp = requests.get(uri)
                bin_resp.raise_for_status()
                data = bin_resp.content
                mimetype = bin_resp.headers.get('content-type')

            self.rsrc_api.create_or_replace(
                    uid, mimetype=mimetype, provided_imr=provided_imr,
                    stream=BytesIO(data))
        else:
            mimetype = 'text/turtle'
            # @TODO This can be improved by creating a resource API method for
            # creating a resource from an RDFLib graph. Here we had to deserialize
            # the RDF data to gather information but have to pass the original
            # serialized stream, which has to be deserialized again in the model.
            self.rsrc_api.create_or_replace(
                    uid, mimetype=mimetype, stream=BytesIO(data))

        self._ct += 1
        if self._ct % 10 ==0:
            print('{} resources processed.'.format(self._ct))

        # Now, crawl through outbound links.
        # LDP-NR fcr:metadata must be checked too.
        for pred, obj in gr.predicate_objects():
            obj_uid = obj.replace(ibase, '')
            if (
                    isinstance(obj, URIRef)
                    and obj.startswith(iuri)
                    and str(urldefrag(obj).url) != str(iuri)
                    and not self.rsrc_api.exists(obj_uid) # Avoid ∞ loop
                    and pred not in self.ignored_preds
            ):
                print('Object {} will be crawled.'.format(obj_uid))
                #import pdb; pdb.set_trace()
                self._crawl(urldefrag(obj_uid).url)


    def _remove_temp_options(self):
        """Remove temporary options in configuration."""
        del(env.config['application']['store']['ldp_rs']['disable_checks'])
        with open('{}/application.yml'.format(self.config_dir), 'w') \
                as config_file:
            config_file.write(yaml.dump(env.config['application']))
