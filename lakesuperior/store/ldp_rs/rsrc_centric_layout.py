import logging

from collections import defaultdict
from hashlib import sha256
from itertools import chain
from os import path
from string import Template
from urllib.parse import urldefrag

import arrow

from rdflib import Dataset, Literal, URIRef, plugin
from rdflib.compare import to_isomorphic
from rdflib.namespace import RDF
from rdflib.query import ResultException
from rdflib.resource import Resource
from rdflib.store import Store

from lakesuperior import basedir, env
from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.dictionaries.namespaces import ns_mgr as nsm
from lakesuperior.dictionaries.srv_mgd_terms import  srv_mgd_subjects, \
        srv_mgd_predicates, srv_mgd_types
from lakesuperior.globals import ROOT_RSRC_URI
from lakesuperior.exceptions import (InvalidResourceError,
        ResourceNotExistsError, TombstoneError, PathSegmentError)
from lakesuperior.model.rdf.graph import Graph
from lakesuperior.util.toolbox import get_tree_size


META_GR_URI = nsc['fcsystem']['meta']
HIST_GR_URI = nsc['fcsystem']['histmeta']
PTREE_GR_URI = nsc['fcsystem']['pairtree']
VERS_CONT_LABEL = 'fcr:versions'

Lmdb = plugin.register('Lmdb', Store,
        'lakesuperior.store.ldp_rs.lmdb_store', 'LmdbStore')
logger = logging.getLogger(__name__)


class RsrcCentricLayout:
    """
    This class exposes an interface to build graph store layouts. It also
    provides the basics of the triplestore connection.

    Some store layouts are provided. New ones aimed at specific uses
    and optimizations of the repository may be developed by extending this
    class and implementing all its abstract methods.

    A layout is implemented via application configuration. However, once
    contents are ingested in a repository, changing a layout will most likely
    require a migration.

    The custom layout must be in the lakesuperior.store.rdf
    package and the class implementing the layout must be called
    `StoreLayout`. The module name is the one defined in the app
    configuration.

    E.g. if the configuration indicates `simple_layout` the application will
    look for
    `lakesuperior.store.rdf.simple_layout.SimpleLayout`.
    """
    _graph_uids = ('fcadmin', 'fcmain', 'fcstruct')

    # @TODO Move to a config file?
    attr_map = {
        nsc['fcadmin']: {
            # List of server-managed predicates. Triples bearing one of these
            # predicates will go in the metadata graph.
            'p': {
                nsc['ebucore'].hasMimeType,
                nsc['fcrepo'].created,
                nsc['fcrepo'].createdBy,
                nsc['fcrepo'].hasParent,
                nsc['fcrepo'].hasVersion,
                nsc['fcrepo'].lastModified,
                nsc['fcrepo'].lastModifiedBy,
                nsc['fcsystem'].tombstone,
                # The following 3 are set by the user but still in this group
                # for convenience.
                nsc['ldp'].membershipResource,
                nsc['ldp'].hasMemberRelation,
                nsc['ldp'].insertedContentRelation,

                nsc['iana'].describedBy,
                nsc['premis'].hasMessageDigest,
                nsc['premis'].hasSize,
            },
            # List of metadata RDF types. Triples bearing one of these types in
            # the object will go in the metadata graph.
            't': {
                nsc['fcrepo'].Binary,
                nsc['fcrepo'].Container,
                nsc['fcrepo'].Pairtree,
                nsc['fcrepo'].Resource,
                nsc['fcrepo'].Version,
                nsc['fcsystem'].Tombstone,
                nsc['ldp'].BasicContainer,
                nsc['ldp'].Container,
                nsc['ldp'].DirectContainer,
                nsc['ldp'].IndirectContainer,
                nsc['ldp'].NonRDFSource,
                nsc['ldp'].RDFSource,
                nsc['ldp'].Resource,
            },
        },
        nsc['fcstruct']: {
            # These are placed in a separate graph for optimization purposes.
            'p': {
                nsc['ldp'].contains,
                # NOTE the term below is opinionated. It may be handled
                # differently in the future.
                nsc['pcdm'].hasMember,
            }
        },
    }
    """
    Human-manageable map of attribute routes.

    This serves as the source for :data:`attr_routes`.
    """

    graph_ns_types = {
        nsc['fcadmin']: nsc['fcsystem'].AdminGraph,
        nsc['fcmain']: nsc['fcsystem'].UserProvidedGraph,
        nsc['fcstruct']: nsc['fcsystem'].StructureGraph,
    }
    """
    RDF types of graphs by prefix.
    """

    ignore_vmeta_preds = {
        nsc['foaf'].primaryTopic,
    }
    """
    Predicates of version metadata to be ignored in output.
    """

    ignore_vmeta_types = {
        nsc['fcsystem'].AdminGraph,
        nsc['fcsystem'].UserProvidedGraph,
    }
    """
    RDF types of version metadata to be ignored in output.
    """


    ## MAGIC METHODS ##

    def __init__(self, config):
        """Initialize the graph store and a layout.

        NOTE: `rdflib.Dataset` requires a RDF 1.1 compliant store with support
        for Graph Store HTTP protocol
        (https://www.w3.org/TR/sparql11-http-rdf-update/). Blazegraph supports
        this only in the (currently unreleased) 2.2 branch. It works with Jena,
        which is currently the reference implementation.
        """
        self.config = config
        self.store = plugin.get('Lmdb', Store)(config['location'])
        self.ds = Dataset(self.store, default_union=True)
        self.ds.namespace_manager = nsm


    @property
    def attr_routes(self):
        """
        This is a map that allows specific triples to go to certain graphs.
        It is a machine-friendly version of the static attribute `attr_map`
        which is formatted for human readability and to avoid repetition.
        The attributes not mapped here (usually user-provided triples with no
        special meaning to the application) go to the `fcmain:` graph.

        The output of this is a dict with a similar structure::

            {
                'p': {
                    <Predicate P1>: <destination graph G1>,
                    <Predicate P2>: <destination graph G1>,
                    <Predicate P3>: <destination graph G1>,
                    <Predicate P4>: <destination graph G2>,
                    [...]
                },
                't': {
                    <RDF Type T1>: <destination graph G1>,
                    <RDF Type T2>: <destination graph G3>,
                    [...]
                }
            }
        """
        if not hasattr(self, '_attr_routes'):
            self._attr_routes = {'p': {}, 't': {}}
            for dest in self.attr_map.keys():
                for term_k, terms in self.attr_map[dest].items():
                    self._attr_routes[term_k].update(
                            {term: dest for term in terms})

        return self._attr_routes


    def bootstrap(self):
        """
        Delete all graphs and insert the basic triples.
        """
        from lakesuperior.store.metadata_store import MetadataStore

        logger.info('Deleting all data from the graph store.')
        store = self.ds.store
        if getattr(store, 'is_txn_open', False):
            logger.warning('store txn is open.')
            store.abort()
        store.close()
        store.destroy()

        logger.info('Initializing the graph store with system data.')
        store.open()
        fname = path.join(
                basedir, 'data', 'bootstrap', 'rsrc_centric_layout.sparql')
        with store.txn_ctx(True):
            #import pdb; pdb.set_trace()
            with open(fname, 'r') as f:
                data = Template(f.read())
                self.ds.update(data.substitute(timestamp=arrow.utcnow()))
        with store.txn_ctx():
            imr = self.get_imr('/', incl_inbound=False, incl_children=True)

        #gr = Graph(identifier=imr.uri)
        #gr += imr.data
        #checksum = to_isomorphic(gr).graph_digest()
        #digest = sha256(str(checksum).encode('ascii')).digest()

        ## Clear and initialize metadata store.
        #md_store = env.app_globals.md_store
        #if getattr(md_store, 'is_txn_open', False):
        #    logger.warning('Metadata store txn is open.')
        #    md_store.abort()
        #md_store.close_env()
        #md_store.destroy()
        #md_store.open_env(True)
        #md_store.update_checksum(ROOT_RSRC_URI, digest)
        #md_store.close_env()


    def get_raw(self, subject, ctx=None):
        """
        Get a raw graph of a non-LDP resource.

        The graph is queried across all contexts or within a specific one.

        :param rdflib.term.URIRef subject: URI of the subject.
        :param rdflib.term.URIRef ctx: URI of the optional context. If None,
            all named graphs are queried.

        :rtype: Graph
        """
        return self.store.triple_keys((subject, None, None), ctx)


    def count_rsrc(self):
        """
        Return a count of first-class resources, subdivided in "live" and
        historic snapshots.
        """
        main = set(self.store.triples(
            (None, nsc['foaf'].primaryTopic, None), META_GR_URI))
        hist = set(self.store.triples(
            (None, nsc['foaf'].primaryTopic, None), HIST_GR_URI))

        return {'main': len(main), 'hist': len(hist)}


    def raw_query(self, qry_str):
        """
        Perform a straight query to the graph store.
        """
        return self.ds.query(qry_str)


    def get_imr(
                self, uid, ver_uid=None, strict=True, incl_inbound=False,
                incl_children=True, **kwargs):
        """
        See base_rdf_layout.get_imr.
        """
        #import pdb; pdb.set_trace()
        if ver_uid:
            uid = self.snapshot_uid(uid, ver_uid)

        contexts = {pfx[uid] for pfx in self.graph_ns_types.keys()}

        # Exclude children: remove containment contexts.
        if not incl_children:
            contexts.remove(nsc['fcstruct'][uid])

        imr = Graph(self.store, uri=nsc['fcres'][uid])

        for ctx in contexts:
            gr = self.store.triple_keys((None, None, None), ctx)
            imr |= gr

        # Include inbound relationships.
        if incl_inbound and len(imr):
            gr = Graph(
                self.store, data={*self.get_inbound_rel(nsc['fcres'][uid])}
            )
            imr |= gr

        if strict:
            self._check_rsrc_status(imr)

        return imr


    def ask_rsrc_exists(self, uid):
        """
        See base_rdf_layout.ask_rsrc_exists.
        """
        logger.debug('Checking if resource exists: {}'.format(uid))
        #import pdb; pdb.set_trace()
        res = self.store.triples(
            (nsc['fcres'][uid], RDF.type, nsc['fcrepo'].Resource),
            nsc['fcadmin'][uid])
        try:
            next(res)
        except StopIteration:
            return False
        else:
            return True


    def get_metadata(self, uid, ver_uid=None, strict=True):
        """
        This is an optimized query to get only the administrative metadata.
        """
        logger.debug('Getting metadata for: {}'.format(uid))
        if ver_uid:
            uid = self.snapshot_uid(uid, ver_uid)
        imr = self.store.triple_keys(
            (None, None, None),
            context=nsc['fcadmin'][uid],
            uri=nsc['fcres'][uid]
        )

        if strict:
            self._check_rsrc_status(imr)

        return imr


    def get_user_data(self, uid):
        """
        Get all the user-provided data.

        :param string uid: Resource UID.
        :rtype: rdflib.Graph
        """
        # *TODO* This only works as long as there is only one user-provided
        # graph. If multiple user-provided graphs will be supported, this
        # should use another query to get all of them.
        uri = nsc['fcres'][uid]
        userdata = self.store.triple_keys(
            (None, None, None),
            context=nsc['fcmain'][uid],
            uri=uri
        )

        return userdata


    def get_version_info(self, uid):
        """
        Get all metadata about a resource's versions.

        :param string uid: Resource UID.
        :rtype: Graph
        """
        # **Note:** This pretty much bends the ontology—it replaces the graph
        # URI with the subject URI. But the concepts of data and metadata in
        # Fedora are quite fluid anyways...

        vmeta = Graph(self.store, uri=nsc['fcres'][uid])

        #Get version graphs proper.
        for vtrp in self.store.triple_keys(
            (nsc['fcres'][uid], nsc['fcrepo'].hasVersion, None),
            nsc['fcadmin'][uid]
        ):
            # Add the hasVersion triple to the result graph.
            vmeta.add((vtrp,))
            vmeta_gr = self.store.triple_keys(
                (None, nsc['foaf'].primaryTopic, vtrp[2]), HIST_GR_URI
            )
            # Get triples in the meta graph filtering out undesired triples.
            for vmtrp in vmeta_gr:
                for trp in self.store.triple_keys(
                    (vmtrp[0], None, None), HIST_GR_URI
                ):
                    if (
                            (trp[1] != nsc['rdf'].type
                            or trp[2] not in self.ignore_vmeta_types)
                            and (trp[1] not in self.ignore_vmeta_preds)):
                        vmeta.add(((vtrp[2], trp[1], trp[2]),))

        return vmeta


    def get_inbound_rel(self, subj_uri, full_triple=True):
        """
        Query inbound relationships for a subject.

        This can be a list of either complete triples, or of subjects referring
        to the given URI. It excludes historic version snapshots.

        :param rdflib.URIRef subj_uri: Subject URI.
        :param boolean full_triple: Whether to return the full triples found
            or only the subjects. By default, full triples are returned.

        :rtype: Iterator(tuple(rdflib.term.Identifier) or rdflib.URIRef)
        :return: Inbound triples or subjects.
        """
        # Only return non-historic graphs.
        # TODO self.store.triple_keys?
        meta_gr = self.ds.graph(META_GR_URI)
        ptopic_uri = nsc['foaf'].primaryTopic

        yield from (
            (match[0] if full_triple else match[0][0])
            for match in self.store.triples((None, None, subj_uri))
            if set(meta_gr[ : ptopic_uri : match[0][0]])
        )


    def get_descendants(self, uid, recurse=True):
        """
        Get descendants (recursive children) of a resource.

        :param str uid: Resource UID.

        :rtype: Iterator(rdflib.URIRef)
        :return: Subjects of descendant resources.
        """
        #import pdb; pdb.set_trace()
        #ds = self.ds
        subj_uri = nsc['fcres'][uid]
        ctx_uri = nsc['fcstruct'][uid]
        cont_p = nsc['ldp'].contains
        def _recurse(dset, s, c):
            new_dset = self.store.triple_keys(
                (s, cont_p, None), c
            )[s : cont_p]
            #new_dset = set(ds.graph(c)[s : cont_p])
            for ss in new_dset:
                dset.add(ss)
                cc = URIRef(ss.replace(nsc['fcres'], nsc['fcstruct']))
                sub_dset = self.store.triples((ss, cont_p, None), cc)
                #if set(ds.graph(cc)[ss : cont_p]):
                try:
                    next(sub_dset)
                except StopIteration:
                    pass
                else:
                    _recurse(dset, ss, cc)
            return dset

        if recurse:
            return _recurse(set(), subj_uri, ctx_uri)
        else:
            #return ds.graph(ctx_uri)[subj_uri : cont_p : ])
            return self.store.triple_keys(
                (subj_uri, cont_p, None), ctx_uri
            )[subj_uri : cont_p]


    def get_last_version_uid(self, uid):
        """
        Get the UID of the last version of a resource.

        This can be used for tombstones too.
        """
        ver_info = self.get_version_info(uid)
        last_version_uri = sorted(
            [trp for trp in ver_info if trp[1] == nsc['fcrepo'].created],
            key=lambda trp:trp[2]
        )[-1][0]

        return str(last_version_uri).split(VERS_CONT_LABEL + '/')[-1]


    def patch_rsrc(self, uid, qry):
        """
        Patch a resource with SPARQL-Update statements.

        The statement(s) is/are executed on the user-provided graph only
        to ensure that the scope is limited to the resource.

        :param str uid: UID of the resource to be patched.
        :param dict qry: Parsed and translated query, or query string.
        """
        # Add meta graph for user-defined triples. This may not be used but
        # it's simple and harmless to add here.
        self.store.add(
                (nsc['fcmain'][uid], nsc['foaf'].primaryTopic,
                nsc['fcres'][uid]), META_GR_URI)
        gr = self.ds.graph(nsc['fcmain'][uid])
        #logger.debug('Updating graph {} with statements: {}'.format(
        #    nsc['fcmain'][uid], qry))

        return gr.update(qry)


    def forget_rsrc(self, uid, inbound=True, children=True):
        """
        Completely delete a resource and (optionally) its children and inbound
        references.

        NOTE: inbound references in historic versions are not affected.
        """
        # Localize variables to be used in loops.
        uri = nsc['fcres'][uid]
        uid_fn = self.uri_to_uid

        # remove children and descendants.
        if children:
            #logger.debug('Forgetting offspring of {}'.format(uid))
            for desc_uri in self.get_descendants(uid):
                self.forget_rsrc(uid_fn(desc_uri), inbound, False)
            # Remove structure graph.
            self.store.remove_graph(nsc['fcstruct'][uid])

        # Remove inbound references.
        if inbound:
            for ibs in self.get_inbound_rel(uri):
                self.ds.remove(ibs)

        # Remove versions.
        for ver_uri in self.ds.graph(nsc['fcadmin'][uid])[
                uri : nsc['fcrepo'].hasVersion : None]:
            self.delete_rsrc(uid_fn(ver_uri), True)

        # Remove resource itself.
        self.delete_rsrc(uid)


    def truncate_rsrc(self, uid):
        """
        Remove all user-provided data from a resource and only leave admin and
        structure data.
        """
        userdata = set(self.get_user_data(uid))

        return self.modify_rsrc(uid, remove_trp=userdata)


    def modify_rsrc(self, uid, remove_trp=set(), add_trp=set()):
        """
        Modify triples about a subject.

        This method adds and removes triple sets from specific graphs,
        indicated by the term router. It also adds metadata about the changed
        graphs.
        """
        remove_routes = defaultdict(set)
        add_routes = defaultdict(set)
        historic = VERS_CONT_LABEL in uid

        graph_types = set() # Graphs that need RDF type metadata added.
        # Create add and remove sets for each graph.
        for t in remove_trp:
            #logger.debug('Adding triple to remove list: {}'.format(remove_trp))
            map_graph = self._map_graph_uri(t, uid)
            target_gr_uri = map_graph[0]
            remove_routes[target_gr_uri].add(t)
            graph_types.add(map_graph)
        for t in add_trp:
            #logger.debug('Adding triple to add list: {}'.format(add_trp))
            map_graph = self._map_graph_uri(t, uid)
            target_gr_uri = map_graph[0]
            add_routes[target_gr_uri].add(t)
            graph_types.add(map_graph)

        # Decide if metadata go into historic or current graph.
        meta_gr_uri = HIST_GR_URI if historic else META_GR_URI
        meta_gr = self.ds.graph(meta_gr_uri)

        # Remove and add triple sets from each graph.
        #import pdb; pdb.set_trace()
        for gr_uri, triples in remove_routes.items():
            for trp in triples:
                logger.debug('Removing triple: {}'.format(trp))
                self.store.remove(trp, gr_uri)
        for gr_uri, triples in add_routes.items():
            for trp in triples:
                logger.debug('Adding triple: {}'.format(trp))
                self.store.add(trp, gr_uri)
            # Add metadata.
            meta_gr.set(
                    (gr_uri, nsc['foaf'].primaryTopic, nsc['fcres'][uid]))
            ts = getattr(env, 'timestamp_term', Literal(arrow.utcnow()))
            meta_gr.set((gr_uri, nsc['fcrepo'].created, ts))
            if historic:
                # @FIXME Ugly reverse engineering.
                ver_uid = uid.split(VERS_CONT_LABEL)[1].lstrip('/')
                meta_gr.set((
                    gr_uri, nsc['fcrepo'].hasVersionLabel, Literal(ver_uid)))
            # *TODO* More provenance metadata can be added here.

        # Add graph RDF types.
        for gr_uri, gr_type in graph_types:
            #logger.debug('Adding RDF type: {}'.format(gr_uri))
            meta_gr.add((gr_uri, RDF.type, gr_type))


    def delete_rsrc(self, uid, historic=False):
        """
        Delete all aspect graphs of an individual resource.

        :param uid: Resource UID.
        :param bool historic: Whether the UID is of a historic version.
        """
        meta_gr_uri = HIST_GR_URI if historic else META_GR_URI
        for gr_uri in self.ds.graph(meta_gr_uri)[
                : nsc['foaf'].primaryTopic : nsc['fcres'][uid]]:
            self.ds.remove_context(gr_uri)
            self.ds.graph(meta_gr_uri).remove((gr_uri, None, None))


    def snapshot_uid(self, uid, ver_uid):
        """
        Create a versioned UID string from a main UID and a version UID.
        """
        if VERS_CONT_LABEL in uid:
            raise InvalidResourceError(uid,
                    'Resource \'{}\' is already a version.')

        return '{}/{}/{}'.format(uid, VERS_CONT_LABEL, ver_uid)


    def uri_to_uid(self, uri):
        """
        Convert an internal URI to a UID.
        """
        return str(uri).replace(nsc['fcres'], '')


    def find_refint_violations(self):
        """
        Find all referential integrity violations.

        This method looks for dangling relationships within a repository by
        checking the objects of each triple; if the object is an in-repo
        resource reference, and no resource with that URI results to be in the
        repo, that triple is reported.

        :rtype: set
        :return: Triples referencing a repository URI that is not a resource.
        """
        logger.debug('Find referential integrity violations.')
        for i, obj in enumerate(self.store.all_terms('o'), start=1):
            #logger.debug('term: {}'.format(obj))
            if (
                    isinstance(obj, URIRef)
                    and obj.startswith(nsc['fcres'])
                    and not obj.endswith('fcr:fixity')
                    and not obj.endswith('fcr:versions')
                    and not self.ask_rsrc_exists(self.uri_to_uid(
                        urldefrag(obj).url))):
                logger.warning('Object not found: {}'.format(obj))
                for trp in self.store.triples((None, None, obj)):
                    yield trp
            if i % 100 == 0:
                logger.info('{} terms processed.'.format(i))


    ## PROTECTED MEMBERS ##

    def _check_rsrc_status(self, imr):
        """
        Check if a resource is not existing or if it is a tombstone.
        """
        #import pdb; pdb.set_trace()
        uid = self.uri_to_uid(imr.uri)
        if not len(imr):
            raise ResourceNotExistsError(uid)

        # Check if resource is a tombstone.
        if imr[imr.uri: RDF.type: nsc['fcsystem'].Tombstone]:
            ts = imr.value(nsc['fcrepo'].created)
            raise TombstoneError(uid, ts)
        elif imr.value(nsc['fcsystem'].tombstone):
            raise TombstoneError(
                    self.uri_to_uid(imr.value(nsc['fcsystem'].tombstone)),
                    imr.value(nsc['fcrepo'].created))


    def _map_graph_uri(self, t, uid):
        """
        Map a triple to a namespace prefix corresponding to a graph.

        :rtype: tuple
        :return: 2-tuple with a graph URI and an associated RDF type.
        """
        if t[1] in self.attr_routes['p'].keys():
            pfx = self.attr_routes['p'][t[1]]
        elif t[1] == RDF.type and t[2] in self.attr_routes['t'].keys():
            pfx = self.attr_routes['t'][t[2]]
        else:
            pfx = nsc['fcmain']

        return (pfx[uid], self.graph_ns_types[pfx])
