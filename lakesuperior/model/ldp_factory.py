import logging

from pprint import pformat
from uuid import uuid4

from rdflib import Graph, parser, plugin, serializer
from rdflib.resource import Resource
from rdflib.namespace import RDF

from lakesuperior import model
from lakesuperior.config_parser import config
from lakesuperior.env import env
from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.exceptions import (
        IncompatibleLdpTypeError, InvalidResourceError, ResourceExistsError,
        ResourceNotExistsError)


rdfly = env.app_globals.rdfly


class LdpFactory:
    '''
    Generate LDP instances.
    The instance classes are based on provided client data or on stored data.
    '''
    LDP_NR_TYPE = nsc['ldp'].NonRDFSource
    LDP_RS_TYPE = nsc['ldp'].RDFSource

    _logger = logging.getLogger(__name__)


    @staticmethod
    def new_container(uid):
        if not uid:
            raise InvalidResourceError(uid)
        if rdfly.ask_rsrc_exists(uid):
            raise ResourceExistsError(uid)
        rsrc = model.ldp_rs.Ldpc(
                uid, provided_imr=Resource(Graph(), nsc['fcres'][uid]))

        return rsrc


    @staticmethod
    def from_stored(uid, repr_opts={}, **kwargs):
        '''
        Create an instance for retrieval purposes.

        This factory method creates and returns an instance of an LDPR subclass
        based on information that needs to be queried from the underlying
        graph store.

        N.B. The resource must exist.

        @param uid UID of the instance.
        '''
        #__class__._logger.info('Retrieving stored resource: {}'.format(uid))
        imr_urn = nsc['fcres'][uid]

        rsrc_meta = rdfly.get_metadata(uid)
        #__class__._logger.debug('Extracted metadata: {}'.format(
        #        pformat(set(rsrc_meta.graph))))
        rdf_types = set(rsrc_meta.graph[imr_urn : RDF.type])

        if __class__.LDP_NR_TYPE in rdf_types:
            __class__._logger.info('Resource is a LDP-NR.')
            rsrc = model.ldp_nr.LdpNr(uid, repr_opts, **kwargs)
        elif __class__.LDP_RS_TYPE in rdf_types:
            __class__._logger.info('Resource is a LDP-RS.')
            rsrc = model.ldp_rs.LdpRs(uid, repr_opts, **kwargs)
        else:
            raise ResourceNotExistsError(uid)

        # Sneak in the already extracted metadata to save a query.
        rsrc._metadata = rsrc_meta

        return rsrc


    @staticmethod
    def from_provided(uid, mimetype, stream=None, **kwargs):
        '''
        Determine LDP type from request content.

        @param uid (string) UID of the resource to be created or updated.
        @param mimetype (string) The provided content MIME type.
        @param stream (IOStream | None) The provided data stream. This can be
        RDF or non-RDF content, or None. In the latter case, an empty container
        is created.
        '''
        uri = nsc['fcres'][uid]

        logger = __class__._logger

        if not stream:
            # Create empty LDPC.
            logger.info('No data received in request. '
                    'Creating empty container.')
            inst = model.ldp_rs.Ldpc(
                    uid, provided_imr=Resource(Graph(), uri), **kwargs)

        elif __class__.is_rdf_parsable(mimetype):
            # Create container and populate it with provided RDF data.
            input_rdf = stream.read()
            gr = Graph().parse(data=input_rdf, format=mimetype, publicID=uri)
            #logger.debug('Provided graph: {}'.format(
            #        pformat(set(provided_gr))))
            provided_imr = Resource(gr, uri)

            # Determine whether it is a basic, direct or indirect container.
            Ldpr = model.ldpr.Ldpr
            if Ldpr.MBR_RSRC_URI in gr.predicates() and \
                    Ldpr.MBR_REL_URI in gr.predicates():
                if Ldpr.INS_CNT_REL_URI in gr.predicates():
                    cls = model.ldp_rs.LdpIc
                else:
                    cls = model.ldp_rs.LdpDc
            else:
                cls = model.ldp_rs.Ldpc

            inst = cls(uid, provided_imr=provided_imr, **kwargs)

            # Make sure we are not updating an LDP-RS with an LDP-NR.
            if inst.is_stored and __class__.LDP_NR_TYPE in inst.ldp_types:
                raise IncompatibleLdpTypeError(uid, mimetype)

            if kwargs.get('handling', 'strict') != 'none':
                inst._check_mgd_terms(inst.provided_imr.graph)

        else:
            # Create a LDP-NR and equip it with the binary file provided.
            provided_imr = Resource(Graph(), uri)
            inst = model.ldp_nr.LdpNr(uid, stream=stream, mimetype=mimetype,
                    provided_imr=provided_imr, **kwargs)

            # Make sure we are not updating an LDP-NR with an LDP-RS.
            if inst.is_stored and __class__.LDP_RS_TYPE in inst.ldp_types:
                raise IncompatibleLdpTypeError(uid, mimetype)

        logger.info('Creating resource of type: {}'.format(
                inst.__class__.__name__))

        try:
            types = inst.types
        except:
            types = set()

        return inst


    @staticmethod
    def is_rdf_parsable(mimetype):
        '''
        Checks whether a MIME type support RDF parsing by a RDFLib plugin.

        @param mimetype (string) MIME type to check.
        '''
        try:
            plugin.get(mimetype, parser.Parser)
        except plugin.PluginException:
            return False
        else:
            return True


    @staticmethod
    def is_rdf_serializable(mimetype):
        '''
        Checks whether a MIME type support RDF serialization by a RDFLib plugin

        @param mimetype (string) MIME type to check.
        '''
        try:
            plugin.get(mimetype, serializer.Serializer)
        except plugin.PluginException:
            return False
        else:
            return True


    @staticmethod
    def mint_uid(parent_uid, path=None):
        '''
        Mint a new resource UID based on client directives.

        This method takes a parent ID and a tentative path and returns an LDP
        resource UID.

        This may raise an exception resulting in a 404 if the parent is not
        found or a 409 if the parent is not a valid container.

        @param parent_uid (string) UID of the parent resource. It must be an
        existing LDPC.
        @param path (string) path to the resource, relative to the parent.

        @return string The confirmed resource UID. This may be different from
        what has been indicated.
        '''
        def split_if_legacy(uid):
            if config['application']['store']['ldp_rs']['legacy_ptree_split']:
                uid = tbox.split_uuid(uid)
            return uid

        # Shortcut!
        if not path and parent_uid == '':
            uid = split_if_legacy(str(uuid4()))
            return uid

        parent = LdpFactory.from_stored(parent_uid,
                repr_opts={'incl_children' : False})

        # Set prefix.
        if parent_uid:
            if nsc['ldp'].Container not in parent.types:
                raise InvalidResourceError(parent_uid,
                        'Parent {} is not a container.')
            pfx = parent_uid + '/'
        else:
            pfx = ''

        # Create candidate UID and validate.
        if path:
            cnd_uid = pfx + path
            if rdfly.ask_rsrc_exists(cnd_uid):
                uid = pfx + split_if_legacy(str(uuid4()))
            else:
                uid = cnd_uid
        else:
            uid = pfx + split_if_legacy(str(uuid4()))

        return uid


