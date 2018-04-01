import logging

from pprint import pformat
from uuid import uuid4

from rdflib import Graph, parser, plugin, serializer
from rdflib.resource import Resource
from rdflib.namespace import RDF

from lakesuperior.model.ldpr import Ldpr
from lakesuperior.model.ldp_nr import LdpNr
from lakesuperior.model.ldp_rs import LdpRs, Ldpc, LdpDc, LdpIc
from lakesuperior.config_parser import config
from lakesuperior.env import env
from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.exceptions import (
        IncompatibleLdpTypeError, InvalidResourceError, ResourceExistsError,
        ResourceNotExistsError, TombstoneError)


LDP_NR_TYPE = nsc['ldp'].NonRDFSource
LDP_RS_TYPE = nsc['ldp'].RDFSource

rdfly = env.app_globals.rdfly
logger = logging.getLogger(__name__)


class LdpFactory:
    """
    Generate LDP instances.
    The instance classes are based on provided client data or on stored data.
    """
    @staticmethod
    def new_container(uid):
        if not uid.startswith('/') or uid == '/':
            raise InvalidResourceError(uid)
        if rdfly.ask_rsrc_exists(uid):
            raise ResourceExistsError(uid)
        rsrc = Ldpc(uid, provided_imr=Resource(Graph(), nsc['fcres'][uid]))

        return rsrc


    @staticmethod
    def from_stored(uid, repr_opts={}, **kwargs):
        """
        Create an instance for retrieval purposes.

        This factory method creates and returns an instance of an LDPR subclass
        based on information that needs to be queried from the underlying
        graph store.

        N.B. The resource must exist.

        :param  uid: UID of the instance.
        """
        #logger.info('Retrieving stored resource: {}'.format(uid))
        imr_urn = nsc['fcres'][uid]

        rsrc_meta = rdfly.get_metadata(uid)
        #logger.debug('Extracted metadata: {}'.format(
        #        pformat(set(rsrc_meta.graph))))
        rdf_types = set(rsrc_meta.graph[imr_urn : RDF.type])

        if LDP_NR_TYPE in rdf_types:
            logger.info('Resource is a LDP-NR.')
            rsrc = LdpNr(uid, repr_opts, **kwargs)
        elif LDP_RS_TYPE in rdf_types:
            logger.info('Resource is a LDP-RS.')
            rsrc = LdpRs(uid, repr_opts, **kwargs)
        else:
            raise ResourceNotExistsError(uid)

        # Sneak in the already extracted metadata to save a query.
        rsrc._metadata = rsrc_meta

        return rsrc


    @staticmethod
    def from_provided(
            uid, mimetype=None, stream=None, provided_imr=None, **kwargs):
        r"""
        Determine LDP type from request content.

        :param str uid: UID of the resource to be created or updated.
        :param str mimetype: The provided content MIME type.
        :param stream: The provided data stream. This can be
            RDF or non-RDF content, or None. In the latter case, an empty
            container is created.
        :type stream: IOStream or None
        :param \*\*kwargs: Arguments passed to the LDP class constructor.
        """
        uri = nsc['fcres'][uid]

        if not stream and not mimetype:
            # Create empty LDPC.
            logger.info('No data received in request. '
                    'Creating empty container.')
            inst = Ldpc(uid, provided_imr=Resource(Graph(), uri), **kwargs)
        elif __class__.is_rdf_parsable(mimetype):
            # Create container and populate it with provided RDF data.
            input_rdf = stream.read()
            gr = Graph().parse(data=input_rdf, format=mimetype, publicID=uri)
            #logger.debug('Provided graph: {}'.format(
            #        pformat(set(provided_gr))))
            provided_imr = Resource(gr, uri)

            # Determine whether it is a basic, direct or indirect container.
            if Ldpr.MBR_RSRC_URI in gr.predicates() and \
                    Ldpr.MBR_REL_URI in gr.predicates():
                if Ldpr.INS_CNT_REL_URI in gr.predicates():
                    cls = LdpIc
                else:
                    cls = LdpDc
            else:
                cls = Ldpc

            inst = cls(uid, provided_imr=provided_imr, **kwargs)

            # Make sure we are not updating an LDP-RS with an LDP-NR.
            if inst.is_stored and LDP_NR_TYPE in inst.ldp_types:
                raise IncompatibleLdpTypeError(uid, mimetype)

            if kwargs.get('handling', 'strict') != 'none':
                inst._check_mgd_terms(inst.provided_imr.graph)

        else:
            # Create a LDP-NR and equip it with the binary file provided.
            # The IMR can also be provided for additional metadata.
            if not provided_imr:
                provided_imr = Resource(Graph(), uri)
            inst = LdpNr(uid, stream=stream, mimetype=mimetype,
                    provided_imr=provided_imr, **kwargs)

            # Make sure we are not updating an LDP-NR with an LDP-RS.
            if inst.is_stored and LDP_RS_TYPE in inst.ldp_types:
                raise IncompatibleLdpTypeError(uid, mimetype)

        logger.info('Creating resource of type: {}'.format(
                inst.__class__.__name__))

        try:
            types = inst.types
        except (TombstoneError, ResourceNotExistsError):
            types = set()

        return inst


    @staticmethod
    def is_rdf_parsable(mimetype):
        """
        Checks whether a MIME type support RDF parsing by a RDFLib plugin.

        :param str mimetype: MIME type to check.
        """
        try:
            plugin.get(mimetype, parser.Parser)
        except plugin.PluginException:
            return False
        else:
            return True


    @staticmethod
    def is_rdf_serializable(mimetype):
        """
        Checks whether a MIME type support RDF serialization by a RDFLib plugin

        :param str mimetype: MIME type to check.
        """
        try:
            plugin.get(mimetype, serializer.Serializer)
        except plugin.PluginException:
            return False
        else:
            return True


    @staticmethod
    def mint_uid(parent_uid, path=None):
        """
        Mint a new resource UID based on client directives.

        This method takes a parent ID and a tentative path and returns an LDP
        resource UID.

        This may raise an exception resulting in a 404 if the parent is not
        found or a 409 if the parent is not a valid container.

        :param str parent_uid: UID of the parent resource. It must be an
            existing LDPC.
        :param str path: path to the resource, relative to the parent.

        :rtype: str
        :return: The confirmed resource UID. This may be different from
            what has been indicated.
        """
        def split_if_legacy(uid):
            if config['application']['store']['ldp_rs']['legacy_ptree_split']:
                uid = tbox.split_uuid(uid)
            return uid

        if path and path.startswith('/'):
            raise ValueError('Slug cannot start with a slash.')
        # Shortcut!
        if not path and parent_uid == '/':
            return '/' + split_if_legacy(str(uuid4()))

        if not parent_uid.startswith('/'):
            raise ValueError('Invalid parent UID: {}'.format(parent_uid))

        parent = LdpFactory.from_stored(parent_uid)
        if nsc['ldp'].Container not in parent.types:
            raise InvalidResourceError(parent_uid,
                    'Parent {} is not a container.')

        pfx = parent_uid.rstrip('/') + '/'
        if path:
            cnd_uid = pfx + path
            if not rdfly.ask_rsrc_exists(cnd_uid):
                return cnd_uid

        return pfx + split_if_legacy(str(uuid4()))


