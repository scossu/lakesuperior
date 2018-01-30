import logging

from pprint import pformat

import rdflib

from flask import current_app, g
from rdflib import Graph
from rdflib.resource import Resource
from rdflib.namespace import RDF

from lakesuperior import model
from lakesuperior.model.generic_resource import PathSegment
from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.exceptions import (IncompatibleLdpTypeError,
        InvalidResourceError, ResourceNotExistsError)

class LdpFactory:
    '''
    Generate LDP instances.
    The instance classes are based on provided client data or on stored data.
    '''
    LDP_NR_TYPE = nsc['ldp'].NonRDFSource
    LDP_RS_TYPE = nsc['ldp'].RDFSource

    _logger = logging.getLogger(__name__)

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

        rsrc_meta = current_app.rdfly.get_metadata(uid)
        __class__._logger.debug('Extracted metadata: {}'.format(
                pformat(set(rsrc_meta.graph))))
        rdf_types = set(rsrc_meta.graph[imr_urn : RDF.type])

        if __class__.LDP_NR_TYPE in rdf_types:
            __class__._logger.info('Resource is a LDP-NR.')
            rsrc = model.ldp_nr.LdpNr(uid, repr_opts, **kwargs)
        elif __class__.LDP_RS_TYPE in rdf_types:
            __class__._logger.info('Resource is a LDP-RS.')
            rsrc = model.ldp_rs.LdpRs(uid, repr_opts, **kwargs)
        elif nsc['fcsystem']['PathSegment'] in rdf_types:
            return PathSegment(uid)
        else:
            raise ResourceNotExistsError(uid)

        # Sneak in the already extracted metadata to save a query.
        rsrc._metadata = rsrc_meta

        return rsrc


    @staticmethod
    def from_provided(uid, content_length, mimetype, stream, **kwargs):
        '''
        Determine LDP type from request content.

        @param uid (string) UID of the resource to be created or updated.
        @param content_length (int) The provided content length.
        @param mimetype (string) The provided content MIME type.
        @param stream (IOStream) The provided data stream. This can be RDF or
        non-RDF content.
        '''
        urn = nsc['fcres'][uid]

        logger = __class__._logger

        if not content_length:
            # Create empty LDPC.
            logger.info('No data received in request. '
                    'Creating empty container.')
            inst = model.ldp_rs.Ldpc(
                    uid, provided_imr=Resource(Graph(), urn), **kwargs)

        elif __class__.is_rdf_parsable(mimetype):
            # Create container and populate it with provided RDF data.
            input_rdf = stream.read()
            provided_gr = Graph().parse(data=input_rdf,
                    format=mimetype, publicID=urn)
            #logger.debug('Provided graph: {}'.format(
            #        pformat(set(provided_gr))))
            local_gr = g.tbox.localize_graph(provided_gr)
            #logger.debug('Parsed local graph: {}'.format(
            #        pformat(set(local_gr))))
            provided_imr = Resource(local_gr, urn)

            # Determine whether it is a basic, direct or indirect container.
            Ldpr = model.ldpr.Ldpr
            if Ldpr.MBR_RSRC_URI in local_gr.predicates() and \
                    Ldpr.MBR_REL_URI in local_gr.predicates():
                if Ldpr.INS_CNT_REL_URI in local_gr.predicates():
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
            provided_imr = Resource(Graph(), urn)
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
        if nsc['fcrepo'].Pairtree in types:
            raise InvalidResourceError(inst.uid, 'Resource {} is a Pairtree.')

        return inst


    @staticmethod
    def is_rdf_parsable(mimetype):
        '''
        Checks whether a MIME type support RDF parsing by a RDFLib plugin.

        @param mimetype (string) MIME type to check.
        '''
        try:
            rdflib.plugin.get(mimetype, rdflib.parser.Parser)
        except rdflib.plugin.PluginException:
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
            rdflib.plugin.get(mimetype, rdflib.serializer.Serializer)
        except rdflib.plugin.PluginException:
            return False
        else:
            return True

