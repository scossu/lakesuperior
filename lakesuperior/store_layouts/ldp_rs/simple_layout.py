from copy import deepcopy

import arrow

from rdflib import Graph
from rdflib.namespace import RDF, XSD
from rdflib.query import ResultException
from rdflib.resource import Resource
from rdflib.term import Literal, URIRef, Variable

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.dictionaries.namespaces import ns_mgr as nsm
from lakesuperior.dictionaries.srv_mgd_terms import  srv_mgd_subjects, \
        srv_mgd_predicates, srv_mgd_types
from lakesuperior.exceptions import InvalidResourceError, \
        ResourceNotExistsError, TombstoneError
from lakesuperior.store_layouts.ldp_rs.base_rdf_layout import BaseRdfLayout
from lakesuperior.toolbox import Toolbox


class SimpleLayout(BaseRdfLayout):
    '''
    This is the simplest layout.

    It uses a flat triple structure without named graphs aimed at performance.

    Changes are destructive.

    In theory it could be used on top of a triplestore instead of a quad-store
    for (possible) improved speed and reduced storage.
    '''

    def extract_imr(self, uri, strict=False, incl_inbound=False,
                incl_children=True, embed_children=False, incl_srv_mgd=True):
        '''
        See base_rdf_layout.extract_imr.
        '''
        inbound_construct = '\n?s1 ?p1 ?s .' if incl_inbound else ''
        inbound_qry = '\nOPTIONAL { ?s1 ?p1 ?s . } .' if incl_inbound else ''

        # Include and/or embed children.
        embed_children_trp = embed_children_qry = ''
        if incl_srv_mgd and incl_children:
            incl_children_qry = ''

            # Embed children.
            if embed_children:
                embed_children_trp = '?c ?cp ?co .'
                embed_children_qry = '''
                OPTIONAL {{
                  ?s ldp:contains ?c .
                  {}
                }}
                '''.format(embed_children_trp)
        else:
            incl_children_qry = '\nFILTER ( ?p != ldp:contains )' \

        q = '''
        CONSTRUCT {{
            ?s ?p ?o .{inb_cnst}
            {embed_chld_t}
        }} WHERE {{
            ?s ?p ?o .{inb_qry}{incl_chld}{embed_chld}
        }}
        '''.format(inb_cnst=inbound_construct,
                inb_qry=inbound_qry, incl_chld=incl_children_qry,
                embed_chld_t=embed_children_trp, embed_chld=embed_children_qry)

        try:
            qres = self._conn.query(q, initBindings={'s' : uri})
        except ResultException:
            # RDFlib bug: https://github.com/RDFLib/rdflib/issues/775
            g = Graph()
        else:
            g = qres.graph

        #self._logger.debug('Found resource: {}'.format(
        #        g.serialize(format='turtle').decode('utf-8')))
        if strict and not len(g):
            raise ResourceNotExistsError(uri)

        rsrc = Resource(g, uri)

        # Check if resource is a tombstone.
        if rsrc[RDF.type : nsc['fcsystem'].Tombstone]:
            raise TombstoneError(
                    Toolbox().uri_to_uuid(rsrc.identifier),
                    rsrc.value(nsc['fcrepo'].created))
        elif rsrc.value(nsc['fcsystem'].tombstone):
            raise TombstoneError(
                    Toolbox().uri_to_uuid(
                            rsrc.value(nsc['fcsystem'].tombstone).identifier),
                    rsrc.value(nsc['fcrepo'].created))

        return rsrc


    def ask_rsrc_exists(self, urn):
        '''
        See base_rdf_layout.ask_rsrc_exists.
        '''
        self._logger.info('Checking if resource exists: {}'.format(urn))

        return self._conn.query('ASK { ?s ?p ?o . }', initBindings={
            's' : urn})


    def modify_dataset(self, remove_trp=[], add_trp=[]):
        '''
        See base_rdf_layout.update_rsrc.
        '''
        self._logger.debug('Remove graph: {}'.format(set(remove_trp)))
        self._logger.debug('Add graph: {}'.format(set(add_trp)))

        for t in remove_trp:
            self.ds.remove(t)
        for t in add_trp:
            self.ds.add(t)


    def delete_tombstone(self, urn):
        '''
        See BaseRdfLayout.leave_tombstone
        '''
        self.ds.remove((urn, RDF.type, nsc['fcsystem'].Tombstone))
        self.ds.remove((urn, nsc['fcrepo'].created, None))
        self.ds.remove((None, nsc['fcsystem'].tombstone, urn))

