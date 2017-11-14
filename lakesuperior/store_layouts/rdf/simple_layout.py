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
from lakesuperior.store_layouts.rdf.base_rdf_layout import BaseRdfLayout
from lakesuperior.util.translator import Translator


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
        inbound_construct = '\n?s1 ?p1 {} .'.format(uri.n3()) \
                if incl_inbound else ''
        inbound_qry = '\nOPTIONAL {{ ?s1 ?p1 {} . }} .'.format(uri.n3()) \
                if incl_inbound else ''
        embed_children_qry = '''
        OPTIONAL {{
          {0} ldp:contains ?c .
          ?c ?cp ?co .
        }}
        '''.format(uri.n3()) if incl_children and embed_children else ''

        incl_children_qry = '\nFILTER ( ?p != ldp:contains )' \
                if not incl_children else ''

        srv_mgd_qry = ''
        if not incl_srv_mgd:
            for p in srv_mgd_predicates:
                self._logger.debug('Removing predicate: {}'.format(p))
                srv_mgd_qry += '\nFILTER ( ?p != {} ) .'.format(p.n3())
            for t in srv_mgd_types:
                self._logger.debug('Removing type: {}'.format(t))
                srv_mgd_qry += '\nMINUS {{ ?s a {} .}} .'.format(t.n3())

        q = '''
        CONSTRUCT {{
            {uri} ?p ?o .{inb_cnst}
            ?c ?cp ?co .
        }} WHERE {{
            {uri} ?p ?o .{inb_qry}{incl_chld}{embed_chld}{omit_srv_mgd}
            #FILTER (?p != premis:hasMessageDigest) .
        }}
        '''.format(uri=uri.n3(), inb_cnst=inbound_construct,
                inb_qry=inbound_qry, incl_chld=incl_children_qry,
                embed_chld=embed_children_qry, omit_srv_mgd=srv_mgd_qry)

        try:
            qres = self.query(q)
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
                    Translator.uri_to_uuid(rsrc.identifier),
                    rsrc.value(nsc['fcrepo'].created))
        elif rsrc.value(nsc['fcsystem'].tombstone):
            tombstone_rsrc = rsrc.value(nsc['fcsystem'].tombstone)
            raise TombstoneError(
                    Translator.uri_to_uuid(rsrc.identifier),
                    tombstone_rsrc.value(nsc['fcrepo'].created))

        return rsrc


    def ask_rsrc_exists(self, urn):
        '''
        See base_rdf_layout.ask_rsrc_exists.
        '''
        self._logger.info('Checking if resource exists: {}'.format(urn))
        imr = self.extract_imr(urn, incl_children=False)
        return len(imr.graph) > 0


    def create_rsrc(self, imr):
        '''
        See base_rdf_layout.create_rsrc.
        '''
        self._logger.debug('Creating resource:\n{}'.format(
            imr.graph.serialize(format='turtle').decode('utf8')))
        #self.ds |= imr.graph # This does not seem to work with datasets.
        for t in imr.graph:
            self.ds.add(t)

        return self.RES_CREATED


    def replace_rsrc(self, imr):
        '''
        See base_rdf_layout.replace_rsrc.
        '''
        rsrc = self.rsrc(imr.identifier)

        # Delete the stored triples but spare the protected predicates.
        del_trp_qry = []
        for p in rsrc.predicates():
            if p.identifier not in self.protected_pred:
                self._logger.debug('Removing {}'.format(p.identifier))
                rsrc.remove(p.identifier)
            else:
                self._logger.debug('NOT Removing {}'.format(p))
                imr.remove(p.identifier)

        #self.ds |= imr.graph # This does not seem to work with datasets.
        for t in imr.graph:
            self.ds.add(t)

        return self.RES_UPDATED


    def modify_dataset(self, remove_trp, add_trp):
        '''
        See base_rdf_layout.update_rsrc.
        '''
        #self._logger.debug('Remove triples: {}'.format(
        #        remove_trp.serialize(format='turtle').decode('utf-8')))
        #self._logger.debug('Add triples: {}'.format(
        #        add_trp.serialize(format='turtle').decode('utf-8')))

        for t in remove_trp:
            self.ds.remove(t)
        for t in add_trp:
            self.ds.add(t)


    ## PROTECTED METHODS ##

    def _do_delete_rsrc(self, rsrc, inbound):
        '''
        See BaseRdfLayout._do_delete_rsrc
        '''
        urn = rsrc.identifier
        print('Removing resource {}.'.format(urn))

        rsrc.remove(Variable('p'))

        if inbound:
            self.ds.remove((Variable('s'), Variable('p'), rsrc.identifier))

        return urn


    def _leave_tombstone(self, urn, parent_urn=None):
        '''
        See BaseRdfLayout._leave_tombstone
        '''
        if parent_urn:
            self.ds.add((urn, nsc['fcsystem'].tombstone, parent_urn))
        else:
            # @TODO Use gunicorn to get request timestamp.
            ts = Literal(arrow.utcnow(), datatype=XSD.dateTime)
            self.ds.add((urn, RDF.type, nsc['fcsystem'].Tombstone))
            self.ds.add((urn, nsc['fcrepo'].created, ts))


