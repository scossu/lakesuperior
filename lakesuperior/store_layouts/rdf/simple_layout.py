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
from lakesuperior.store_layouts.rdf.base_rdf_layout import BaseRdfLayout, \
        needs_rsrc
from lakesuperior.util.translator import Translator


class SimpleLayout(BaseRdfLayout):
    '''
    This is the simplest layout.

    It uses a flat triple structure without named graphs aimed at performance.

    Changes are destructive.

    In theory it could be used on top of a triplestore instead of a quad-store
    for (possible) improved speed and reduced storage.
    '''

    def extract_imr(self, uri=None, graph=None, minimal=False,
            incl_inbound=False, embed_children=False, incl_srv_mgd=True):
        '''
        See base_rdf_layout.extract_imr.
        '''
        uri = uri or self.base_urn

        inbound_qry = '\n?s1 ?p1 {}'.format(self.base_urn.n3()) \
                if incl_inbound else ''
        embed_children_qry = '''
        OPTIONAL {{
          {0} ldp:contains ?c .
          ?c ?cp ?co .
        }}
        '''.format(uri.n3()) if embed_children else ''

        q = '''
        CONSTRUCT {{
            {0} ?p ?o .{1}
            ?c ?cp ?co .
        }} WHERE {{
            {0} ?p ?o .{1}{2}
            #FILTER (?p != premis:hasMessageDigest) .
        }}
        '''.format(uri.n3(), inbound_qry, embed_children_qry)

        try:
            qres = self.query(q)
        except ResultException:
            # RDFlib bug: https://github.com/RDFLib/rdflib/issues/775
            g = Graph()
        else:
            g = qres.graph
            if not incl_srv_mgd:
                self._logger.info('Removing server managed triples.')
                for p in srv_mgd_predicates:
                    self._logger.debug('Removing predicate: {}'.format(p))
                    rsrc.remove(p)
                for t in srv_mgd_types:
                    self._logger.debug('Removing type: {}'.format(t))
                    rsrc.remove(RDF.type, t)

        return Resource(g, uri)


    def ask_rsrc_exists(self, uri=None):
        '''
        See base_rdf_layout.ask_rsrc_exists.
        '''
        if not uri:
            if self.rsrc is not None:
                uri = self.rsrc.identifier
            else:
                return False

        self._logger.info('Checking if resource exists: {}'.format(uri))
        return (uri, Variable('p'), Variable('o')) in self.ds


    @needs_rsrc
    def create_rsrc(self, imr):
        '''
        See base_rdf_layout.create_rsrc.
        '''
        for s, p, o in imr.graph:
            self.ds.add((s, p, o))

        return self.RES_CREATED


    @needs_rsrc
    def replace_rsrc(self, imr):
        '''
        See base_rdf_layout.replace_rsrc.
        '''
        # Delete all triples but keep creation date and creator.
        created = self.rsrc.value(nsc['fcrepo'].created)
        created_by = self.rsrc.value(nsc['fcrepo'].createdBy)

        imr.set(nsc['fcrepo'].created, created)
        imr.set(nsc['fcrepo'].createdBy, created_by)

        # Delete the stored triples.
        self.delete_rsrc()

        self.ds |= imr.graph
        #for s, p, o in imr.graph:
        #    self.ds.add((s, p, o))

        return self.RES_UPDATED


    @needs_rsrc
    def modify_rsrc(self, remove_trp, add_trp):
        '''
        See base_rdf_layout.update_rsrc.
        '''
        self.ds -= remove_trp
        self.ds += add_trp
        #for t in remove.predicate_objects():
        #    self.rsrc.remove(t[0], t[1])

        #for t in add.predicate_objects():
        #    self.rsrc.add(t[0], t[1])


    def delete_rsrc(self, inbound=True):
        '''
        Delete a resource. If `inbound` is specified, delete all inbound
        relationships as well (this is the default).
        '''
        print('Removing resource {}.'.format(self.rsrc.identifier))

        self.rsrc.remove(Variable('p'))
        if inbound:
            self.ds.remove(
                    (Variable('s'), Variable('p'), self.rsrc.identifier))


