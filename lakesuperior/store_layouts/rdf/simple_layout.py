from copy import deepcopy

import arrow

from rdflib import Graph
from rdflib.namespace import XSD
from rdflib.query import ResultException
from rdflib.resource import Resource
from rdflib.term import Literal, URIRef, Variable

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.dictionaries.namespaces import ns_mgr as nsm
from lakesuperior.store_layouts.rdf.base_rdf_layout import BaseRdfLayout, \
        needs_rsrc
from lakesuperior.util.digest import Digest
from lakesuperior.util.translator import Translator


class SimpleLayout(BaseRdfLayout):
    '''
    This is the simplest layout.

    It uses a flat triple structure without named graphs aimed at performance.

    Changes are destructive.

    In theory it could be used on top of a triplestore instead of a quad-store
    for (possible) improved speed and reduced storage.
    '''

    @property
    def headers(self):
        '''
        See base_rdf_layout.headers.
        '''
        headers = {
            'Link' : [],
        }

        # @NOTE: Easy with these one-by-one picks. Each one of them is a call
        # to the triplestore.
        digest = self.rsrc.value(nsc['premis'].hasMessageDigest)
        if digest:
            etag = digest.identifier.split(':')[-1]
            headers['ETag'] = 'W/"{}"'.format(etag),

        last_updated_term = self.rsrc.value(nsc['fcrepo'].lastUpdated)
        if last_updated_term:
            headers['Last-Modified'] = arrow.get(last_updated_term)\
                .format('ddd, D MMM YYYY HH:mm:ss Z')

        return headers


    def extract_imr(self, uri=None, graph=None, inbound=False):
        '''
        See base_rdf_layout.extract_imr.
        '''
        uri = uri or self.base_urn

        inbound_qry = '\n?s1 ?p1 {}'.format(self.base_urn.n3()) \
                if inbound else ''

        q = '''
        CONSTRUCT {{
            {0} ?p ?o .{1}
        }} WHERE {{
            {0} ?p ?o .{1}
            #FILTER (?p != premis:hasMessageDigest) .
        }}
        '''.format(uri.n3(), inbound_qry)

        try:
            qres = self.query(q)
        except ResultException:
            # RDFlib bug? https://github.com/RDFLib/rdflib/issues/775
            g = Graph()
        else:
            g = qres.graph

        return Resource(g, uri)


    @needs_rsrc
    def out_rsrc(self, srv_mgd=True, inbound=False, embed_children=False):
        '''
        See base_rdf_layout.out_rsrc.
        '''
        im_rsrc = self.extract_imr(inbound=inbound)

        im_rsrc.remove(nsc['premis'].hasMessageDigest)

        return im_rsrc


    def ask_rsrc_exists(self, uri=None):
        '''
        See base_rdf_layout.ask_rsrc_exists.
        '''
        if not uri:
            if self.rsrc is not None:
                uri = self.rsrc.identifier
            else:
                return False

        self._logger.info('Searching for resource: {}'.format(uri))
        return (uri, Variable('p'), Variable('o')) in self.ds


    @needs_rsrc
    def create_or_replace_rsrc(self, g):
        '''
        See base_rdf_layout.create_or_replace_rsrc.
        '''
        # @TODO Use gunicorn to get request timestamp.
        ts = Literal(arrow.utcnow(), datatype=XSD.dateTime)

        if self.ask_rsrc_exists():
            self._logger.info(
                    'Resource {} exists. Removing all outbound triples.'
                    .format(self.rsrc.identifier))

            # Delete all triples but keep creation date and creator.
            created = self.rsrc.value(nsc['fcrepo'].created)
            created_by = self.rsrc.value(nsc['fcrepo'].createdBy)

            self.delete_rsrc()
        else:
            created = ts
            created_by = Literal('BypassAdmin')

        self.rsrc.set(nsc['fcrepo'].created, created)
        self.rsrc.set(nsc['fcrepo'].createdBy, created_by)

        self.rsrc.set(nsc['fcrepo'].lastUpdated, ts)
        self.rsrc.set(nsc['fcrepo'].lastUpdatedBy, Literal('BypassAdmin'))

        for s, p, o in g:
            self.ds.add((s, p, o))


    @needs_rsrc
    def create_rsrc(self, g):
        '''
        See base_rdf_layout.create_rsrc.
        '''
        # @TODO Use gunicorn to get request timestamp.
        ts = Literal(arrow.utcnow(), datatype=XSD.dateTime)

        self.rsrc.set(nsc['fcrepo'].created, ts)
        self.rsrc.set(nsc['fcrepo'].createdBy, Literal('BypassAdmin'))

        cksum = Digest.rdf_cksum(self.rsrc.graph)
        self.rsrc.set(nsc['premis'].hasMessageDigest,
                URIRef('urn:sha1:{}'.format(cksum)))

        for s, p, o in g:
            self.ds.add((s, p, o))


    @needs_rsrc
    def patch_rsrc(self, data):
        '''
        Perform a SPARQL UPDATE on a resource.

        @TODO deprecate.
        '''
        # @TODO Use gunicorn to get request timestamp.
        ts = Literal(arrow.utcnow(), datatype=XSD.dateTime)

        q = Translator.localize_string(data).replace(
                '<>', self.rsrc.identifier.n3())

        self.rsrc.set(nsc['fcrepo'].lastUpdated, ts)
        self.rsrc.set(nsc['fcrepo'].lastUpdatedBy, Literal('BypassAdmin'))

        self.ds.update(q)


    @needs_rsrc
    def modify_rsrc(self, remove, add):
        '''
        See base_rdf_layout.update_rsrc.
        '''
        for t in remove.predicate_objects():
            self.rsrc.remove(t[0], t[1])

        for t in add.predicate_objects():
            self.rsrc.add(t[0], t[1])


    def delete_rsrc(self, inbound=False):
        '''
        Delete a resource. If `inbound` is specified, delete all inbound
        relationships as well.
        '''
        print('Removing resource {}.'.format(self.rsrc.identifier))

        self.rsrc.remove(Variable('p'))
        if inbound:
            self.ds.remove((Variable('s'), Variable('p'), self.rsrc.identifier))


    ## PROTECTED METHODS ##

    def _unique_value(self, p):
        '''
        Use this to retrieve a single value knowing that there SHOULD be only
        one (e.g. `skos:prefLabel`), If more than one is found, raise an
        exception.

        @param rdflib.Resource rsrc The resource to extract value from.
        @param rdflib.term.URIRef p The predicate to serach for.

        @throw ValueError if more than one value is found.
        '''
        values = self.rsrc[p]
        value = next(values)
        try:
            next(values)
        except StopIteration:
            return value

        # If the second next() did not raise a StopIteration, something is
        # wrong.
        raise ValueError('Predicate {} should be single valued. Found: {}.'\
                .format(set(values)))
