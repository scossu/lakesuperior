from copy import deepcopy

import arrow

from rdflib import Graph
from rdflib.namespace import XSD
from rdflib.term import Literal, URIRef, Variable

from lakesuperior.core.namespaces import ns_collection as nsc
from lakesuperior.core.namespaces import ns_mgr as nsm
from lakesuperior.store_strategies.rdf.base_rdf_strategy import \
        BaseRdfStrategy


class SimpleStrategy(BaseRdfStrategy):
    '''
    This is the simplest strategy.

    It uses a flat triple structure without named graphs aimed at performance.

    Changes are destructive.

    In theory it could be used on top of a triplestore instead of a quad-store
    for (possible) improved speed and reduced storage.
    '''

    @property
    def out_graph(self):
        '''
        See base_rdf_strategy.out_graph.
        '''
        return self.rsrc.graph


    def ask_rsrc_exists(self, rsrc=None):
        '''
        See base_rdf_strategy.ask_rsrc_exists.
        '''
        if not rsrc:
            if self.rsrc is not None:
                rsrc = self.rsrc
            else:
                return False

        self._logger.info('Searching for resource: {}'
                .format(rsrc.identifier))
        return (rsrc.identifier, Variable('p'), Variable('o')) in self.ds


    def create_or_replace_rsrc(self, g):
        '''
        See base_rdf_strategy.create_or_replace_rsrc.
        '''
        # @TODO Use gunicorn to get request timestamp.
        ts = Literal(arrow.utcnow(), datatype=XSD.dateTime)

        if self.ask_rsrc_exists():
            self._logger.info(
                    'Resource {} exists. Removing all outbound triples.'
                    .format(self.rsrc.identifier))

            # Delete all triples but keep creation date and creator.
            created = self.rsrc.value(nsc['fedora'].created)
            created_by = self.rsrc.value(nsc['fedora'].createdBy)

            self.delete_rsrc()
        else:
            created = ts
            created_by = Literal('BypassAdmin')

        self.rsrc.set(nsc['fedora'].created, created)
        self.rsrc.set(nsc['fedora'].createdBy, created_by)

        self.rsrc.set(nsc['fedora'].lastUpdated, ts)
        self.rsrc.set(nsc['fedora'].lastUpdatedBy, Literal('BypassAdmin'))

        for s, p, o in g:
            self.ds.add((s, p, o))


    def create_rsrc(self, g):
        '''
        See base_rdf_strategy.create_rsrc.
        '''
        # @TODO Use gunicorn to get request timestamp.
        ts = Literal(arrow.utcnow(), datatype=XSD.dateTime)

        self.rsrc.set(nsc['fedora'].created, ts)
        self.rsrc.set(nsc['fedora'].createdBy, Literal('BypassAdmin'))

        for s, p, o in g:
            self.ds.add((s, p, o))


    def patch_rsrc(self, data):
        '''
        Perform a SPARQL UPDATE on a resource.
        '''
        # @TODO Use gunicorn to get request timestamp.
        ts = Literal(arrow.utcnow(), datatype=XSD.dateTime)

        q = Translator.localize_string(data).replace(
                '<>', self.rsrc.identifier.n3())

        self.rsrc.set(nsc['fedora'].lastUpdated, ts)
        self.rsrc.set(nsc['fedora'].lastUpdatedBy, Literal('BypassAdmin'))

        self.ds.update(q)


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
