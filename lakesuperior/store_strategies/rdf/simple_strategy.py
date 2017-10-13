from copy import deepcopy

from rdflib import Graph
from rdflib.namespace import XSD
from rdflib.term import Literal, URIRef, Variable

from lakesuperior.core.namespaces import ns_collection as nsc
from lakesuperior.core.namespaces import ns_mgr as nsm
from lakesuperior.store_strategies.rdf.base_rdf_strategy import \
        BaseRdfStrategy, ResourceExistsError


class SimpleStrategy(BaseRdfStrategy):
    '''
    This is the simplest strategy.

    It uses a flat triple structure without named graphs aimed at performance.
    In theory it could be used on top of a triplestore instead of a quad-store
    for (possible) improved speed and reduced storage.
    '''

    def ask_rsrc_exists(self, urn):
        '''
        See base_rdf_strategy.ask_rsrc_exists.
        '''
        return (urn, Variable('p'), Variable('o')) in self.ds


    def get_rsrc(self, urn, globalize=True):
        '''
        See base_rdf_strategy.get_rsrc.
        '''
        res = self.ds.query(
            'CONSTRUCT WHERE { ?s ?p ?o }',
            initBindings={'s' : urn}
        )

        g = Graph()
        g += res

        return self.globalize_triples(g) if globalize else g


    def create_or_replace_rsrc(self, urn, g, ts, format='text/turtle',
            base_types=None, commit=False):
        '''
        See base_rdf_strategy.create_or_replace_rsrc.
        '''
        if self.ask_rsrc_exists(urn):
            print('Resource exists. Removing.')
            old_rsrc = deepcopy(self.get_rsrc(urn, False)).resource(urn)

            self.delete_rsrc(urn)
            g.add((urn, nsc['fedora'].created,
                    old_rsrc.value(nsc['fedora'].created)))
            g.add((urn, nsc['fedora'].createdBy,
                    old_rsrc.value(nsc['fedora'].createdBy)))

        else:
            print('New resource.')
            g.add((urn, nsc['fedora'].created, ts))
            g.add((urn, nsc['fedora'].createdBy, Literal('BypassAdmin')))

        for s, p, o in g:
            self.ds.add((s, p, o))

        if commit:
            self.conn.store.commit()


    def create_rsrc(self, urn, g, ts, base_types=None, commit=False):
        '''
        See base_rdf_strategy.create_rsrc.
        '''
        if self.ask_rsrc_exists(urn):
            raise ResourceExistsError(
                'Resource #{} already exists. It cannot be re-created with '
                'this method.'.format(urn))

        g.add((self.urn, nsc['fedora'].created, ts))
        g.add((self.urn, nsc['fedora'].createdBy, Literal('BypassAdmin')))

        for s, p, o in g:
            self.ds.add((s, p, o))

        if commit:
            self.conn.store.commit()


    def patch_rsrc(self, urn, data, ts, commit=False):
        '''
        Perform a SPARQL UPDATE on a resource.
        '''
        q = self.localize_string(data).replace('<>', urn.n3())
        self.ds.update(q)

        if commit:
            self.conn.store.commit()


    def delete_rsrc(self, urn, inbound=False, commit=False):
        '''
        Delete a resource. If `inbound` is specified, delete all inbound
        relationships as well.
        '''
        print('Removing resource {}.'.format(urn))

        self.ds.remove((urn, Variable('p'), Variable('o')))
        if inbound:
            self.ds.remove((Variable('s'), Variable('p'), urn))

        if commit:
            self.conn.store.commit()


    ## PROTECTED METHODS ##

