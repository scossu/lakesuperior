#!/usr/bin/env python3

## Small set of tests to begin with.
## For testing, import this file:
##
## `from tests.initial_tests import *`
##
## Then clear the data store with clear() and run
## individual functions inspecting the dataset at each step.

import pdb

import rdflib

from rdflib.graph import Dataset
from rdflib.namespace import RDF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from rdflib.term import URIRef

query_ep = 'http://localhost:3030/lakesuperior-dev/query'
update_ep = 'http://localhost:3030/lakesuperior-dev/update'

store = SPARQLUpdateStore(queryEndpoint=query_ep, update_endpoint=update_ep,
        autocommit=False)
ds = Dataset(store, default_union=True)


def query(q):
    res = ds.query(q)
    print(res.serialize().decode('utf-8'))


def clear():
    '''Clear triplestore.'''
    for g in ds.graphs():
        ds.remove_graph(g)
    store.commit()
    print('All graphs removed from store.')


def insert(report=False):
    '''Add a resource.'''

    res1 = ds.graph(URIRef('urn:res:001'))
    res1.add((URIRef('urn:lake:12873624'), RDF.type, URIRef('http://example.edu#Blah')))

    # Add to default graph. This allows to do something like:
    # CONSTRUCT {?s ?p ?o}
    # WHERE {
    #   ?g1 a <http://example.edu#Resource>  .
    #   GRAPH ?g1 {
    #     ?s ?p ?o .
    #   }
    # }
    ds.add((URIRef('urn:res:001'), RDF.type, URIRef('http://example.edu#Resource')))
    store.commit()

    if report:
        print('Inserted resource:')
        query('''
            SELECT *
            WHERE {
              ?g1 a <http://example.edu#Resource>  .
              GRAPH ?g1 {
                ?s ?p ?o .
              }
            }'''
        )


def update(report=False):
    '''Update resource and create a historic snapshot.'''

    res2 = ds.graph(URIRef('urn:res:002'))
    res2.add((URIRef('urn:lake:12873624'), RDF.type, URIRef('http://example.edu#Boo')))

    ds.remove((URIRef('urn:res:001'), RDF.type, URIRef('http://example.edu#Resource')))
    ds.add((URIRef('urn:res:001'), RDF.type, URIRef('http://example.edu#Snapshot')))
    ds.add((URIRef('urn:res:002'), RDF.type, URIRef('http://example.edu#Resource')))
    ds.add((URIRef('urn:res:002'), URIRef('http://example.edu#hasVersion'), URIRef('urn:res:001')))
    store.commit()

    if report:
        print('Updated resource:')
        query('''
            SELECT *
            WHERE {
              ?g1 a <http://example.edu#Resource>  .
              GRAPH ?g1 {
                ?s ?p ?o .
              }
            }'''
        )
        print('Version snapshot:')
        query('''
            SELECT *
            WHERE {
              ?g1 a <http://example.edu#Snapshot>  .
              GRAPH ?g1 {
                ?s ?p ?o .
              }
            }'''
        )


def delete(report=False):
    '''Delete resource and leave a tombstone.'''

    ds.remove((URIRef('urn:res:002'), RDF.type, URIRef('http://example.edu#Resource')))
    ds.add((URIRef('urn:res:002'), RDF.type, URIRef('http://example.edu#Tombstone')))
    store.commit()

    if report:
        print('Deleted resource (tombstone):')
        query('''
            SELECT *
            WHERE {
              ?g1 a <http://example.edu#Tombstone>  .
              GRAPH ?g1 {
                ?s ?p ?o .
              }
            }'''
        )


def undelete(report=False):
    '''Resurrect resource from a tombstone.'''

    ds.remove((URIRef('urn:res:002'), RDF.type, URIRef('http://example.edu#Tombstone')))
    ds.add((URIRef('urn:res:002'), RDF.type, URIRef('http://example.edu#Resource')))
    store.commit()

    if report:
        print('Undeleted resource:')
        query('''
            SELECT *
            WHERE {
              ?g1 a <http://example.edu#Resource>  .
              GRAPH ?g1 {
                ?s ?p ?o .
              }
            }'''
        )


def abort_tx(report=False):
    '''Abort an operation in the middle of a transaction and roll back.'''

    try:
        res2 = ds.graph(URIRef('urn:res:002'))
        res2.add((URIRef('urn:lake:12873624'), RDF.type, URIRef('http://example.edu#Werp')))
        raise RuntimeError('Something awful happened!')
        store.commit()
    except RuntimeError as e:
        print('Exception caught: {}'.format(e))
        store.rollback()

    if report:
        print('Failed operation (no updates):')
        query('''
            SELECT *
            WHERE {
              ?g1 a <http://example.edu#Resource>  .
              GRAPH ?g1 {
                ?s ?p ?o .
              }
            }'''
        )


def partial_query(report=False):
    '''Execute a query containing a token that throws an error in the middle.

    The purpose of this is to verify whether the store is truly transactional,
    i.e. the whole operation in a transaction is rolled back even if some
    updates have already been processed.'''

    # @TODO
    pass
