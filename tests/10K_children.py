#!/usr/bin/env python
import sys
sys.path.append('.')

from uuid import uuid4

import arrow
import requests

from rdflib import Graph, URIRef, Literal

from util.generators import random_utf8_string


default_n = 10000
webroot = 'http://localhost:8000/ldp'
#webroot = 'http://localhost:8080/fcrepo/rest'
container_uri = webroot + '/pomegranate'

sys.stdout.write('How many children? [{}] >'.format(default_n))
choice = input().lower()
n = int(choice) if choice else default_n

sys.stdout.write('Delete container? [n] >')
choice = input().lower()
del_cont = choice or 'n'

sys.stdout.write('POST or PUT? [PUT] >')
choice = input().lower()
if choice and choice.lower() not in ('post', 'put'):
    raise ValueError('Not a valid verb.')
method = choice.lower() or 'put'

# Generate 10,000 children of root node.

if del_cont  == 'y':
    requests.delete(container_uri, headers={'prefer': 'no-tombstone'})
requests.put(container_uri)


start = arrow.utcnow()
ckpt = start

print('Inserting {} children.'.format(n))

# URI used to establish an in-repo relationship.
prev_uri = container_uri
size = 50 # Size of graph to be multiplied by 4.

try:
    for i in range(1, n):
        url = '{}/{}'.format(container_uri, uuid4()) if method == 'put' \
                else container_uri

        # Generate synthetic graph.
        #print('generating graph: {}'.format(i))
        g = Graph()
        for ii in range(size):
            g.add((
                URIRef(''),
                URIRef('urn:inturi_p:{}'.format(ii % size)),
                URIRef(prev_uri)
            ))
            g.add((
                URIRef(''),
                URIRef('urn:lit_p:{}'.format(ii % size)),
                Literal(random_utf8_string(64))
            ))
            g.add((
                URIRef(''),
                URIRef('urn:lit_p:{}'.format(ii % size)),
                Literal(random_utf8_string(64))
            ))
            g.add((
                URIRef(''),
                URIRef('urn:exturi_p:{}'.format(ii % size)),
                URIRef('http://exmple.edu/res/{}'.format(ii // 10))
            ))

        # Send request.
        rsp = requests.request(
                method, url, data=g.serialize(format='ttl'),
                headers={ 'content-type': 'text/turtle'})
        rsp.raise_for_status()
        prev_uri = rsp.headers['location']
        if i % 10 == 0:
            now = arrow.utcnow()
            tdelta = now - ckpt
            ckpt = now
            print('Record: {}\tTime elapsed: {}'.format(i, tdelta))
except KeyboardInterrupt:
    print('Interruped after {} iterations.'.format(i))

tdelta = arrow.utcnow() - start
print('Total elapsed time: {}'.format(tdelta))
print('Average time per resource: {}'.format(tdelta.total_seconds()/i))
