#!/usr/bin/env python
import sys

from uuid import uuid4

import arrow

from rdflib import Dataset
from rdflib.term import URIRef

default_n = 100000
sys.stdout.write('How many resources? [{}] >'.format(default_n))
choice = input().lower()
n = int(choice) if choice else default_n

ds = Dataset('Sleepycat')
ds.open('/tmp/lsup_bdb.db')
gr = ds.graph('http://ex.org/graph#g1')

start = arrow.utcnow()
ckpt = start

for i in range(1, n):
    if i % 100 == 0:
        print('inserted {} resources.'.format(i))
    subj = URIRef('http://ex.org/rdf/{}'.format(uuid4()))
    gr.add((subj, URIRef('http://ex.org/p1'), URIRef('http://ex.org/o1')))
    gr.add((URIRef('http://ex.org/s1'), URIRef('http://ex.org/p2'), subj))

    now = arrow.utcnow()
    tdelta = now - ckpt
    ckpt = now
    print('Record: {}\tTime elapsed: {}'.format(i, tdelta))

tdelta = arrow.utcnow() - start
print('Total elapsed time: {}'.format(tdelta))
print('Average time per resource: {}'.format(tdelta.total_seconds()/n))
print('Graph size: {}'.format(len(gr)))

ds.close()
