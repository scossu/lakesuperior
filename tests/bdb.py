#!/usr/bin/env python
import sys

from random import randrange
from uuid import uuid4

import arrow

from rdflib import Dataset
from rdflib import plugin
from rdflib.store import Store
from rdflib.term import URIRef

default_n = 100000
sys.stdout.write('How many resources? [{}] >'.format(default_n))
choice = input().lower()
n = int(choice) if choice else default_n
store_uid = randrange(8192)
store_name = '/tmp/lsup_{}.db'.format(store_uid)

store = plugin.get('Sleepycat', Store)()
ds = Dataset(store)
store.open(store_name)

start = arrow.utcnow()
ckpt = start

for i in range(1, n):
    try:
        subj = URIRef('http://ex.org/rdf/{}'.format(uuid4()))
        gr = ds.graph('http://ex.org/graph#g{}'.format(i))
        for ii in range(1, 100):
            gr.add((subj, URIRef('http://ex.org/p1'),
                URIRef('http://ex.org/random#'.format(randrange(2048)))))
        gr.add((URIRef('http://ex.org/s1'), URIRef('http://ex.org/p2'), subj))

        now = arrow.utcnow()
        tdelta = now - ckpt
        ckpt = now
        if i % 100 == 0:
            print('Record: {}\tTime elapsed: {}'.format(i, tdelta))
    except KeyboardInterrupt:
        print('Interrupted after {} iterations.'.format(i))
        break

tdelta = arrow.utcnow() - start
print('Store name: {}'.format(store_name))
print('Total elapsed time: {}'.format(tdelta))
print('Average time per resource: {}'.format(tdelta.total_seconds()/i))
print('Graph size: {}'.format(len(gr)))

store.close()
