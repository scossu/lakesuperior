#!/usr/bin/env python
import sys

from uuid import uuid4

import arrow
import requests

default_n = 10000
webroot = 'http://localhost:8000/ldp'
#webroot = 'http://lake.devbox.local/fcrepo/rest'
container = webroot + '/pomegranate'
datafile = 'tests/data/marcel_duchamp_single_subject.ttl'

sys.stdout.write('How many children? [{}] >'.format(default_n))
choice = input().lower()
n = int(choice) if choice else default_n

sys.stdout.write('Delete container? [n] >')
choice = input().lower()
del_cont = choice or 'n'

# Generate 10,000 children of root node.

if del_cont  == 'y':
    requests.delete(container, headers={'prefer': 'no-tombstone'})
requests.put(container)

start = arrow.utcnow()
ckpt = start

print('Inserting {} children.'.format(n))

data = open(datafile, 'rb').read()
for i in range(1, n):
    requests.put('{}/{}'.format(container, uuid4()), data=data, headers={
        'content-type': 'text/turtle'})
    if i % 100 == 0:
        now = arrow.utcnow()
        tdelta = now - ckpt
        ckpt = now
        print('Record: {}\tTime elapsed: {}'.format(i, tdelta))

tdelta = arrow.utcnow() - start
print('Total elapsed time: {}'.format(tdelta))
print('Average time per resource: {}'.format(tdelta.total_seconds()/n))
