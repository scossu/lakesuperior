#!/usr/bin/env python

import arrow
import requests

# Generate 10,000 children of root node.

requests.put('http://localhost:5000/ldp/pomegranate')

start = arrow.utcnow()

for i in range(1, 10000):
    requests.post('http://localhost:5000/ldp/pomegranate')
    if i % 100 == 0:
        tdelta = arrow.utcnow() - start
        print('Record: \t{} Time elapsed: \t{}'.format(i, tdelta))

tdelta = arrow.utcnow() - start
print('Total elapsed time: {}'.format(tdelta))
