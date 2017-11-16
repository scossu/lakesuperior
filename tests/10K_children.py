#!/usr/bin/env python

import arrow
import requests

# Generate 10,000 children of root node.

requests.put('http://localhost:8000/ldp/pomegranate')

start = arrow.utcnow()
ckpt = start

for i in range(1, 10000):
    requests.post('http://localhost:8000/ldp/pomegranate')
    if i % 100 == 0:
        now = arrow.utcnow()
        tdelta = now - ckpt
        ckpt = now
        print('Record: {}\tTime elapsed: {}'.format(i, tdelta))

tdelta = arrow.utcnow() - start
print('Total elapsed time: {}'.format(tdelta))
