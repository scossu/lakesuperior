#!/usr/bin/env python
import sys

import arrow
import requests

default_n = 10000

sys.stdout.write('How many children? [{}] >'.format(default_n))
choice = input().lower()

n = int(choice) or default_n

# Generate 10,000 children of root node.

requests.put('http://localhost:8000/ldp/pomegranate')

start = arrow.utcnow()
ckpt = start

print('Inserting {} children.'.format(n))

for i in range(1, n):
    requests.post('http://localhost:8000/ldp/pomegranate')
    if i % 100 == 0:
        now = arrow.utcnow()
        tdelta = now - ckpt
        ckpt = now
        print('Record: {}\tTime elapsed: {}'.format(i, tdelta))

tdelta = arrow.utcnow() - start
print('Total elapsed time: {}'.format(tdelta))
print('Average time per resource: {}'.format(tdelta.total_seconds()/n))
