#!/usr/bin/env python
import sys
sys.path.append('.')

from uuid import uuid4

import arrow
import requests

from lakesuperior.util.generators import (
        random_image, random_graph, random_utf8_string)

__doc__ = '''
Benchmark script to measure write performance.
'''

default_n = 10000
webroot = 'http://localhost:8000/ldp'
container_uri = webroot + '/pomegranate'

def run():
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

    sys.stdout.write('RDF Sources (r), Non-RDF (n), or Both 50/50 (b)? [b] >')
    choice = input().lower()
    res_type = choice or 'b'

    if del_cont  == 'y':
        requests.delete(container_uri, headers={'prefer': 'no-tombstone'})
    requests.put(container_uri)

    start = arrow.utcnow()
    ckpt = start

    print('Inserting {} children.'.format(n))

    # URI used to establish an in-repo relationship.
    ref = container_uri
    size = 200 # Size of graph.

    try:
        for i in range(1, n + 1):
            url = '{}/{}'.format(container_uri, uuid4()) if method == 'put' \
                    else container_uri

            if res_type == 'r' or (res_type == 'b' and i % 2 == 0):
                data = random_graph(size, ref).serialize(format='ttl')
                headers = {'content-type': 'text/turtle'}
            else:
                img = random_image(name=uuid4(), ts=16, ims=512)
                data = img['content']
                data.seek(0)
                headers = {
                        'content-type': 'image/png',
                        'content-disposition': 'attachment; filename="{}"'
                            .format(uuid4())}

            #import pdb; pdb.set_trace()
            rsp = requests.request(method, url, data=data, headers=headers)
            rsp.raise_for_status()
            ref = rsp.headers['location']
            if i % 10 == 0:
                now = arrow.utcnow()
                tdelta = now - ckpt
                ckpt = now
                print('Record: {}\tTime elapsed: {}'.format(i, tdelta))
    except KeyboardInterrupt:
        print('Interrupted after {} iterations.'.format(i))

    tdelta = arrow.utcnow() - start
    print('Total elapsed time: {}'.format(tdelta))
    print('Average time per resource: {}'.format(tdelta.total_seconds()/i))

if __name__ == '__main__':
    run()
