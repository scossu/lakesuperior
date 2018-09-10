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
#webroot = 'http://localhost:8080/rest'
webroot = 'http://localhost:5000/ldp'
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

    sys.stdout.write('RDF Sources (r), Non-RDF (n), or Both 50/50 (b)? [r] >')
    choice = input().lower()
    res_type = choice or 'r'

    if del_cont  == 'y':
        requests.delete(container_uri, headers={'prefer': 'no-tombstone'})
    requests.put(container_uri)

    print('Inserting {} children.'.format(n))

    # URI used to establish an in-repo relationship.
    ref = container_uri
    size = 200 # Size of graph.

    wclock_start = arrow.utcnow()
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
            # Start timing after generating the data.
            ckpt = arrow.utcnow()
            if i == 1:
                tcounter = ckpt - ckpt
                prev_tcounter = tcounter

            rsp = requests.request(method, url, data=data, headers=headers)
            tdelta = arrow.utcnow() - ckpt
            tcounter += tdelta

            rsp.raise_for_status()
            ref = rsp.headers['location']
            if i % 10 == 0:
                print(
                    f'Record: {i}\tTime elapsed: {tcounter}\t'
                    f'Per resource: {(tcounter - prev_tcounter) / 10}')
                prev_tcounter = tcounter
    except KeyboardInterrupt:
        print('Interrupted after {} iterations.'.format(i))

    wclock = arrow.utcnow() - wclock_start
    print(f'Total elapsed time: {wclock}')
    print(f'Total time spent ingesting resources: {tcounter}')
    print(f'Average time per resource: {tcounter.total_seconds()/i}')

if __name__ == '__main__':
    run()
