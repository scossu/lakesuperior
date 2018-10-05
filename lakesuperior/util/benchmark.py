#!/usr/bin/env python3

import sys

from uuid import uuid4

import arrow
import click
import requests

from matplotlib import pyplot as plt

from lakesuperior.util.generators import (
        random_image, random_graph, random_utf8_string)

__doc__ = '''
Benchmark script to measure write performance.
'''

def_endpoint = 'http://localhost:8000/ldp'
def_ct = 10000
def_parent = '/pomegranate'
def_gr_size = 200


@click.command()
@click.option(
    '--endpoint', '-e', default=def_endpoint,
    help=f'LDP endpoint. Default: {def_endpoint}')
@click.option(
    '--count', '-c', default=def_ct,
    help='Number of resources to ingest. Default: {def_ct}')
@click.option(
    '--parent', '-p', default=def_parent,
    help='Path to the container resource under which the new resources will be '
        'created. It must begin with a slash (`/`) character. '
        f'Default: {def_parent}')
@click.option(
    '--delete-container', '-d', is_flag=True,
    help='Delete container resource and its children if already existing. By '
    'default, the container is not deleted and new resources are added to it.')
@click.option(
    '--method', '-m', default='put',
    help='HTTP method to use. Case insensitive. Either PUT '
    f'or POST. Default: PUT')
@click.option(
    '--graph-size', '-s', default=def_gr_size,
    help=f'Number of triples in each graph. Default: {def_gr_size}')
@click.option(
    '--resource-type', '-t', default='r',
    help='Type of resources to ingest. One of `r` (only LDP-RS, i.e. RDF), '
    '`n` (only  LDP-NR, i.e. binaries), or `b` (50/50% of both). '
    'Default: r')
@click.option(
    '--graph', '-g', is_flag=True, help='Plot a graph of ingest timings. '
    'The graph figure is displayed on screen with basic manipulation and save '
    'options.')

def run(
        endpoint, count, parent, method, delete_container,
        graph_size, resource_type, graph):

    container_uri = endpoint + parent

    method = method.lower()
    if method not in ('post', 'put'):
        raise ValueError(f'HTTP method not supported: {method}')

    if delete_container:
        requests.delete(container_uri, headers={'prefer': 'no-tombstone'})
    requests.put(container_uri)

    print(f'Inserting {count} children under {container_uri}.')

    # URI used to establish an in-repo relationship. This is set to
    # the most recently created resource in each loop.
    ref = container_uri

    wclock_start = arrow.utcnow()
    if graph:
        print('Results will be plotted.')
        # Plot coordinates: X is request count, Y is request timing.
        px = []
        py = []
        plt.xlabel('Requests')
        plt.ylabel('ms per request')
        plt.title('FCREPO Benchmark')

    try:
        for i in range(1, count + 1):
            url = '{}/{}'.format(container_uri, uuid4()) if method == 'put' \
                    else container_uri

            if resource_type == 'r' or (resource_type == 'b' and i % 2 == 0):
                data = random_graph(graph_size, ref).serialize(format='ttl')
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
                avg10 = (tcounter - prev_tcounter) / 10
                print(
                    f'Record: {i}\tTime elapsed: {tcounter}\t'
                    f'Per resource: {avg10}')
                prev_tcounter = tcounter

                if graph:
                    px.append(i)
                    # Divide by 1000 for µs → ms
                    py.append(avg10.microseconds // 1000)

    except KeyboardInterrupt:
        print('Interrupted after {} iterations.'.format(i))

    wclock = arrow.utcnow() - wclock_start
    print(f'Total elapsed time: {wclock}')
    print(f'Total time spent ingesting resources: {tcounter}')
    print(f'Average time per resource: {tcounter.total_seconds()/i}')

    if graph:
        if resource_type == 'r':
            type_label = 'LDP-RS'
        elif resource_type == 'n':
            type_label = 'LDP-NR'
        else:
            type_label = 'LDP-RS + LDP-NR'
        label = (
            f'{container_uri}; {method.upper()}; {graph_size} trp/graph; '
            f'{type_label}')
        plt.plot(px, py, label=label)
        plt.legend()
        plt.show()


if __name__ == '__main__':
    run()
