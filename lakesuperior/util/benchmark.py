#!/usr/bin/env python3

import logging
import sys

from os import path
from uuid import uuid4

import arrow
import click
import rdflib
import requests

from matplotlib import pyplot as plt

from lakesuperior.util.generators import (
        random_image, random_graph, random_utf8_string)
from lakesuperior.exceptions import ResourceNotExistsError

__doc__ = '''
Benchmark script to measure write performance.
'''

def_mode = 'ldp'
def_endpoint = 'http://localhost:8000/ldp'
def_ct = 10000
def_parent = '/pomegranate'
def_gr_size = 200
def_img_size = 1024

logging.disable(logging.WARN)


@click.command()

@click.option(
    '--mode', '-m', default=def_mode,
    help=(
        'Mode of ingestion. One of `ldp`, `python`. With the former, the '
        'HTTP/LDP web server is used. With the latter, the Python API is '
        'used, in which case the server need not be running. '
        f'Default: {def_endpoint}'
    )
)

@click.option(
    '--endpoint', '-e', default=def_endpoint,
    help=(
        'LDP endpoint. Only meaningful with `ldp` mode. '
        f'Default: {def_endpoint}'
    )
)

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
    '--method', '-X', default='put',
    help=(
        'HTTP method to use. Case insensitive. Either PUT or POST. '
        'Default: PUT'
    )
)

@click.option(
    '--graph-size', '-s', default=def_gr_size,
    help=(
        'Number of triples in each random graph, rounded down to a multiple '
        f'of 8. Default: {def_gr_size}'
    )
)

@click.option(
    '--image-size', '-S', default=def_img_size,
    help=(
        'Size of random square image, in pixels for each dimension, rounded '
        f'down to a multiple of 8. Default: {def_img_size}'
    )
)

@click.option(
    '--resource-type', '-t', default='r',
    help='Type of resources to ingest. One of `r` (only LDP-RS, i.e. RDF), '
    '`n` (only  LDP-NR, i.e. binaries), or `b` (50/50% of both). '
    'Default: r')

@click.option(
    '--plot', '-P', is_flag=True, help='Plot a graph of ingest timings. '
    'The graph figure is displayed on screen with basic manipulation and save '
    'options.')

def run(
    mode, endpoint, count, parent, method, delete_container,
    graph_size, image_size, resource_type, plot
):
    """
    Run the benchmark.
    """

    method = method.lower()
    if method not in ('post', 'put'):
        raise ValueError(f'Insertion method not supported: {method}')

    mode = mode.lower()
    if mode == 'ldp':
        parent = '{}/{}'.format(endpoint.strip('/'), parent.strip('/'))

        if delete_container:
            print('Removing previously existing container.')
            requests.delete(parent)
            requests.delete(f'{parent}/fcr:tombstone')
        requests.put(parent)

    elif mode == 'python':
        from lakesuperior import env_setup
        from lakesuperior.api import resource as rsrc_api

        if delete_container:
            try:
                print('Removing previously existing container.')
                rsrc_api.delete(parent, soft=False)
            except ResourceNotExistsError:
                pass
        rsrc_api.create_or_replace(parent)
    else:
        raise ValueError(f'Mode not supported: {mode}')

    if resource_type != 'r':
        # Set image parameters.
        ims = max(image_size - image_size % 8, 128)
        tn = ims // 32

    # URI used to establish an in-repo relationship. This is set to
    # the most recently created resource in each loop.
    ref = parent

    print(f'Inserting {count} children under {parent}.')

    wclock_start = arrow.utcnow()
    if plot:
        print('Results will be plotted.')
        # Plot coordinates: X is request count, Y is request timing.
        px = []
        py = []
        plt.xlabel('Requests')
        plt.ylabel('ms per request')
        plt.title('Lakesuperior / FCREPO Benchmark')

    try:
        for i in range(1, count + 1):
            if mode == 'ldp':
                dest = (
                    f'{parent}/{uuid4()}' if method == 'put'
                    else parent
                )
            else:
                dest = (
                    path.join(parent, str(uuid4()))
                    if method == 'put' else parent
                )

            if resource_type == 'r' or (resource_type == 'b' and i % 2 == 0):
                data = random_graph(graph_size, ref)
                headers = {'content-type': 'text/turtle'}
            else:
                img = random_image(tn=tn, ims=ims)
                data = img['content']
                data.seek(0)
                headers = {
                        'content-type': 'image/png',
                        'content-disposition': 'attachment; filename="{}"'
                            .format(uuid4())}

            # Start timing after generating the data.
            ckpt = arrow.utcnow()
            if i == 1:
                tcounter = ckpt - ckpt
                prev_tcounter = tcounter

            #import pdb; pdb.set_trace()
            ref = (
                _ingest_ldp(
                    method, dest, data, headers, ref
                )
                if mode == 'ldp'
                else _ingest_py(method, dest, data, ref)
            )
            tcounter += (arrow.utcnow() - ckpt)

            if i % 10 == 0:
                avg10 = (tcounter - prev_tcounter) / 10
                print(
                    f'Record: {i}\tTime elapsed: {tcounter}\t'
                    f'Per resource: {avg10}')
                prev_tcounter = tcounter

                if plot:
                    px.append(i)
                    # Divide by 1000 for µs → ms
                    py.append(avg10.microseconds // 1000)

    except KeyboardInterrupt:
        print('Interrupted after {} iterations.'.format(i))

    wclock = arrow.utcnow() - wclock_start
    print(f'Total elapsed time: {wclock}')
    print(f'Total time spent ingesting resources: {tcounter}')
    print(f'Average time per resource: {tcounter.total_seconds()/i}')

    if plot:
        if resource_type == 'r':
            type_label = 'LDP-RS'
        elif resource_type == 'n':
            type_label = 'LDP-NR'
        else:
            type_label = 'LDP-RS + LDP-NR'
        label = (
            f'{parent}; {method.upper()}; {graph_size} trp/graph; '
            f'{type_label}')
        plt.plot(px, py, label=label)
        plt.legend()
        plt.show()


def _ingest_ldp(method, uri, data, headers, ref):
    """
    Ingest the graph via HTTP/LDP.
    """
    if isinstance(data, rdflib.Graph):
        data = data.serialize(format='ttl')
    rsp = requests.request(method, uri, data=data, headers=headers)
    rsp.raise_for_status()
    return rsp.headers['location']


def _ingest_py(method, dest, data, ref):
    from lakesuperior.api import resource as rsrc_api

    kwargs = {}
    if isinstance(data, rdflib.Graph):
        kwargs['graph'] = data
    else:
        kwargs['stream'] = data
        kwargs['mimetype'] = 'image/png'

    if method == 'put':
        _, rsrc = rsrc_api.create_or_replace(dest, **kwargs)
    else:
        rsrc = rsrc_api.create(dest, **kwargs)

    return rsrc.uid


if __name__ == '__main__':
    run()
