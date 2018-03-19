import io
import random

from hashlib import sha1
from math import floor

import requests
import numpy

from PIL import Image
from rdflib import Graph, URIRef, Literal
from rdflib.namespace import Namespace, NamespaceManager


# @TODO Update this to include code point ranges to be sampled
include_ranges = [
    ( 0x0021, 0x0021 ),
    ( 0x0023, 0x0026 ),
    ( 0x0028, 0x007E ),
    ( 0x00A1, 0x00AC ),
    ( 0x00AE, 0x00FF ),
    ( 0x0100, 0x017F ),
    ( 0x0180, 0x024F ),
    ( 0x2C60, 0x2C7F ),
    ( 0x16A0, 0x16F0 ),
    ( 0x0370, 0x0377 ),
    ( 0x037A, 0x037E ),
    ( 0x0384, 0x038A ),
    ( 0x038C, 0x038C ),
]

def random_utf8_string(length):
    alphabet = [
        chr(code_point) for current_range in include_ranges
            for code_point in range(current_range[0], current_range[1] + 1)
    ]
    return ''.join(random.choice(alphabet) for i in range(length))


def random_image(name, ts=8, ims=256):
    imarray = numpy.random.rand(ts, ts, 3) * 255
    im = Image.fromarray(imarray.astype('uint8')).convert('RGBA')
    im = im.resize((ims, ims), Image.NEAREST)

    imf = io.BytesIO()
    im.save(imf, format='png')
    imf.seek(0)
    hash = sha1(imf.read()).hexdigest()

    return {
        'content' : imf,
        'hash' : hash,
        'filename' : random_utf8_string(32) + '.png'
    }


nsm = NamespaceManager(Graph())
nsc = {
    'extp': Namespace('http://ex.org/exturi_p#'),
    'intp': Namespace('http://ex.org/inturi_p#'),
    'litp': Namespace('http://ex.org/lit_p#'),
}
for pfx, ns in nsc.items():
    nsm.bind(pfx, ns)

def random_graph(size, ref):
    '''
    Generate a synthetic graph.

    @param size (int) size Size of the graph. It will be rounded by a
    multiplier of 4.
    '''
    gr = Graph()
    gr.namespace_manager = nsm
    for ii in range(floor(size / 4)):
        gr.add((
            URIRef(''),
            nsc['intp'][str(ii % size)],
            URIRef(ref)
        ))
        gr.add((
            URIRef(''),
            nsc['litp'][str(ii % size)],
            Literal(random_utf8_string(64))
        ))
        gr.add((
            URIRef(''),
            nsc['litp'][str(ii % size)],
            Literal(random_utf8_string(64))
        ))
        gr.add((
            URIRef(''),
            nsc['extp'][str(ii % size)],
            URIRef('http://example.edu/res/{}'.format(ii // 10))
        ))

    #print('Graph: {}'.format(gr.serialize(format='turtle').decode('utf-8')))
    return gr
