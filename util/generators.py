import io
import random

from hashlib import sha1

import requests
import numpy

from PIL import Image


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


