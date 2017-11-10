#/usr/bin/env python

import uuid
import random
import requests
import numpy
import sys

from PIL import Image

host='http://localhost:5000' # Set this
user='' # Set this
password='' # Set this


img_path = '/tmp'
uid=str(uuid.uuid4())[-12:]

## Update this to include code point ranges to be sampled
#include_ranges = [
#    ( 0x0021, 0x0021 ),
#    ( 0x0023, 0x0026 ),
#    ( 0x0028, 0x007E ),
#    ( 0x00A1, 0x00AC ),
#    ( 0x00AE, 0x00FF ),
#    ( 0x0100, 0x017F ),
#    ( 0x0180, 0x024F ),
#    ( 0x2C60, 0x2C7F ),
#    ( 0x16A0, 0x16F0 ),
#    ( 0x0370, 0x0377 ),
#    ( 0x037A, 0x037E ),
#    ( 0x0384, 0x038A ),
#    ( 0x038C, 0x038C ),
#]
#
#def random_utf8_string(length):
#    alphabet = [
#        chr(code_point) for current_range in include_ranges
#            for code_point in range(current_range[0], current_range[1] + 1)
#    ]
#    return ''.join(random.choice(alphabet) for i in range(length))


def random_image(name, th=8, tv=8, w=256, h=256):
    imarray = numpy.random.rand(th, tv, 3) * 255
    im = Image.fromarray(imarray.astype('uint8')).convert('RGBA')
    im = im.resize((w, h), Image.NEAREST)
    fname = '{}/{}.png'.format(img_path, name)
    im.save(fname)
    return fname


with open(random_image(uid), 'rb') as f:
    rsp = requests.post(
        '{}/ldp'.format(host),
        auth=(user,password) if user or password else None,
        data = f.read(),
    )

    print('Response URL: {}'.format(rsp.url))
    print('Response code: {}'.format(rsp.status_code))
    print('Response message: {}'.format(rsp.text))

