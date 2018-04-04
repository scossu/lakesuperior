#/usr/bin/env python

import uuid
from generators import random_image, random_utf8_string

host='http://localhost:5000' # Set this
user='' # Set this
password='' # Set this


img_path = '/tmp'
uid=str(uuid.uuid4())[-12:]

with open(random_image(uid), 'rb') as f:
    rsp = requests.post(
        '{}/ldp'.format(host),
        auth=(user,password) if user or password else None,
        data = f.read(),
    )

    print('Response URL: {}'.format(rsp.url))
    print('Response code: {}'.format(rsp.status_code))
    print('Response message: {}'.format(rsp.text))

