import json
import logging
import uuid

from activipy import vocab


class ActivityStreamsFormatter:
    '''
    Format message as ActivityStreams.

    This is not really a `logging.Formatter` subclass, but a plain string
    builder.
    '''
    ev_names = {
        'Update' : 'Resource Modification',
        'Create' : 'Resource Creation',
        'Delete' : 'Resource Deletion',
    }

    def __init__(self, uri, ev_type, time, type, data=None,
                data_fmt='text/turtle', metadata=None):
        '''
        Format output according to granularity level.

        NOTE: Granularity level does not refer to the logging levels, i.e.
        *when* a message gets logged, in fact all the Messaging logger messages
        are logged under the same level. This it is rather about *what* gets
        logged in a message.

        @param record (dict) This holds a dict with the following keys:
        - `uri`: URI of the resource.
        - `ev_type`: one of `create`, `delete` or `update`
        - `time`: Timestamp of the ev_type.
        - `data`: if messaging is configured with `provenance` level, this is
        a `rdflib.Graph` containing the triples that have been removed or
        added.
        - `metadata`: provenance metadata as a rdflib.Graph object. This
        contains properties such as actor(s), action (add/remove), etc. This is
        only present with messaging level set to `provenance`.
        '''
        self.uri = uri
        self.ev_type = ev_type
        self.time = time
        self.type = type
        self.data = data.serialize(format=data_fmt).decode('utf8') \
                if data else None
        self.metadata = metadata


    def __str__(self):
        '''
        Output structured data as string.
        '''
        ret = {
            '@context': 'https://www.w3.org/ns/activitystreams',
            'id' : 'urn:uuid:{}'.format(uuid.uuid4()),
            'type' : self.ev_type,
            'name' : self.ev_names[self.ev_type],
            'object' : {
                'id' : self.uri,
                'updated' : self.time,
                'type' : self.type,
            },
            'actor' : self.metadata.setdefault('actor', None),
            'data' : self.data or '',
        }

        return json.dumps(ret)

