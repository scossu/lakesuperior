import json
import logging
import uuid

from abc import ABCMeta, abstractmethod

from lakesuperior.model.ldpr import Ldpr


class BaseASFormatter(metaclass=ABCMeta):
    '''
    Format message as ActivityStreams.

    This is not really a `logging.Formatter` subclass, but a plain string
    builder.
    '''
    ev_types = {
        Ldpr.RES_CREATED : 'Create',
        Ldpr.RES_DELETED : 'Delete',
        Ldpr.RES_UPDATED : 'Update',
    }

    ev_names = {
        Ldpr.RES_CREATED : 'Resource Modification',
        Ldpr.RES_DELETED : 'Resource Creation',
        Ldpr.RES_UPDATED : 'Resource Deletion',
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
        only meaningful for `ASDeltaFormatter`.
        '''
        self.uri = uri
        self.ev_type = ev_type
        self.time = time
        self.type = type
        self.data = data or None
        self.metadata = metadata


    @abstractmethod
    def __str__(self):
        pass



class ASResourceFormatter(BaseASFormatter):
    '''
    Sends information about a resource being created, updated or deleted, by
    who and when, with no further information about what changed.
    '''

    def __str__(self):
        '''
        Output structured data as string.
        '''
        ret = {
            '@context': 'https://www.w3.org/ns/activitystreams',
            'id' : 'urn:uuid:{}'.format(uuid.uuid4()),
            'type' : self.ev_types[self.ev_type],
            'name' : self.ev_names[self.ev_type],
            'object' : {
                'id' : self.uri,
                'updated' : self.time,
                'type' : self.type,
            },
            'actor' : self.metadata.get('actor', None),
        }

        return json.dumps(ret)



class ASDeltaFormatter(BaseASFormatter):
    '''
    Sends the same information as `ASResourceFormatter` with the addition of
    the triples that were added and the ones that were removed in the request.
    This may be used to send rich provenance data to a preservation system.
    '''
    def __str__(self):
        '''
        Output structured data as string.
        '''
        ret = {
            '@context': 'https://www.w3.org/ns/activitystreams',
            'id' : 'urn:uuid:{}'.format(uuid.uuid4()),
            'type' : self.ev_types[self.ev_type],
            'name' : self.ev_names[self.ev_type],
            'object' : {
                'id' : self.uri,
                'updated' : self.time,
                'type' : self.type,
            },
            'actor' : self.metadata.get('actor', None),
            'data' : self.data,
        }

        return json.dumps(ret)

