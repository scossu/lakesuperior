import json
import logging
import uuid

from abc import ABCMeta, abstractmethod

from lakesuperior.globals import RES_CREATED, RES_DELETED, RES_UPDATED


class BaseASFormatter(metaclass=ABCMeta):
    """
    Format message as ActivityStreams.

    This is not really a `logging.Formatter` subclass, but a plain string
    builder.
    """
    ev_types = {
        RES_CREATED : 'Create',
        RES_DELETED : 'Delete',
        RES_UPDATED : 'Update',
    }

    ev_names = {
        RES_CREATED : 'Resource Creation',
        RES_DELETED : 'Resource Deletion',
        RES_UPDATED : 'Resource Modification',
    }

    def __init__(
            self, rsrc_uri, ev_type, timestamp, rsrc_type, actor, data=None):
        """
        Format output according to granularity level.

        NOTE: Granularity level does not refer to the logging levels, i.e.
        *when* a message gets logged, in fact all the Messaging logger messages
        are logged under the same level. This it is rather about *what* gets
        logged in a message.

        :param rdflib.URIRef rsrc_uri: URI of the resource.
        :param str ev_type: one of `create`, `delete` or `update`
        :param str timestamp: Timestamp of the event.
        :param  data: (tuple(set)) if messaging is configured with `provenance`
        level, this is a 2-tuple with one set (as 3-tuples of
        RDFlib.Identifier instances) for removed triples, and one set for
        added triples.
        """
        self.rsrc_uri = rsrc_uri
        self.ev_type = ev_type
        self.timestamp = timestamp
        self.rsrc_type = rsrc_type
        self.actor = actor
        self.data = data


    @abstractmethod
    def __str__(self):
        pass



class ASResourceFormatter(BaseASFormatter):
    """
    Sends information about a resource being created, updated or deleted, by
    who and when, with no further information about what changed.
    """

    def __str__(self):
        """Output structured data as string."""
        ret = {
            '@context': 'https://www.w3.org/ns/activitystreams',
            'id' : 'urn:uuid:{}'.format(uuid.uuid4()),
            'type' : self.ev_types[self.ev_type],
            'name' : self.ev_names[self.ev_type],
            'object' : {
                'id' : self.rsrc_uri,
                'updated' : self.timestamp,
                'type' : self.rsrc_type,
            },
            'actor' : self.actor,
        }

        return json.dumps(ret)



class ASDeltaFormatter(BaseASFormatter):
    """
    Sends the same information as `ASResourceFormatter` with the addition of
    the triples that were added and the ones that were removed in the request.
    This may be used to send rich provenance data to a preservation system.
    """
    def __str__(self):
        """Output structured data as string."""
        ret = {
            '@context': 'https://www.w3.org/ns/activitystreams',
            'id' : 'urn:uuid:{}'.format(uuid.uuid4()),
            'type' : self.ev_types[self.ev_type],
            'name' : self.ev_names[self.ev_type],
            'object' : {
                'id' : self.rsrc_uri,
                'updated' : self.timestamp,
                'type' : self.rsrc_type,
            },
            'actor' : self.actor,
            'data' : self.data,
        }

        return json.dumps(ret)

