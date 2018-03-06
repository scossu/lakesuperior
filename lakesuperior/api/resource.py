import logging

from functools import wraps
from itertools import groupby
from multiprocessing import Process
from threading import Lock, Thread

import arrow

from rdflib import Literal
from rdflib.namespace import XSD

from lakesuperior.config_parser import config
from lakesuperior.exceptions import InvalidResourceError
from lakesuperior.env import env
from lakesuperior.globals import RES_DELETED
from lakesuperior.model.ldp_factory import LDP_NR_TYPE, LdpFactory
from lakesuperior.store.ldp_rs.lmdb_store import TxnManager


logger = logging.getLogger(__name__)
app_globals = env.app_globals

__doc__ = '''
Primary API for resource manipulation.

Quickstart:

>>> # First import default configuration and globals—only done once.
>>> import lakesuperior.default_env
>>> from lakesuperior.api import resource
>>> # Get root resource.
>>> rsrc = resource.get('/')
>>> # Dump graph.
>>> set(rsrc.imr())
{(rdflib.term.URIRef('info:fcres/'),
  rdflib.term.URIRef('http://purl.org/dc/terms/title'),
  rdflib.term.Literal('Repository Root')),
 (rdflib.term.URIRef('info:fcres/'),
  rdflib.term.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
  rdflib.term.URIRef('http://fedora.info/definitions/v4/repository#Container')),
 (rdflib.term.URIRef('info:fcres/'),
  rdflib.term.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
  rdflib.term.URIRef('http://fedora.info/definitions/v4/repository#RepositoryRoot')),
 (rdflib.term.URIRef('info:fcres/'),
  rdflib.term.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
  rdflib.term.URIRef('http://fedora.info/definitions/v4/repository#Resource')),
 (rdflib.term.URIRef('info:fcres/'),
  rdflib.term.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
  rdflib.term.URIRef('http://www.w3.org/ns/ldp#BasicContainer')),
 (rdflib.term.URIRef('info:fcres/'),
  rdflib.term.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
  rdflib.term.URIRef('http://www.w3.org/ns/ldp#Container')),
 (rdflib.term.URIRef('info:fcres/'),
  rdflib.term.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
  rdflib.term.URIRef('http://www.w3.org/ns/ldp#RDFSource'))}

'''

def transaction(write=False):
    '''
    Handle atomic operations in a store.

    This wrapper ensures that a write operation is performed atomically. It
    also takes care of sending a message for each resource changed in the
    transaction.

    ALL write operations on the LDP-RS and LDP-NR stores go through this
    wrapper.
    '''
    def _transaction_deco(fn):
        @wraps(fn)
        def _wrapper(*args, **kwargs):
            # Mark transaction begin timestamp. This is used for create and
            # update timestamps on resources.
            env.timestamp = arrow.utcnow()
            env.timestamp_term = Literal(env.timestamp, datatype=XSD.dateTime)
            with TxnManager(app_globals.rdf_store, write=write) as txn:
                ret = fn(*args, **kwargs)
            if len(app_globals.changelog):
                job = Thread(target=process_queue)
                job.start()
            delattr(env, 'timestamp')
            delattr(env, 'timestamp_term')
            return ret
        return _wrapper
    return _transaction_deco


def process_queue():
    '''
    Process the message queue on a separate thread.
    '''
    lock = Lock()
    lock.acquire()
    while len(app_globals.changelog):
        send_event_msg(*app_globals.changelog.popleft())
    lock.release()


def send_event_msg(remove_trp, add_trp, metadata):
    '''
    Break down delta triples, find subjects and send event message.
    '''
    remove_grp = groupby(remove_trp, lambda x : x[0])
    remove_dict = { k[0] : k[1] for k in remove_grp }

    add_grp = groupby(add_trp, lambda x : x[0])
    add_dict = { k[0] : k[1] for k in add_grp }

    subjects = set(remove_dict.keys()) | set(add_dict.keys())
    for rsrc_uri in subjects:
        logger.info('subject: {}'.format(rsrc_uri))
        app_globals.messenger.send


### API METHODS ###

@transaction()
def get(uid, repr_options={}):
    '''
    Get an LDPR resource.

    The resource comes preloaded with user data and metadata as indicated by
    the `repr_options` argument. Any further handling of this resource is done
    outside of a transaction.

    @param uid (string) Resource UID.
    @param repr_options (dict(bool)) Representation options. This is a dict
    that is unpacked downstream in the process. The default empty dict results
    in default values. The accepted dict keys are:
    - incl_inbound: include inbound references. Default: False.
    - incl_children: include children URIs. Default: True.
    - embed_children: Embed full graph of all child resources. Default: False
    '''
    rsrc = LdpFactory.from_stored(uid, repr_options)
    # Load graph before leaving the transaction.
    rsrc.imr

    return rsrc


@transaction()
def get_version_info(uid):
    '''
    Get version metadata (fcr:versions).
    '''
    return LdpFactory.from_stored(uid).version_info


@transaction()
def get_version(uid, ver_uid):
    '''
    Get version metadata (fcr:versions).
    '''
    return LdpFactory.from_stored(uid).get_version(ver_uid)


@transaction(True)
def create(parent, slug, **kwargs):
    '''
    Mint a new UID and create a resource.

    The UID is computed from a given parent UID and a "slug", a proposed path
    relative to the parent. The application will attempt to use the suggested
    path but it may use a different one if a conflict with an existing resource
    arises.

    @param parent (string) UID of the parent resource.
    @param slug (string) Tentative path relative to the parent UID.
    @param **kwargs Other parameters are passed to the
    LdpFactory.from_provided method. Please see the documentation for that
    method for explanation of individual parameters.

    @return string UID of the new resource.
    '''
    uid = LdpFactory.mint_uid(parent, slug)
    logger.debug('Minted UID for new resource: {}'.format(uid))
    rsrc = LdpFactory.from_provided(uid, **kwargs)

    rsrc.create_or_replace_rsrc(create_only=True)

    return uid


@transaction(True)
def create_or_replace(uid, stream=None, **kwargs):
    '''
    Create or replace a resource with a specified UID.

    If the resource already exists, all user-provided properties of the
    existing resource are deleted. If the resource exists and the provided
    content is empty, an exception is raised (not sure why, but that's how
    FCREPO4 handles it).

    @param uid (string) UID of the resource to be created or updated.
    @param stream (BytesIO) Content stream. If empty, an empty container is
    created.
    @param **kwargs Other parameters are passed to the
    LdpFactory.from_provided method. Please see the documentation for that
    method for explanation of individual parameters.

    @return string Event type: whether the resource was created or updated.
    '''
    rsrc = LdpFactory.from_provided(uid, stream=stream, **kwargs)

    if not stream and rsrc.is_stored:
        raise InvalidResourceError(rsrc.uid,
                'Resource {} already exists and no data set was provided.')

    return rsrc.create_or_replace_rsrc()


@transaction(True)
def update(uid, update_str, is_metadata=False):
    '''
    Update a resource with a SPARQL-Update string.

    @param uid (string) Resource UID.
    @param update_str (string) SPARQL-Update statements.
    @param is_metadata (bool) Whether the resource metadata is being updated.
    If False, and the resource being updated is a LDP-NR, an error is raised.
    '''
    rsrc = LdpFactory.from_stored(uid)
    if LDP_NR_TYPE in rsrc.ldp_types:
        if is_metadata:
            rsrc.patch_metadata(update_str)
        else:
            raise InvalidResourceError(uid)
    else:
        rsrc.patch(update_str)

    return rsrc


@transaction(True)
def create_version(uid, ver_uid):
    '''
    Create a resource version.

    @param uid (string) Resource UID.
    @param ver_uid (string) Version UID to be appended to the resource URI.
    NOTE: this is a "slug", i.e. the version URI is not guaranteed to be the
    one indicated.

    @return string Version UID.
    '''
    return LdpFactory.from_stored(uid).create_version(ver_uid)


@transaction(True)
def delete(uid, leave_tstone=True):
    '''
    Delete a resource.

    @param uid (string) Resource UID.
    @param leave_tstone (bool) Whether to perform a soft-delete and leave a
    tombstone resource, or wipe any memory of the resource.
    '''
    # If referential integrity is enforced, grab all inbound relationships
    # to break them.
    refint = app_globals.rdfly.config['referential_integrity']
    inbound = True if refint else inbound
    repr_opts = {'incl_inbound' : True} if refint else {}

    children = app_globals.rdfly.get_descendants(uid)

    if leave_tstone:
        rsrc = LdpFactory.from_stored(uid, repr_opts)
        ret = rsrc.bury_rsrc(inbound)

        for child_uri in children:
            try:
                child_rsrc = LdpFactory.from_stored(
                    app_globals.rdfly.uri_to_uid(child_uri),
                    repr_opts={'incl_children' : False})
            except (TombstoneError, ResourceNotExistsError):
                continue
            child_rsrc.bury_rsrc(inbound, tstone_pointer=rsrc.uri)
    else:
        ret = forget(uid, inbound)
        for child_uri in children:
            forget(app_globals.rdfly.uri_to_uid(child_uri), inbound)

    return ret


@transaction(True)
def resurrect(uid):
    '''
    Reinstate a buried (soft-deleted) resource.

    @param uid (string) Resource UID.
    '''
    return LdpFactory.from_stored(uid).resurrect_rsrc()


@transaction(True)
def forget(uid, inbound=True):
    '''
    Delete a resource completely, removing all its traces.

    @param uid (string) Resource UID.
    @param inbound (bool) Whether the inbound relationships should be deleted
    as well. If referential integrity is checked system-wide inbound references
    are always deleted and this option has no effect.
    '''
    refint = app_globals.rdfly.config['referential_integrity']
    inbound = True if refint else inbound
    app_globals.rdfly.forget_rsrc(uid, inbound)

    return RES_DELETED

