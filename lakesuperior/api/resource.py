import logging

from functools import wraps
from itertools import groupby
from multiprocessing import Process
from threading import Lock, Thread

import arrow

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import XSD

from lakesuperior.config_parser import config
from lakesuperior.exceptions import (
        InvalidResourceError, ResourceNotExistsError, TombstoneError)
from lakesuperior import env, thread_env
from lakesuperior.globals import RES_DELETED, RES_UPDATED
from lakesuperior.model.ldp_factory import LDP_NR_TYPE, LdpFactory
from lakesuperior.store.ldp_rs.lmdb_store import TxnManager


logger = logging.getLogger(__name__)

__doc__ = """
Primary API for resource manipulation.

Quickstart:

>>> # First import default configuration and globals—only done once.
>>> import lakesuperior.default_env
>>> from lakesuperior.api import resource
>>> # Get root resource.
>>> rsrc = resource.get('/')
>>> # Dump graph.
>>> set(rsrc.imr)
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
"""

def transaction(write=False):
    """
    Handle atomic operations in a store.

    This wrapper ensures that a write operation is performed atomically. It
    also takes care of sending a message for each resource changed in the
    transaction.

    ALL write operations on the LDP-RS and LDP-NR stores go through this
    wrapper.
    """
    def _transaction_deco(fn):
        @wraps(fn)
        def _wrapper(*args, **kwargs):
            # Mark transaction begin timestamp. This is used for create and
            # update timestamps on resources.
            thread_env.timestamp = arrow.utcnow()
            thread_env.timestamp_term = Literal(
                    thread_env.timestamp, datatype=XSD.dateTime)
            with TxnManager(env.app_globals.rdf_store, write=write) as txn:
                ret = fn(*args, **kwargs)
            if len(env.app_globals.changelog):
                job = Thread(target=_process_queue)
                job.start()
            delattr(thread_env, 'timestamp')
            delattr(thread_env, 'timestamp_term')
            return ret
        return _wrapper
    return _transaction_deco


def _process_queue():
    """
    Process the message queue on a separate thread.
    """
    lock = Lock()
    lock.acquire()
    while len(env.app_globals.changelog):
        _send_event_msg(*env.app_globals.changelog.popleft())
    lock.release()


def _send_event_msg(remove_trp, add_trp, metadata):
    """
    Send messages about a changed LDPR.

    A single LDPR message packet can contain multiple resource subjects, e.g.
    if the resource graph contains hash URIs or even other subjects. This
    method groups triples by subject and sends a message for each of the
    subjects found.
    """
    # Group delta triples by subject.
    remove_grp = groupby(remove_trp, lambda x : x[0])
    remove_dict = {k[0]: k[1] for k in remove_grp}

    add_grp = groupby(add_trp, lambda x : x[0])
    add_dict = {k[0]: k[1] for k in add_grp}

    subjects = set(remove_dict.keys()) | set(add_dict.keys())
    for rsrc_uri in subjects:
        logger.debug('Processing event for subject: {}'.format(rsrc_uri))
        env.app_globals.messenger.send(rsrc_uri, **metadata)


### API METHODS ###

@transaction()
def exists(uid):
    """
    Return whether a resource exists (is stored) in the repository.

    :param string uid: Resource UID.
    """
    try:
        exists = LdpFactory.from_stored(uid).is_stored
    except ResourceNotExistsError:
        exists = False
    return exists


@transaction()
def get_metadata(uid):
    """
    Get metadata (admin triples) of an LDPR resource.

    :param string uid: Resource UID.
    """
    return LdpFactory.from_stored(uid).metadata


@transaction()
def get(uid, repr_options={}):
    """
    Get an LDPR resource.

    The resource comes preloaded with user data and metadata as indicated by
    the `repr_options` argument. Any further handling of this resource is done
    outside of a transaction.

    :param string uid: Resource UID.
    :param  repr_options: (dict(bool)) Representation options. This is a dict
        that is unpacked downstream in the process. The default empty dict
        results in default values. The accepted dict keys are:

    - incl_inbound: include inbound references. Default: False.
    - incl_children: include children URIs. Default: True.
    - embed_children: Embed full graph of all child resources. Default: False
    """
    rsrc = LdpFactory.from_stored(uid, repr_opts=repr_options)
    # Load graph before leaving the transaction.
    rsrc.imr

    return rsrc


@transaction()
def get_version_info(uid):
    """
    Get version metadata (fcr:versions).
    """
    return LdpFactory.from_stored(uid).version_info


@transaction()
def get_version(uid, ver_uid):
    """
    Get version metadata (fcr:versions).
    """
    return LdpFactory.from_stored(uid).get_version(ver_uid)


@transaction(True)
def create(parent, slug, **kwargs):
    r"""
    Mint a new UID and create a resource.

    The UID is computed from a given parent UID and a "slug", a proposed path
    relative to the parent. The application will attempt to use the suggested
    path but it may use a different one if a conflict with an existing resource
    arises.

    :param str parent: UID of the parent resource.
    :param str slug: Tentative path relative to the parent UID.
    :param \*\*kwargs: Other parameters are passed to the
      :py:meth:`~lakesuperior.model.ldp_factory.LdpFactory.from_provided`
      method.

    :rtype: str
    :return: UID of the new resource.
    """
    uid = LdpFactory.mint_uid(parent, slug)
    logger.debug('Minted UID for new resource: {}'.format(uid))
    rsrc = LdpFactory.from_provided(uid, **kwargs)

    rsrc.create_or_replace(create_only=True)

    return uid


@transaction(True)
def create_or_replace(uid, **kwargs):
    r"""
    Create or replace a resource with a specified UID.

    :param string uid: UID of the resource to be created or updated.
    :param \*\*kwargs: Other parameters are passed to the
        :py:meth:`~lakesuperior.model.ldp_factory.LdpFactory.from_provided`
        method.

    :rtype: str
    :return: Event type: whether the resource was created or updated.
    """
    return LdpFactory.from_provided(uid, **kwargs).create_or_replace()


@transaction(True)
def update(uid, update_str, is_metadata=False):
    """
    Update a resource with a SPARQL-Update string.

    :param string uid: Resource UID.
    :param string update_str: SPARQL-Update statements.
    :param bool is_metadata: Whether the resource metadata are being updated.

    :raise InvalidResourceError: If ``is_metadata`` is False and the resource
        being updated is a LDP-NR.
    """
    # FCREPO is lenient here and Hyrax requires it.
    rsrc = LdpFactory.from_stored(uid, handling='lenient')
    if LDP_NR_TYPE in rsrc.ldp_types and not is_metadata:
        raise InvalidResourceError(
                'Cannot use this method to update an LDP-NR content.')

    delta = rsrc.sparql_delta(update_str)
    rsrc.modify(RES_UPDATED, *delta)

    return rsrc


@transaction(True)
def update_delta(uid, remove_trp, add_trp):
    """
    Update a resource graph (LDP-RS or LDP-NR) with sets of add/remove triples.

    A set of triples to add and/or a set of triples to remove may be provided.

    :param string uid: Resource UID.
    :param set(tuple(rdflib.term.Identifier)) remove_trp: Triples to
        remove, as 3-tuples of RDFLib terms.
    :param set(tuple(rdflib.term.Identifier)) add_trp: Triples to
        add, as 3-tuples of RDFLib terms.
    """
    rsrc = LdpFactory.from_stored(uid)
    remove_trp = rsrc.check_mgd_terms(remove_trp)
    add_trp = rsrc.check_mgd_terms(add_trp)

    return rsrc.modify(RES_UPDATED, remove_trp, add_trp)


@transaction(True)
def create_version(uid, ver_uid):
    """
    Create a resource version.

    :param string uid: Resource UID.
    :param string ver_uid: Version UID to be appended to the resource URI.
      NOTE: this is a "slug", i.e. the version URI is not guaranteed to be the
      one indicated.

    :rtype: str
    :return: Version UID.
    """
    return LdpFactory.from_stored(uid).create_version(ver_uid)


@transaction(True)
def delete(uid, soft=True, inbound=True):
    """
    Delete a resource.

    :param string uid: Resource UID.
    :param bool soft: Whether to perform a soft-delete and leave a
        tombstone resource, or wipe any memory of the resource.
    """
    # If referential integrity is enforced, grab all inbound relationships
    # to break them.
    refint = env.app_globals.rdfly.config['referential_integrity']
    inbound = True if refint else inbound
    repr_opts = {'incl_inbound' : True} if refint else {}

    rsrc = LdpFactory.from_stored(uid, repr_opts, strict=soft)
    if soft:
        return rsrc.bury(inbound)
    else:
        return rsrc.forget(inbound)


@transaction(True)
def revert_to_version(uid, ver_uid):
    """
    Restore a resource to a previous version state.

    :param str uid: Resource UID.
    :param str ver_uid: Version UID.
    """
    return LdpFactory.from_stored(uid).revert_to_version(ver_uid)


@transaction(True)
def resurrect(uid):
    """
    Reinstate a buried (soft-deleted) resource.

    :param str uid: Resource UID.
    """
    try:
        rsrc = LdpFactory.from_stored(uid)
    except TombstoneError as e:
        if e.uid != uid:
            raise
        else:
            return LdpFactory.from_stored(uid, strict=False).resurrect()
    else:
        raise InvalidResourceError(
                uid, msg='Resource {} is not dead.'.format(uid))
