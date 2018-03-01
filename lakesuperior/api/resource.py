from functools import wraps
from multiprocessing import Process
from threading import Lock, Thread

from flask import (
        Blueprint, current_app, g, make_response, render_template,
        request, send_file)

from lakesuperior.store.ldp_rs.lmdb_store import TxnManager


def transaction(write=False):
    '''
    Handle atomic operations in a store.

    This wrapper ensures that a write operation is performed atomically. It
    also takes care of sending a message for each resource changed in the
    transaction.
    '''
    def _transaction_deco(fn):
        @wraps(fn)
        def _wrapper(*args, **kwargs):
            if not hasattr(g, 'changelog'):
                g.changelog = []
            store = current_app.rdfly.store
            with TxnManager(store, write=write) as txn:
                ret = fn(*args, **kwargs)
            if len(g.changelog):
                job = Thread(target=process_queue)
                job.start()
            return ret

        return _wrapper
    return _transaction_deco


def process_queue():
    '''
    Process the message queue on a separate thread.
    '''
    lock = Lock()
    lock.acquire()
    while len(g.changelog):
        send_event_msg(g.changelog.pop())
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
        self._logger.info('subject: {}'.format(rsrc_uri))
        #current_app.messenger.send
