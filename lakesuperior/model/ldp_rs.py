import logging

from rdflib import Graph

from lakesuperior.env import env
from lakesuperior.globals import RES_UPDATED
from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.model.ldpr import Ldpr


logger = logging.getLogger(__name__)


class LdpRs(Ldpr):
    '''LDP-RS (LDP RDF source).

    Definition: https://www.w3.org/TR/ldp/#ldprs
    '''
    def __init__(self, uuid, repr_opts={}, handling='lenient', **kwargs):
        '''
        Extends Ldpr.__init__ by adding LDP-RS specific parameters.

        @param handling (string) One of `strict`, `lenient` (the default) or
        `none`. `strict` raises an error if a server-managed term is in the
        graph. `lenient` removes all sever-managed triples encountered. `none`
        skips all server-managed checks. It is used for internal modifications.
        '''
        super().__init__(uuid, **kwargs)
        self.base_types = super().base_types | {
            nsc['fcrepo'].Container,
            nsc['ldp'].Container,
        }

        # provided_imr can be empty. If None, it is an outbound resource.
        if self.provided_imr is not None:
            self.workflow = self.WRKF_INBOUND
        else:
            self.workflow = self.WRKF_OUTBOUND
            self._imr_options = repr_opts

        self.handling = handling


    ## LDP METHODS ##

    def patch(self, update_str):
        '''
        Update an existing resource by applying a SPARQL-UPDATE query.

        @param update_str (string) SPARQL-Update staements.
        '''
        self.handling = 'lenient' # FCREPO does that and Hyrax requires it.

        return self._sparql_update(update_str)


    #def _sparql_delta(self, q):
    #    '''
    #    Calculate the delta obtained by a SPARQL Update operation.

    #    This is a critical component of the SPARQL update prcess and does a
    #    couple of things:

    #    1. It ensures that no resources outside of the subject of the request
    #    are modified (e.g. by variable subjects)
    #    2. It verifies that none of the terms being modified is server managed.

    #    This method extracts an in-memory copy of the resource and performs the
    #    query on that once it has checked if any of the server managed terms is
    #    in the delta. If it is, it raises an exception.

    #    NOTE: This only checks if a server-managed term is effectively being
    #    modified. If a server-managed term is present in the query but does not
    #    cause any change in the updated resource, no error is raised.

    #    @return tuple(rdflib.Graph) Remove and add graphs. These can be used
    #    with `BaseStoreLayout.update_resource` and/or recorded as separate
    #    events in a provenance tracking system.
    #    '''
    #    logger.debug('Provided SPARQL query: {}'.format(q))
    #    pre_gr = self.imr.graph

    #    post_gr = pre_gr | Graph()
    #    post_gr.update(q)

    #    remove_gr, add_gr = self._dedup_deltas(pre_gr, post_gr)

    #    #logger.debug('Removing: {}'.format(
    #    #    remove_gr.serialize(format='turtle').decode('utf8')))
    #    #logger.debug('Adding: {}'.format(
    #    #    add_gr.serialize(format='turtle').decode('utf8')))

    #    remove_gr = self._check_mgd_terms(remove_gr)
    #    add_gr = self._check_mgd_terms(add_gr)

    #    return set(remove_gr), set(add_gr)



class Ldpc(LdpRs):
    '''LDPC (LDP Container).'''

    def __init__(self, uuid, *args, **kwargs):
        super().__init__(uuid, *args, **kwargs)
        self.base_types |= {
            nsc['fcrepo'].Container,
            nsc['ldp'].Container,
        }



class LdpBc(Ldpc):
    '''LDP-BC (LDP Basic Container).'''
    def __init__(self, uuid, *args, **kwargs):
        super().__init__(uuid, *args, **kwargs)
        self.base_types |= {
            nsc['ldp'].BasicContainer,
        }



class LdpDc(Ldpc):
    '''LDP-DC (LDP Direct Container).'''

    def __init__(self, uuid, *args, **kwargs):
        super().__init__(uuid, *args, **kwargs)
        self.base_types |= {
            nsc['ldp'].DirectContainer,
        }



class LdpIc(Ldpc):
    '''LDP-IC (LDP Indirect Container).'''

    def __init__(self, uuid, *args, **kwargs):
        super().__init__(uuid, *args, **kwargs)
        self.base_types |= {
            nsc['ldp'].IndirectContainer,
        }

