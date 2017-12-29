from copy import deepcopy

from flask import g

from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.model.ldpr import Ldpr, atomic

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

    @atomic
    def patch(self, update_str):
        '''
        https://www.w3.org/TR/ldp/#ldpr-HTTP_PATCH

        Update an existing resource by applying a SPARQL-UPDATE query.

        @param update_str (string) SPARQL-Update staements.
        '''
        local_update_str = g.tbox.localize_ext_str(update_str, self.urn)
        delta = self._sparql_delta(local_update_str)
        self._ensure_single_subject_rdf(delta[0], add_fragment=False)
        self._ensure_single_subject_rdf(delta[1])

        return self._modify_rsrc(self.RES_UPDATED, *delta)


    def _sparql_delta(self, q):
        '''
        Calculate the delta obtained by a SPARQL Update operation.

        This is a critical component of the SPARQL query prcess and does a
        couple of things:

        1. It ensures that no resources outside of the subject of the request
        are modified (e.g. by variable subjects)
        2. It verifies that none of the terms being modified is server managed.

        This method extracts an in-memory copy of the resource and performs the
        query on that once it has checked if any of the server managed terms is
        in the delta. If it is, it raises an exception.

        NOTE: This only checks if a server-managed term is effectively being
        modified. If a server-managed term is present in the query but does not
        cause any change in the updated resource, no error is raised.

        @return tuple(rdflib.Graph) Remove and add graphs. These can be used
        with `BaseStoreLayout.update_resource` and/or recorded as separate
        events in a provenance tracking system.
        '''
        #self._logger.debug('Provided SPARQL query: {}'.format(q))
        pre_gr = self.imr.graph

        post_gr = pre_g | Graph()
        post_gr.update(q)

        remove_gr, add_gr = self._dedup_deltas(pre_gr, post_gr)

        #self._logger.info('Removing: {}'.format(
        #    remove_gr.serialize(format='turtle').decode('utf8')))
        #self._logger.info('Adding: {}'.format(
        #    add_gr.serialize(format='turtle').decode('utf8')))

        remove_gr = self._check_mgd_terms(remove_gr)
        add_gr = self._check_mgd_terms(add_gr)

        return remove_gr, add_gr



class Ldpc(LdpRs):
    '''LDPC (LDP Container).'''

    def __init__(self, uuid, *args, **kwargs):
        super().__init__(uuid, *args, **kwargs)
        self.base_types = super().base_types | {
            nsc['fcrepo'].Container,
            nsc['ldp'].Container,
        }



class LdpBc(Ldpc):
    '''LDP-BC (LDP Basic Container).'''
    def __init__(self, uuid, *args, **kwargs):
        super().__init__(uuid, *args, **kwargs)
        self.base_types = super().base_types | {
            nsc['ldp'].BasicContainer,
        }



class LdpDc(Ldpc):
    '''LDP-DC (LDP Direct Container).'''

    def __init__(self, uuid, *args, **kwargs):
        super().__init__(uuid, *args, **kwargs)
        self.base_types = super().base_types | {
            nsc['ldp'].DirectContainer,
        }



class LdpIc(Ldpc):
    '''LDP-IC (LDP Indirect Container).'''

    def __init__(self, uuid, *args, **kwargs):
        super().__init__(uuid, *args, **kwargs)
        self.base_types = super().base_types | {
            nsc['ldp'].IndirectContainer,
        }





