from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.model.ldpr import Ldpr, atomic

class LdpRs(Ldpr):
    '''LDP-RS (LDP RDF source).

    Definition: https://www.w3.org/TR/ldp/#ldprs
    '''

    base_types = {
        nsc['fcrepo'].Resource,
        nsc['ldp'].Resource,
        nsc['ldp'].RDFSource,
    }


    def __init__(self, uuid, repr_opts={}, handling='strict', **kwargs):
        '''
        Extends Ldpr.__init__ by adding LDP-RS specific parameters.

        @param handling (string) One of `strict` (the default), `lenient` or
        `none`. `strict` raises an error if a server-managed term is in the
        graph. `lenient` removes all sever-managed triples encountered. `none`
        skips all server-managed checks. It is used for internal modifications.
        '''
        super().__init__(uuid, **kwargs)

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
        delta = self._sparql_delta(update_str.replace('<>', self.urn.n3()))

        return self._modify_rsrc(self.RES_UPDATED, *delta)



class Ldpc(LdpRs):
    '''LDPC (LDP Container).'''

    def __init__(self, uuid, *args, **kwargs):
        super().__init__(uuid, *args, **kwargs)
        self.base_types.update({
            nsc['ldp'].Container,
        })




class LdpBc(Ldpc):
    '''LDP-BC (LDP Basic Container).'''
    def __init__(self, uuid, *args, **kwargs):
        super().__init__(uuid, *args, **kwargs)
        self.base_types.update({
            nsc['ldp'].BasicContainer,
        })



class LdpDc(Ldpc):
    '''LDP-DC (LDP Direct Container).'''

    def __init__(self, uuid, *args, **kwargs):
        super().__init__(uuid, *args, **kwargs)
        self.base_types.update({
            nsc['ldp'].DirectContainer,
        })



class LdpIc(Ldpc):
    '''LDP-IC (LDP Indirect Container).'''

    def __init__(self, uuid, *args, **kwargs):
        super().__init__(uuid, *args, **kwargs)
        self.base_types.update({
            nsc['ldp'].IndirectContainer,
        })





