import logging

from rdflib import Graph

from lakesuperior import env
from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.model.ldp.ldpr import RES_UPDATED, Ldpr


logger = logging.getLogger(__name__)


class LdpRs(Ldpr):
    """
    LDP-RS (LDP RDF source).

    https://www.w3.org/TR/ldp/#ldprs
    """

    def __init__(self, *args, **kwargs):
        """
        Extends :meth:`lakesuperior.model.Ldpr.__init__` by adding LDP-RS
        specific parameters.
        """
        super().__init__(*args, **kwargs)

        self.base_types = super().base_types | {
            nsc['ldp'].RDFSource,
        }



class Ldpc(LdpRs):
    """LDPC (LDP Container)."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base_types |= {
            nsc['fcrepo'].Container,
            nsc['ldp'].Container,
        }



class LdpBc(Ldpc):
    """LDP-BC (LDP Basic Container)."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base_types |= {
            nsc['ldp'].BasicContainer,
        }



class LdpDc(Ldpc):
    """LDP-DC (LDP Direct Container)."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base_types |= {
            nsc['ldp'].DirectContainer,
        }



class LdpIc(Ldpc):
    """LDP-IC (LDP Indirect Container)."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base_types |= {
            nsc['ldp'].IndirectContainer,
        }

