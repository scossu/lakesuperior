import logging

from rdflib import Graph

from lakesuperior import env
from lakesuperior.globals import RES_UPDATED
from lakesuperior.dictionaries.namespaces import ns_collection as nsc
from lakesuperior.model.ldpr import Ldpr


logger = logging.getLogger(__name__)


class LdpRs(Ldpr):
    """
    LDP-RS (LDP RDF source).

    https://www.w3.org/TR/ldp/#ldprs
    """
    def __init__(self, uuid, repr_opts={}, handling='lenient', **kwargs):
        """
        Extends :meth:`Ldpr.__init__`by adding LDP-RS specific parameters.

        :param str handling: One of ``strict``, ``lenient`` (the default) or
        ``none``. ``strict`` raises an error if a server-managed term is in the
        graph. ``lenient`` removes all sever-managed triples encountered.
        ``none`` skips all server-managed checks. It is used for internal
        modifications.
        """
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



class Ldpc(LdpRs):
    """LDPC (LDP Container)."""
    def __init__(self, uuid, *args, **kwargs):
        super().__init__(uuid, *args, **kwargs)
        self.base_types |= {
            nsc['fcrepo'].Container,
            nsc['ldp'].Container,
        }



class LdpBc(Ldpc):
    """LDP-BC (LDP Basic Container)."""
    def __init__(self, uuid, *args, **kwargs):
        super().__init__(uuid, *args, **kwargs)
        self.base_types |= {
            nsc['ldp'].BasicContainer,
        }



class LdpDc(Ldpc):
    """LDP-DC (LDP Direct Container)."""
    def __init__(self, uuid, *args, **kwargs):
        super().__init__(uuid, *args, **kwargs)
        self.base_types |= {
            nsc['ldp'].DirectContainer,
        }



class LdpIc(Ldpc):
    """LDP-IC (LDP Indirect Container)."""
    def __init__(self, uuid, *args, **kwargs):
        super().__init__(uuid, *args, **kwargs)
        self.base_types |= {
            nsc['ldp'].IndirectContainer,
        }

