from flask import g, current_app
from rdflib import Graph

from lakesuperior.model.ldp_nr import Ldpc


class Version(Ldpc):
    '''
    A resource version.
    '''

    def __init__(self, parent_uuid, label):
        self.uuid = parent_uuid + '/' + label
        self.urn = nsc['fcres'][uuid]
        self.uri = g.tbox.uuid_to_uri(self.uuid)

        self.rdfly = current_app.rdfly

        self.parent_urn = nsc['fcres'][parent_uuid]
        self.label = label


    def create(self):
        if not self.rdfly.ask_rsrc_exists(self.parent_urn):
            add_gr = Graph()
            self._modify_rsrc(self.RES_CREATED, add_trp=add_gr)
