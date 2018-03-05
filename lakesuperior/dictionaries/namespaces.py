import rdflib

from rdflib import Graph
from rdflib.namespace import Namespace, NamespaceManager

from lakesuperior.config_parser import config

# Core namespace prefixes. These add to and override any user-defined prefixes.
# @TODO Some of these have been copy-pasted from FCREPO4 and may be deprecated.
core_namespaces = {
    'dc' : rdflib.namespace.DC,
    'dcterms' : rdflib.namespace.DCTERMS,
    'ebucore' : Namespace(
        'http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#'),
    #'fcrconfig' : Namespace('http://fedora.info/definitions/v4/config#'),
    'fcrepo' : Namespace('http://fedora.info/definitions/v4/repository#'),
    'fcadmin' : Namespace('info:fcsystem/graph/admin'),
    'fcres' : Namespace('info:fcres'),
    'fcmain' : Namespace('info:fcsystem/graph/userdata/_main'),
    'fcstruct' : Namespace('info:fcsystem/graph/structure'),
    'fcsystem' : Namespace('info:fcsystem/'),
    'foaf': Namespace('http://xmlns.com/foaf/0.1/'),
    'iana' : Namespace('http://www.iana.org/assignments/relation/'),
    'ldp' : Namespace('http://www.w3.org/ns/ldp#'),
    # This is used in the layout attribute router.
    'pcdm': Namespace('http://pcdm.org/models#'),
    'premis' : Namespace('http://www.loc.gov/premis/rdf/v1#'),
    'rdf' : rdflib.namespace.RDF,
    'rdfs' : rdflib.namespace.RDFS,
    'webac' : Namespace('http://www.w3.org/ns/auth/acl#'),
    'xml' : Namespace('http://www.w3.org/XML/1998/namespace'),
    'xsd' : rdflib.namespace.XSD,
    'xsi' : Namespace('http://www.w3.org/2001/XMLSchema-instance'),
}

ns_collection = core_namespaces.copy()
ns_collection.update(config['namespaces'])

ns_mgr = NamespaceManager(Graph())
ns_pfx_sparql = {}

# Collection of prefixes in a dict.
for ns,uri in ns_collection.items():
    ns_mgr.bind(ns, uri, override=False)
    #ns_pfx_sparql[ns] = 'PREFIX {}: <{}>'.format(ns, uri)
