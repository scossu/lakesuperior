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
    'fcrepo' : Namespace('http://fedora.info/definitions/v4/repository#'),
    'fcadmin' : Namespace('info:fcsystem/graph/admin'),
    'fcres' : Namespace('info:fcres'),
    'fcmain' : Namespace('info:fcsystem/graph/userdata/_main'),
    'fcstruct' : Namespace('info:fcsystem/graph/structure'),
    'fcsystem' : Namespace('info:fcsystem/'),
    'foaf': Namespace('http://xmlns.com/foaf/0.1/'),
    'iana' : Namespace('http://www.iana.org/assignments/relation/'),
    'ldp' : Namespace('http://www.w3.org/ns/ldp#'),
    'pcdm': Namespace('http://pcdm.org/models#'),
    'premis' : Namespace('http://www.loc.gov/premis/rdf/v1#'),
    'rdf' : rdflib.namespace.RDF,
    'rdfs' : rdflib.namespace.RDFS,
    'webac' : Namespace('http://www.w3.org/ns/auth/acl#'),
    'xsd' : rdflib.namespace.XSD,
}

ns_collection = core_namespaces.copy()
custom_ns = {pfx: Namespace(ns) for pfx, ns in config['namespaces'].items()}
ns_collection.update(custom_ns)

ns_mgr = NamespaceManager(Graph())

# Collection of prefixes in a dict.
for ns,uri in ns_collection.items():
    ns_mgr.bind(ns, uri, override=False)
