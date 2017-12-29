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
    'fcrconfig' : Namespace('http://fedora.info/definitions/v4/config#'),
    'iana' : Namespace('http://www.iana.org/assignments/relation/'),
    'ldp' : Namespace('http://www.w3.org/ns/ldp#'),
    'premis' : Namespace('http://www.loc.gov/premis/rdf/v1#'),
    'rdf' : rdflib.namespace.RDF,
    'rdfs' : rdflib.namespace.RDFS,
    # For info: vs. urn:, see https://tools.ietf.org/html/rfc4452#section-6.3
    'fcres' : Namespace('info:fcres/'),
    'fcmeta' : Namespace('info:fcmeta/'),
    'fcstate' : Namespace('info:fcstate/'),
    'fcsystem' : Namespace('info:fcsystem/'),
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
