import rdflib

from rdflib import Graph
from rdflib.namespace import Namespace, NamespaceManager

from lakesuperior.config_parser import config

# Core namespace prefixes. These add to and override any user-defined prefixes.
# @TODO Some of these have been copy-pasted from FCREPO4 and may be deprecated.
core_namespaces = {
    'authz' : Namespace('http://fedora.info/definitions/v4/authorization#'),
    'cnt' : Namespace('http://www.w3.org/2011/content#'),
    'dc' : rdflib.namespace.DC,
    'dcterms' : rdflib.namespace.DCTERMS,
    'ebucore' : Namespace('http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#'),
    'fedora' : Namespace('http://fedora.info/definitions/v4/repository#'),
    'fedoraconfig' : Namespace('http://fedora.info/definitions/v4/config#'), # fcrepo =< 4.7
    'gen' : Namespace('http://www.w3.org/2006/gen/ont#'),
    'iana' : Namespace('http://www.iana.org/assignments/relation/'),
    'lake' : Namespace('http://definitions.artic.edu/ontology/lake#'),
    'lakemeta' : Namespace('http://definitions.artic.edu/ontology/lake/metadata#'),
    'ldp' : Namespace('http://www.w3.org/ns/ldp#'),
    'owl' : rdflib.namespace.OWL,
    'premis' : Namespace('http://www.loc.gov/premis/rdf/v1#'),
    'rdf' : rdflib.namespace.RDF,
    'rdfs' : rdflib.namespace.RDFS,
    'fcg' : Namespace('urn:fcgraph:'),
    'fcres' : Namespace('urn:fcres:'),
    'fcstate' : Namespace('urn:fcstate:'),
    'webac' : Namespace('http://www.w3.org/ns/auth/acl#'),
    'xml' : Namespace('http://www.w3.org/XML/1998/namespace'),
    'xsd' : rdflib.namespace.XSD,
    'xsi' : Namespace('http://www.w3.org/2001/XMLSchema-instance'),
}

ns_collection = core_namespaces.copy()
ns_collection.update(config['namespaces'])

ns_mgr = NamespaceManager(Graph())
ns_pfx_sparql = dict()

# Collection of prefixes in a dict.
for ns,uri in ns_collection.items():
    ns_mgr.bind(ns, uri, override=False)
    #ns_pfx_sparql[ns] = 'PREFIX {}: <{}>'.format(ns, uri)

# Prefix declarations formatted for SPARQL queries.
#pfx_decl='\n'.join(ns_pfx_sparql.values())

