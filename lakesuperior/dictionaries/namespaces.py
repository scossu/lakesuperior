import rdflib

from rdflib import Graph
from rdflib.namespace import Namespace, NamespaceManager

from lakesuperior import env

ns_collection  = {
        pfx: Namespace(ns)
        for pfx, ns in env.app_globals.config['namespaces'].items()}

ns_mgr = NamespaceManager(Graph())

# Collection of prefixes in a dict.
for ns,uri in ns_collection.items():
    ns_mgr.bind(ns, uri, override=False)
