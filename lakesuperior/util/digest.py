import pickle

from hashlib import sha1

from rdflib.term import Literal, URIRef, Variable

from lakesuperior.core.namespaces import ns_collection as nsc


class Digest:
    '''
    Various digest functions. May be merged into something more generic later.
    '''
    @staticmethod
    def rdf_cksum(g):
        '''
        Generate a checksum for a graph.

        This is not straightforward because a graph is derived from an
        unordered data structure (RDF).

        What this method does is ordering the graph by subject, predicate,
        object, then creating a pickle string and a checksum of it.

        N.B. The context of the triples is ignored, so isomorphic graphs would
        have the same checksum regardless of the context(s) they are found in.

        @TODO This can be later reworked to use a custom hashing algorithm.

        @param rdflib.Graph g The graph to be hashed.

        @return string SHA1 checksum.
        '''
        # Remove the messageDigest property, which very likely reflects the
        # previous state of the resource.
        g.remove((Variable('s'), nsc['premis'].messageDigest, Variable('o')))

        ord_g = sorted(list(g), key=lambda x : (x[0], x[1], x[2]))
        hash = sha1(pickle.dumps(ord_g)).hexdigest()

        return hash


    @staticmethod
    def non_rdf_checksum(data):
        '''
        Generate a checksum of non-RDF content.

        @TODO This can be later reworked to use a custom hashing algorithm.
        '''
        return sha1(data).hexdigest()

