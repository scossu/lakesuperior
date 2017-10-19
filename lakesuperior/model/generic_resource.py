from rdflib import Resource

class GenericResource(Resource):
    '''
    Generic RDF resource that extends from rdflib.Resource.

    This should also serve as the base class for LDP resource classes. Some
    convenience methods missing in that class can also be added here.
    '''

    def extract(self, p=None, o=None):
        '''
        Extract an in-memory copy of the resource containing either a
        sub-graph, defined with the `p` and `o` parameters, or the whole
        resource.
        '''
        # @TODO
        pass
