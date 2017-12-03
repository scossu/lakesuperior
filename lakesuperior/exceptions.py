''' Put all exceptions here. '''

class ResourceError(RuntimeError):
    '''
    Raised in an attempt to create a resource a URI that already exists and is
    not supposed to.

    This usually surfaces at the HTTP level as a 409.
    '''
    def __init__(self, uuid, msg=None):
        self.uuid = uuid
        self.msg = msg


class ResourceExistsError(ResourceError):
    '''
    Raised in an attempt to create a resource a URI that already exists and is
    not supposed to.

    This usually surfaces at the HTTP level as a 409.
    '''
    def __str__(self):
        return self.msg or 'Resource /{} already exists.'.format(self.uuid)



class ResourceNotExistsError(ResourceError):
    '''
    Raised in an attempt to create a resource a URN that does not exist and is
    supposed to.

    This usually surfaces at the HTTP level as a 404.
    '''
    def __str__(self):
        return self.msg or 'Resource /{} not found.'.format(self.uuid)



class InvalidResourceError(ResourceError):
    '''
    Raised when an invalid resource is found.

    This usually surfaces at the HTTP level as a 409 or other error.
    '''
    def __str__(self):
        return self.msg or 'Resource /{} is invalid.'.format(self.uuid)



class ServerManagedTermError(RuntimeError):
    '''
    Raised in an attempt to change a triple containing a server-managed term.

    This usually surfaces at the HTTP level as a 409 or other error.
    '''
    def __init__(self, terms, term_type):
        if term_type == 's':
            term_name = 'subject'
        elif term_type == 'p':
            term_name = 'predicate'
        elif term_type == 't':
            term_name = 'RDF type'
        else:
            term_name = 'term'

        self.terms = terms
        self.term_name = term_name

    def __str__(self):
        return 'Some {}s are server managed and cannot be modified: {}'\
                .format(self.term_name, ' , '.join(self.terms))



class InvalidTripleError(RuntimeError):
    '''
    Raised when a triple in a delta is not valid.

    This does not necessarily that it is not valid RDF, but rather that it may
    not be valid for the context it is meant to be utilized.
    '''
    def __init__(self, t):
        self.t = t

    def __str__(self):
        return '{} is not a valid triple.'.format(self.t)



class RefIntViolationError(RuntimeError):
    '''
    Raised when a provided data set has a link to a non-existing repository
    resource. With some setups this is handled silently, with a strict setting
    it raises this exception that should return a 412 HTTP code.
    '''
    def __init__(self, o):
        self.o = o

    def __str__(self):
        return 'Resource {} does not exist in repository. Linking to it '\
            'constitutes an integrity violation under the current setup.'\
            .format(self.o)



class SingleSubjectError(RuntimeError):
    '''
    Raised when a SPARQL-Update query or a RDF payload for a PUT contain
    subjects that do not correspond to the resource being operated on.
    '''
    def __init__(self, uuid, subject):
        self.uuid = uuid
        self.subject = subject

    def __str__(self):
        return '{} is not in the topic of this RDF, which is {}'.format(
                self.uuid, self.subject)


class TombstoneError(RuntimeError):
    '''
    Raised when a tombstone resource is found.

    It is up to the caller to handle this which may be a benign and expected
    result.
    '''
    def __init__(self, uuid, ts):
        self.uuid = uuid
        self.ts = ts

    def __str__(self):
        return 'Discovered tombstone resource at /{}, departed: {}'.format(
                self.uuid, self.ts)



