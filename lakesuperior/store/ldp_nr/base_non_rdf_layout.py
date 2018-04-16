import logging

from abc import ABCMeta, abstractmethod


logger = logging.getLogger(__name__)


class BaseNonRdfLayout(metaclass=ABCMeta):
    """
    Abstract class for setting the non-RDF (bitstream) store layout.

    Differerent layouts can be created by implementing all the abstract methods
    of this class. A non-RDF layout is not necessarily restricted to a
    traditional filesystem—e.g. a layout persisting to HDFS can be written too.
    """

    def __init__(self, config):
        """
        Initialize the base non-RDF store layout.
        """
        self.config = config
        self.root = config['location']


    ## INTERFACE METHODS ##

    @abstractmethod
    def persist(self, stream):
        """Store the stream in the designated persistence layer."""
        pass


    @abstractmethod
    def delete(self, id):
        """Delete a stream by its identifier (i.e. checksum)."""
        pass


    @abstractmethod
    def local_path(self, uuid):
        """Return the local path of a file."""
        pass
