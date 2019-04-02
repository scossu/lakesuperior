import logging
import os

from abc import ABCMeta, abstractmethod

from lakesuperior.util.toolbox import get_tree_size

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


    @property
    def store_size(self):
        """Calculated the store size on disk."""
        return get_tree_size(self.root)


    @property
    def file_ct(self):
        """Calculated the store size on disk."""
        return sum([len(files) for r, d, files in os.walk(self.root)])

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
