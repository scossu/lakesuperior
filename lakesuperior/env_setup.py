from lakesuperior import env

__doc__="""
Default configuration.

Import this module to initialize the configuration for a production setup::

    >>> import lakesuperior.env_setup

Will load the default configuration.

**Note:** this will be deprecated because it's just as easy to do the same with

::
    >>> import env # which in most cases is imported anyways
    >>> env.setup()
"""

env.setup()
