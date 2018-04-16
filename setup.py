"""
LAKEsuperior setup script.

Proudly ripped from https://github.com/pypa/sampleproject/blob/master/setup.py
"""

import sys

# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from glob import glob
from os import path

here = path.abspath(path.dirname(__file__))

# ``pytest_runner`` is referenced in ``setup_requires``.
# See https://github.com/pytest-dev/pytest-runner#conditional-requirement
needs_pytest = {'pytest', 'test', 'ptr'}.intersection(sys.argv)
pytest_runner = ['pytest-runner'] if needs_pytest else []


# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

# Read release number.
with open(path.realpath(path.join(here, 'VERSION'))) as fh:
    version = fh.readlines()[0]


setup(
    name='lakesuperior',
    version=version,

    description='A Linked Data Platform repository sever.',
    long_description=long_description,
    long_description_content_type='text/x-rst; charset=UTF-8',

    url='https://lakesuperior.readthedocs.io',

    author='Stefano Cossu <@scossu>',
    #author_email='',  # Optional
    license='Apache License Version 2.0',

    # https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 3 - Alpha',

        'Environment :: Console',
        'Environment :: Web Environment',

        'Framework :: Flask',

        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Science/Research',

        'License :: OSI Approved :: Apache Software License',

        'Natural Language :: English',

        'Operating System :: MacOS',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX :: Linux',

        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',

        'Topic :: Database :: Database Engines/Servers',
    ],

    keywords='repository linked-data',

    python_requires='~=3.5',

    packages=find_packages(exclude=['contrib', 'docs', 'tests']),

    # Great reference read about dependency management:
    # https://caremad.io/posts/2013/07/setup-vs-requirement/
    install_requires=[
        'CoilMQ',
        'Flask',
        'HiYaPyCo',
        'PyYAML',
        'arrow',
        'cchardet',
        'click',
        'click-log',
        'gevent',
        'gunicorn',
        'lmdb',
        'rdflib',
        'requests',
        'requests-toolbelt',
        'sphinx-rtd-theme',
        'stomp.py',
    ],

    setup_requires=[] + pytest_runner,
    tests_require=[
        'Pillow',
        'numpy',
        'pytest',
        'pytest-flask',
    ],

    include_package_data=True,
    #extras_require={},
    #package_data={
    #},
    #data_files=[],

    entry_points={
        'console_scripts': [
            'fcrepo=lakesuperior.wsgi:run',
            'lsup-admin=lakesuperior.lsup_admin:admin',
            'lsup-benchmark=lakesuperior.util.benchmark:run',
            'profiler=lakesuperior.profiler:run',
        ],
    },

    # List additional URLs that are relevant to your project as a dict.
    #
    # This field corresponds to the "Project-URL" metadata fields:
    # https://packaging.python.org/specifications/core-metadata/#project-url-multiple-use
    #
    # Examples listed include a pattern for specifying where the package tracks
    # issues, where the source is hosted, where to say thanks to the package
    # maintainers, and where to support the project financially. The key is
    # what's used to render the link text on PyPI.
    project_urls={  # Optional
        'Source Code': 'https://github.com/scossu/lakesuperior/',
        'Documentation': 'https://lakesuperior.readthedocs.io',
        'Discussion': 'https://groups.google.com/forum/#!forum/lakesuperior',
        'Bug Reports': 'https://github.com/scossu/lakesuperior/issues',
    }
)

