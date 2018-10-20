"""
Lakesuperior setup script.

Proudly ripped from https://github.com/pypa/sampleproject/blob/master/setup.py
"""

import sys

# Always prefer setuptools over distutils
from setuptools import Extension, setup, find_packages
# To use a consistent encoding
from codecs import open
from glob import glob
from os import path

import lakesuperior

# Use this version to build C files from .pyx sources.
CYTHON_VERSION='0.29'

try:
    import Cython
    from Cython.Build import cythonize
except ImportError:
    USE_CYTHON = False
else:
    if Cython.__version__ == CYTHON_VERSION:
        USE_CYTHON = True


# ``pytest_runner`` is referenced in ``setup_requires``.
# See https://github.com/pytest-dev/pytest-runner#conditional-requirement
needs_pytest = {'pytest', 'test', 'ptr'}.intersection(sys.argv)
pytest_runner = ['pytest-runner'] if needs_pytest else []


# Get the long description from the README file
readme_fpath = path.join(path.dirname(lakesuperior.basedir), 'README.rst')
with open(readme_fpath, encoding='utf-8') as f:
    long_description = f.read()

# Extensions directory.
lmdb_src_dir = path.join('ext', 'lmdb', 'libraries', 'liblmdb')
tpl_src_dir = path.join('ext', 'tpl', 'src')

include_dirs = [lmdb_src_dir, tpl_src_dir]

if USE_CYTHON:
    print(f'Using Cython {CYTHON_VERSION} to generate C extensions.')
    include_dirs.append(path.join(lakesuperior.basedir, 'cy_include'))
    ext = 'pyx'
else:
    print(f'Cython {CYTHON_VERSION} not found. Using provided C extensions.')
    ext = 'c'

extensions = [
    Extension(
        'lakesuperior.store.base_lmdb_store',
        [
            path.join(lmdb_src_dir, 'mdb.c'),
            path.join(lmdb_src_dir, 'midl.c'),
            path.join('lakesuperior', 'store', f'base_lmdb_store.{ext}'),
        ],
        include_dirs=include_dirs,
    ),
    Extension(
        'lakesuperior.store.ldp_rs.term',
        [
            path.join(tpl_src_dir, 'tpl.c'),
            path.join('lakesuperior', 'store', 'ldp_rs', f'term.{ext}'),
        ],
        include_dirs=include_dirs,
        extra_compile_args=['-fopenmp'],
        extra_link_args=['-fopenmp'],
        libraries=['crypto']
    ),
    Extension(
        'lakesuperior.store.ldp_rs.lmdb_triplestore',
        [
            path.join(lmdb_src_dir, 'mdb.c'),
            path.join(lmdb_src_dir, 'midl.c'),
            path.join(
                'lakesuperior', 'store', 'ldp_rs', f'lmdb_triplestore.{ext}'),
        ],
        include_dirs=include_dirs,
        extra_compile_args=['-fopenmp'],
        extra_link_args=['-fopenmp'],
        libraries=['crypto']
    ),
    # For testing.
    #Extension(
    #    '*',
    #    [
    #        #path.join(tpl_src_dir, 'tpl.c'),
    #        path.join(
    #            path.dirname(lakesuperior.basedir), 'sandbox', f'*.{ext}'),
    #    ],
    #    include_dirs=include_dirs,
    #),
]

if USE_CYTHON:
    extensions = cythonize(extensions, compiler_directives={
        'language_level': 3,
        'boundscheck': False,
        'wraparound': False,
        'profile': True,
    })


setup(
    name='lakesuperior',
    version=lakesuperior.release,

    description='A Linked Data Platform repository sever.',
    long_description=long_description,

    url='https://lakesuperior.readthedocs.io',

    author='Stefano Cossu <@scossu>',
    #author_email='',
    license='Apache License Version 2.0',

    ext_modules=extensions,

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
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',

        'Topic :: Database :: Database Engines/Servers',
    ],

    keywords='repository linked-data',

    python_requires='~=3.6',

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
        'rdflib',
        'rdflib-jsonld',
        'requests',
        'requests-toolbelt',
        'sphinx-rtd-theme',
        'stomp.py',
    ],

    setup_requires=[
        'setuptools>=18.0',
    ] + pytest_runner,
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
            'lsup-admin=lakesuperior.lsup_admin:admin',
            'lsup-benchmark=lakesuperior.util.benchmark:run',
            'lsup-profiler=lakesuperior.profiler:run',
            'lsup-server=lakesuperior.server:run',
        ],
    },

    scripts=['bin/fcrepo'],

    project_urls={
        'Source Code': 'https://github.com/scossu/lakesuperior/',
        'Documentation': 'https://lakesuperior.readthedocs.io',
        'Discussion': 'https://groups.google.com/forum/#!forum/lakesuperior',
        'Bug Reports': 'https://github.com/scossu/lakesuperior/issues',
    }
)

