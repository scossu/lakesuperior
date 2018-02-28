# Performance Benchmark Report

## Environment

### Hardware

- Dell Precison M3800 Laptop
- 8x Intel(R) Core(TM) i7-4712HQ CPU @ 2.30GHz
- 12Gb RAM
- SSD

### Software

- Arch Linux OS
- glibc 2.26-11
- python 3.5.4
- lmdb 0.9.21-1
- db (BerkeleyDB) 5.3.28-3

### Benchmark script

[Generator script](../../tests/10K_children.py)

The script was run with default values: 10,000 children under the same parent,
PUT requests.

### Data Set

Synthetic graph created by the benchmark script. The graph is unique for each
request and consists of 200 triples which are partly random data, with a
consistent size and variation:

- 50 triples have an object that is a URI of an external resource (50 unique
  predicates; 5 unique objects).
- 50 triples have an object that is a URI of a repository-managed resource
  (50 unique predicates; 5 unique objects).
- 100 triples have an object that is a 64-character random Unicode string
  (50 unique predicates; 100 unique objects).

## Results

### FCREPO/Modeshape 4.7.5

15'45" running time
0.094" per resource
3.4M triples total in repo at the end of the process

Retrieval of parent resource (~10000 triples), pipe to /dev/null: 3.64"

Peak memory usage: 2.47Gb
Database size: 3.3 Gb


### LAKEsuperior Alpha 6, LMDB Back End

25' running time
0.152" per resource

Some gaps every ~40-50 requests, probably disk flush

Retrieval of parent resource (10K triples), pipe to /dev/null: 2.13"

Peak memory usage: ~650 Mb (3 idle workers, 1 active)
Database size: 523 Mb

