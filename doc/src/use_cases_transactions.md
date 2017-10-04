# Use cases that may involve a transaction

## Add a LDP-RS

1. Add a named graph with some triples
2. Add metadata about the graph

## Update an LDP-RS

1. Query current resource (named graph)
2. Apply SPARQL-UPDATE to in-memory graph
3. Add new named graph with new dataset
4. Modify (insert and delete triples) metadata in main graph

## (Soft-)Delete a resource

1. Mark resource as deleted in the main graph (set type to tombstone)
