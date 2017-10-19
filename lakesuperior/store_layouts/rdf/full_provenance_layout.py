import arrow

from uuid import uuid4

from rdflib import Dataset, Graph
from rdflib.namespace import FOAF, RDF, XSD
from rdflib.plugins.sparql import prepareQuery
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from rdflib.term import URIRef, Literal

from lakesuperior.core.namespaces import ns_collection as nsc
from lakesuperior.core.namespaces import ns_mgr as nsm
from lakesuperior.store_layouts.rdf.base_rdf_layout import BaseRdfLayout


class FullProvenanceLayout(BaseRdfLayout):
    '''This is an implementation of the
    [graph-per-resource pattern](http://patterns.dataincubator.org/book/graph-per-resource.html)
    which stores each LDP resource in a separate graph, with a "main" graph
    to keep track of resource metadata.
    '''

    DEFAULT_AGENT_URI = nsc['lake'].defaultAgent
    MAIN_GRAPH_URI = nsc['fcg'].meta


    ## MAGIC METHODS ##

    def __init__(self):
        self.main_graph = self.ds.graph(self.MAIN_GRAPH_URI)


    ## PUBLIC METHODS ##

    def ask_rsrc_exists(self, uuid):
        '''Return whether the resource exists.

        @param uuid Resource UUID.

        @retrn boolean
        '''
        res = self.ds.graph(self.UNION_GRAPH_URI).resource(nsc['fcres'][uuid])

        return len(res) > 0


    def get_rsrc(self, uuid):
        '''Get a resource graph.
        '''
        res = self.ds.graph(self.UNION_GRAPH_URI).query(
            'CONSTRUCT WHERE { ?s ?p ?o }',
            initBindings={'s' : nsc['fcres'][uuid]}
        )

        return self.globalize_graph(res.graph)


    def put_rsrc(self, uuid, data, format='text/turtle', base_types=None,
            agent=None):
        '''Create a resource graph.

        If the resource UUID exists already, it is either overwritten or a
        version snapshot is created, depending on the parameters.
        '''
        if agent is None:
            agent = self.DEFAULT_AGENT_URI

        res_urn = nsc['fcres'][uuid]

        # If there is a statement by this agent about this resource, replace
        # its contents.
        if self._get_res_stmt_by_agent(res_urn, agent):
            pass # @TODO


        # If the graph URI does not exist, create a new resource.
        else:
            # Create a new UUID for the statement set.
            stmset_uri = nsc['stmset'][str(uuid4())]

            # Create a temp graph to store the loaded data. For some reason,
            # loading directly into the stored graph throws an assertion error.
            tmp_g = Graph()
            tmp_g.parse(data=data.decode('utf-8'), format=format,
                    publicID=str(res_urn))

            # Create the graph and add the data.
            g = self.ds.graph(stmset_uri)
            g += tmp_g

            # Add metadata.
            ts = arrow.utcnow()
            main_graph = self.ds.graph(self.MAIN_GRAPH_URI)

            main_graph.add((stmset_uri, FOAF.primaryTopic, res_urn))
            main_graph.add((stmset_uri, RDF.type, nsc['prov'].Entity))
            main_graph.add(
                    (stmset_uri, nsc['prov'].generatedAtTime,
                    Literal(ts, datatype=XSD.dateTime)))
            main_graph.add(
                    (stmset_uri, nsc['prov'].wasAttributedTo, agent))


        #self.create_version(res_urn)

        if base_types:
            for type_uri in self.base_types:
                main_graph.add((stmset_uri, RDF.type, type_uri))

        # @TODO Create containment triples

        self.conn.store.commit()



    #def create_version(self, res_urn):
    #    '''Swap out previous version if existing, and create new version
    #    dependency.'''
    #    main_graph = ds.graph(URIRef('urn:lake:' + self.MAIN_GRAPH_NAME))
    #    prv_res_urn = self.select_current_graph_for_res(res_urn)

    #    if prv_res_urn:
    #        main_graph.remove((prv_res_urn, RDF.type, nsc['lake'].Resource))
    #        main_graph.add((prv_res_urn, RDF.type, nsc['lake'].Snapshot))

    #        main_graph.add((res_urn, RDF.type, nsc['lake'].Resource))
    #        main_graph.add((res_urn, nsc['lake'].previousVersion, prv_res_urn))


    #def select_current_graph_for_res(self, urn):
    #    '''Select the current graph URI for a given resource.'''
    #    qry = '''
    #    SELECT ?g {
    #      GRAPH ?mg { ?g a ?gt . }
    #      GRAPH ?g { ?s ?p ?o . }
    #    }
    #    LIMIT 1
    #    '''
    #    rsp = self.ds.query(qry, initBindings={
    #        'mg' : URIRef('urn:lake:' + self.MAIN_GRAPH_NAME),
    #        'gt' : RESOURCE_TYPE_URI,
    #        's' : urn
    #    })

    #    return list(rsp[0][0])


    def _ask_res_stmt_by_agent_exists(self, res_urn, agent):
        '''Ask if any statements have been made by a certain agent about a
        certain resource.

        @param rdflib.term.URIRef res_urn Resource URN.
        @param rdflib.term.URIRef agent Agent URI.

        @return boolean
        '''
        return self.query('''
        ASK {
          GRAPH ?mg {
              ?g prov:wasAttributedTo ?a .
          }
          GRAPH ?g {
              ?s ?p ?o .
          }
        }
        ''', initBindings={
            'a' : agent,
            's' : res_urn,
        })

