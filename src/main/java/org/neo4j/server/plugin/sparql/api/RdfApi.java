package org.neo4j.server.plugin.sparql.api;

import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.impls.neo4j2.Neo4j2Graph;
import com.tinkerpop.blueprints.oupls.sail.GraphSail;
import org.neo4j.graphdb.GraphDatabaseService;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.repository.util.RDFInserter;
import org.openrdf.repository.util.RDFLoader;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author Alexander De Leon <me@alexdeleon.name>
 */
@Path("/rdf")
public class RDFApi {

    private static final Logger LOG = LoggerFactory.getLogger(RDFApi.class);

    private GraphSail sail;
    private SPARQLParser parser;
    private SailRepositoryConnection sc;
    private Neo4j2Graph neo4jGraph;

    public RDFApi(@Context GraphDatabaseService database){
        initSail(database);
    }

    @POST
    @Consumes( MediaType.WILDCARD )
    @Produces( MediaType.APPLICATION_JSON )
    public Response loadRdf(
            @HeaderParam(HttpHeaders.CONTENT_TYPE) String contentType,
            @QueryParam("context") final String context,
            InputStream rdfStream) throws RDFParseException, IOException, RDFHandlerException, RepositoryException {
        LOG.info("Loading RDF");
        ValueFactory vf = sail.getValueFactory();
        try {
            RDFInserter rdfInserter = new RDFInserter(sc);
            if(context != null) {
                rdfInserter.enforceContext(vf.createURI(context));
            }
            RDFLoader loader = new RDFLoader(new ParserConfig(), vf);
            String baseUri = context == null?"":context;
            loader.load(rdfStream, baseUri, RDFFormat.forMIMEType(contentType, RDFFormat.NTRIPLES), rdfInserter);
            sc.commit();
        }
        finally {
            LOG.info("Ended data loading for context"+context);
        }
        return Response.ok().build();

    }

    private void initSail(GraphDatabaseService neo4j) {
        if (sail == null) {
            neo4jGraph = new Neo4j2Graph(neo4j);
            sail = new GraphSail<KeyIndexableGraph>(neo4jGraph);
            try {
                sail.initialize();
                sc = new SailRepository(sail).getConnection();
                parser = new SPARQLParser();
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }
}
