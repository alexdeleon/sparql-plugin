package org.neo4j.server.plugin.sparql.api;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilders;
import org.neo4j.test.server.HTTP;
import sun.nio.ch.IOUtil;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.InputStream;

public class RDFApiTest {

    /**
     * This endpoint enables the load of an entire RDF graph.
     */
    @Test
    public void loadRdf() throws IOException {
        String payload = berlin100();

        // Given
        try ( ServerControls server = TestServerBuilders.newInProcessBuilder()
                .withExtension( "/sparqlPlugin", RDFApi.class )
                .newServer() )
        {
            // When
            HTTP.Response response = HTTP
                    .withHeaders(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_PLAIN)
                    .POST(server.httpURI().resolve("sparqlPlugin/rdf").toString(), HTTP.RawPayload.rawPayload(payload));

            // Then
            Assert.assertEquals(200, response.status());
        }
    }

    private String testTriples(){
        return "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/ProductType> .\n" +
                "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType1> <http://www.w3.org/2000/01/rdf-schema#label> \"Thing\" .";
    }

    private String berlin100() throws IOException {
        InputStream input = getClass().getResourceAsStream("/berlin_nt_100.nt");
        StringBuffer buffer = new StringBuffer();
        for(String line: IOUtils.readLines(input)){
            buffer.append(line).append("\n");
        }
        return buffer.toString();
    }

}