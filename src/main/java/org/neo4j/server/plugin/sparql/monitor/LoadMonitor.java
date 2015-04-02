package org.neo4j.server.plugin.sparql.monitor;

import org.openrdf.model.Statement;
import org.openrdf.rio.helpers.RDFHandlerBase;
import scala.Function1;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Alexander De Leon <me@alexdeleon.name>
 */
public class LoadMonitor extends RDFHandlerBase {

    private AtomicLong counter = new AtomicLong(0);
    private long startTime = 0;
    private long lastCheck  = 0;
    private double throughput = 0;
    private Function1<Double, Void> listener;


    public LoadMonitor(Function1<Double, Void> listener){
        this.listener = listener;
    }

    @Override
    public void startRDF() {
        startTime  = System.currentTimeMillis();
        lastCheck = startTime;
    }

    @Override
    public void handleStatement(Statement st) {
        counter.incrementAndGet();
        long t = System.currentTimeMillis();
        // measure throughput every 2sec
        if (t - lastCheck > 2000) {
            record (counter.get() / ((t - startTime) / 1000));
            lastCheck = t;
        }
    }

    private void record(double value) {
        throughput = value;
        listener.apply(value);
    }
}
