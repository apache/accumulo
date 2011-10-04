package org.apache.accumulo.server.test.randomwalk.bulk;

import java.util.Properties;

import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;


public abstract class BulkTest extends Test {

    @Override
    public void visit(final State state, Properties props) throws Exception {
        Setup.run(state, new Runnable() {
            @Override
            public void run() {
                try {
                    runLater(state);
                } catch (Throwable ex) {
                    log.error(ex, ex);
                }
            }
            
        });
    }

    abstract protected void runLater(State state) throws Exception;
    
}
