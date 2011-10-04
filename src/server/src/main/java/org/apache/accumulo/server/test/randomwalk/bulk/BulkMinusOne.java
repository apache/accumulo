package org.apache.accumulo.server.test.randomwalk.bulk;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.server.test.randomwalk.State;


public class BulkMinusOne extends BulkTest {

    private static final Value negOne = new Value("-1".getBytes());
    
    @Override
    protected void runLater(State state) throws Exception {
        log.info("Decrementing");
        BulkPlusOne.bulkLoadLots(log, state, negOne);
    }

}
