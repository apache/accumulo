package org.apache.accumulo.server.test.randomwalk.bulk;

import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.hadoop.io.Text;


public class Compact extends BulkTest {

    @Override
    protected void runLater(State state) throws Exception {
        Text[] points = Merge.getRandomTabletRange(state);
        log.info("Compacting " + Merge.rangeToString(points));
        state.getConnector().tableOperations().compact(Setup.getTableName(), points[0], points[1], false, true);
    }

}
