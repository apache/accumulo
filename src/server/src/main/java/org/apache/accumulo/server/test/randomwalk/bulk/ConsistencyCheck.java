package org.apache.accumulo.server.test.randomwalk.bulk;

import java.util.Map.Entry;
import java.util.Random;

import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.hadoop.io.Text;


public class ConsistencyCheck extends BulkTest {

    @Override
    protected void runLater(State state) throws Exception {
        Random rand = (Random)state.get("rand");
        Text row = Merge.getRandomRow(rand);
        log.info("Checking " + row);
        String user = state.getConnector().whoami();
        Authorizations auths = state.getConnector().securityOperations().getUserAuthorizations(user);
        Scanner scanner = state.getConnector().createScanner(Setup.getTableName(), auths);
        scanner = new IsolatedScanner(scanner);
        scanner.setRange(new Range(row));
        Value v = null;
        Key first = null;
        for (Entry<Key, Value> entry : scanner) {
            if (v == null) {
                v = entry.getValue();
                first = entry.getKey();
            }
            if (!v.equals(entry.getValue()))
                throw new RuntimeException("Inconsistent value at " + entry.getKey() + " was " + entry.getValue() + " should be " + v + " first read at " + first);
        }
    }

}
