package org.apache.accumulo.server.test.randomwalk.bulk;

import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.hadoop.io.Text;


public class Split extends BulkTest {

    @Override
    protected void runLater(State state) throws Exception {
        SortedSet<Text> splits = new TreeSet<Text>();
        Random rand = (Random)state.get("rand");
        int count = rand.nextInt(20);
        for (int i = 0; i < count; i++)
            splits.add(new Text(String.format(BulkPlusOne.FMT, Math.abs(rand.nextLong()) % BulkPlusOne.LOTS)));
        log.info("splitting " + splits);
        state.getConnector().tableOperations().addSplits(Setup.getTableName(), splits);        
    }
    
}
