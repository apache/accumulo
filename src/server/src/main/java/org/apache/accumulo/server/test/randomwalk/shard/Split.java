package org.apache.accumulo.server.test.randomwalk.shard;

import java.util.Properties;
import java.util.Random;
import java.util.SortedSet;

import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;
import org.apache.hadoop.io.Text;


public class Split extends Test {
	
    @Override
    public void visit(State state, Properties props) throws Exception {
        String indexTableName = (String)state.get("indexTableName");
        int numPartitions = (Integer)state.get("numPartitions");
        Random rand = (Random) state.get("rand");
        
        SortedSet<Text> splitSet = ShardFixture.genSplits(numPartitions, rand.nextInt(numPartitions)+1, "%06x");
        log.debug("adding splits " + indexTableName);
        state.getConnector().tableOperations().addSplits(indexTableName, splitSet);
    }

}
