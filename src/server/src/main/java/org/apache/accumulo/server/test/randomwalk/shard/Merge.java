package org.apache.accumulo.server.test.randomwalk.shard;

import java.util.Collection;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;
import org.apache.hadoop.io.Text;


public class Merge extends Test {

    @Override
    public void visit(State state, Properties props) throws Exception {
        String indexTableName = (String)state.get("indexTableName");
        
        Collection<Text> splits = state.getConnector().tableOperations().getSplits(indexTableName);
        SortedSet<Text> splitSet = new TreeSet<Text>(splits);
        log.debug("merging " + indexTableName);
        state.getConnector().tableOperations().merge(indexTableName, null, null);
        org.apache.accumulo.core.util.Merge merge = new org.apache.accumulo.core.util.Merge();
        merge.mergomatic(state.getConnector(), indexTableName, null, null, 256 * 1024 * 1024, true);
        splits = state.getConnector().tableOperations().getSplits(indexTableName);
        if (splits.size() > splitSet.size())
        {
        	//throw an excpetion so that test will die an no further changes to table will occur... 
        	//this way table is left as is for debugging.
            throw new Exception("There are more tablets after a merge: " + splits.size() + " was " + splitSet.size());
        }
    }

}
