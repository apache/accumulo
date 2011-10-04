package org.apache.accumulo.server.test.randomwalk.shard;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;


public class CloneIndex extends Test {

	@Override
	public void visit(State state, Properties props) throws Exception {
		
		String indexTableName = (String)state.get("indexTableName");
		String tmpIndexTableName = indexTableName+"_tmp";
		
		long t1 = System.currentTimeMillis();
		state.getConnector().tableOperations().flush(indexTableName, null, null, true);
		long t2 = System.currentTimeMillis();
		state.getConnector().tableOperations().clone(indexTableName, tmpIndexTableName, false, new HashMap<String, String>(), new HashSet<String>());
		long t3 = System.currentTimeMillis();
		
		log.debug("Cloned "+tmpIndexTableName+" from "+indexTableName+" flush: "+(t2-t1)+"ms clone: "+(t3-t2)+"ms");
		
	}
	
}
