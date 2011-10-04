package org.apache.accumulo.server.test.randomwalk.sequential;

import java.util.Properties;

import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;


public class Commit extends Test {

	@Override
	public void visit(State state, Properties props) throws Exception {
		
		state.getMultiTableBatchWriter().flush();
		
		log.debug("Committed "+state.getInteger("numWrites")+" writes.  Total writes: "+state.getInteger("totalWrites"));
		state.set("numWrites", new Integer(0));
	}

}
