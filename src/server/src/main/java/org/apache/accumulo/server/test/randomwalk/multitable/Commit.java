package org.apache.accumulo.server.test.randomwalk.multitable;

import java.util.Properties;

import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;


public class Commit extends Test {

	@Override
	public void visit(State state, Properties props) throws Exception {
		state.getMultiTableBatchWriter().flush();
		
		Integer numWrites = state.getInteger("numWrites");
		Integer totalWrites = state.getInteger("totalWrites") + numWrites;
		
		log.debug("Committed "+numWrites+" writes.  Total writes: "+totalWrites);
		
		state.set("totalWrites", totalWrites);
		state.set("numWrites", new Integer(0));
	}

}
