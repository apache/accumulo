package org.apache.accumulo.server.test.randomwalk.shard;

import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;


public class Flush extends Test{

	@Override
	public void visit(State state, Properties props) throws Exception {
		String indexTableName = (String)state.get("indexTableName");
		String dataTableName = (String)state.get("docTableName");
		Random rand = (Random) state.get("rand");
		
		String table;
		
		if(rand.nextDouble() < .5)
			table = indexTableName;
		else
			table = dataTableName;
		
		state.getConnector().tableOperations().flush(table, null, null, true);
		log.debug("Flushed "+table);
	}

}
