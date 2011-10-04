package org.apache.accumulo.server.test.randomwalk.concurrent;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;


public class CloneTable extends Test {

	@Override
	public void visit(State state, Properties props) throws Exception {
		Connector conn = state.getConnector();
		
		Random rand = (Random) state.get("rand");
		
		@SuppressWarnings("unchecked")
		List<String> tableNames = (List<String>) state.get("tables");
		
		String srcTableName = tableNames.get(rand.nextInt(tableNames.size()));
		String newTableName = tableNames.get(rand.nextInt(tableNames.size()));
		boolean flush = rand.nextBoolean();
		
		try {	
			conn.tableOperations().clone(srcTableName, newTableName, flush, new HashMap<String, String>(), new HashSet<String>());
			log.debug("Cloned table "+srcTableName+" "+newTableName+" "+flush);
		} catch (TableExistsException e) {
			log.debug("Clone "+srcTableName+" failed, "+newTableName+" exist");
		}catch (TableNotFoundException e) {
			log.debug("Clone "+srcTableName+" failed, doesnt exist");
		}
	}
}

