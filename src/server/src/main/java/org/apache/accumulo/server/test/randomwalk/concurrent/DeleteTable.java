package org.apache.accumulo.server.test.randomwalk.concurrent;

import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;


public class DeleteTable extends Test {

	@Override
	public void visit(State state, Properties props) throws Exception {
		Connector conn = state.getConnector();
		
		Random rand = (Random) state.get("rand");
		
		@SuppressWarnings("unchecked")
		List<String> tableNames = (List<String>) state.get("tables");
		
		String tableName = tableNames.get(rand.nextInt(tableNames.size()));
		
		try {
			conn.tableOperations().delete(tableName);
			log.debug("Deleted table "+tableName);
		} catch (TableNotFoundException e) {
			log.debug("Delete "+tableName+" failed, doesnt exist");
		}
	}
}
