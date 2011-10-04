package org.apache.accumulo.server.test.randomwalk.multitable;

import java.net.InetAddress;
import java.util.ArrayList;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.server.test.randomwalk.Fixture;
import org.apache.accumulo.server.test.randomwalk.State;


public class MultiTableFixture extends Fixture {
	

	@Override
	public void setUp(State state) throws Exception {
		
		String hostname = InetAddress.getLocalHost().getHostName().replaceAll("[-.]", "_");
		
		state.set("tableNamePrefix", String.format("multi_%s_%s_%d", hostname, state.getPid(), System.currentTimeMillis()));
		state.set("nextId", new Integer(0));
		state.set("numWrites", new Integer(0));
		state.set("totalWrites", new Integer(0));
		state.set("tableList", new ArrayList<String>());
	}

	@Override
	public void tearDown(State state) throws Exception {
		
		Connector conn = state.getConnector();
		
		@SuppressWarnings("unchecked")
		ArrayList<String> tables = (ArrayList<String>) state.get("tableList");
		
		for (String tableName : tables) {
			try {
				conn.tableOperations().delete(tableName);
				log.debug("Dropping table "+tableName);
			} catch (TableNotFoundException e) {
				log.warn("Tried to drop table "+tableName+" but could not be found!");
			}
		}
	}
}
