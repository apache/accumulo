package org.apache.accumulo.server.test.randomwalk.sequential;

import java.net.InetAddress;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.server.test.randomwalk.Fixture;
import org.apache.accumulo.server.test.randomwalk.State;


public class SequentialFixture extends Fixture {
	
	String seqTableName;

	@Override
	public void setUp(State state) throws Exception {
				
		Connector conn = state.getConnector();
		Instance instance = state.getInstance();
		
		String hostname = InetAddress.getLocalHost().getHostName().replaceAll("[-.]", "_");
		
		seqTableName = String.format("sequential_%s_%s_%d", hostname, state.getPid(), System.currentTimeMillis());
		state.set("seqTableName", seqTableName);
		
		try {
			conn.tableOperations().create(seqTableName);
			log.debug("Created table "+seqTableName+" (id:"+Tables.getNameToIdMap(instance).get(seqTableName)+")");
		} catch (TableExistsException e) {
			log.warn("Table "+seqTableName+" already exists!");
			throw e;
		}
		conn.tableOperations().setProperty(seqTableName, "table.scan.max.memory", "1K");
		
		state.set("numWrites", new Integer(0));
		state.set("totalWrites", new Integer(0));
	}

	@Override
	public void tearDown(State state) throws Exception {
		
		log.debug("Dropping tables: "+seqTableName);

		Connector conn = state.getConnector();
		
		conn.tableOperations().delete(seqTableName);
	}
}
