package org.apache.accumulo.server.test.randomwalk.multitable;

import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;


public class OfflineTable extends Test {

	@Override
	public void visit(State state, Properties props) throws Exception {
		
		@SuppressWarnings("unchecked")
		ArrayList<String> tables = (ArrayList<String>) state.get("tableList");
	
		if (tables.size() <= 0) {
			return;
		}
		
		Random rand = new Random();
		String tableName = tables.get(rand.nextInt(tables.size()));
		
		state.getConnector().tableOperations().offline(tableName);
		log.debug("Table "+tableName+" offline ");
		state.getConnector().tableOperations().online(tableName);
		log.debug("Table "+tableName+" online ");
	}
}
