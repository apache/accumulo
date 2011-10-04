package org.apache.accumulo.server.test.randomwalk.multitable;

import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;


public class DropTable extends Test {

	@Override
	public void visit(State state, Properties props) throws Exception {
		
		@SuppressWarnings("unchecked")
		ArrayList<String> tables = (ArrayList<String>) state.get("tableList");
		
		// don't drop a table if we only have one table or less
		if (tables.size() <= 1) {
			return;
		}
		
		Random rand = new Random();
		String tableName = tables.remove(rand.nextInt(tables.size()));
		
		try {
			state.getConnector().tableOperations().delete(tableName);
			log.debug("Dropped " + tableName);
		} catch (TableNotFoundException e) {
			log.error("Tried to drop table "+tableName+" but could not be found!");
		}
	}
}
