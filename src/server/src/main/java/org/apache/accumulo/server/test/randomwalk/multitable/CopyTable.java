package org.apache.accumulo.server.test.randomwalk.multitable;

import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;
import org.apache.hadoop.util.ToolRunner;


public class CopyTable extends Test {

	@Override
	public void visit(State state, Properties props) throws Exception {
		
		@SuppressWarnings("unchecked")
		ArrayList<String> tables = (ArrayList<String>) state.get("tableList");
		if (tables.isEmpty())
			return;
		
		Random rand = new Random();
		String srcTableName = tables.remove(rand.nextInt(tables.size()));

		int nextId = ((Integer) state.get("nextId")).intValue();
		String dstTableName = String.format("%s_%d", state.getString("tableNamePrefix"), nextId);
		
		String[] args = new String[8];
		args[0] = "-libjars";
		args[1] = state.getMapReduceJars();
		args[2] = state.getProperty("USERNAME");
		args[3] = state.getProperty("PASSWORD");
		args[4] = srcTableName;
		args[5] = state.getInstance().getInstanceName();
		args[6] = state.getProperty("ZOOKEEPERS");
		args[7] = dstTableName;
		
		log.debug("copying "+srcTableName+" to "+dstTableName);
		
		state.getConnector().tableOperations().create(dstTableName);
		
		if (ToolRunner.run(CachedConfiguration.getInstance(), new CopyTool(), args) != 0) {
			log.error("Failed to run map/red verify");
			return;
		}

		String tableId = Tables.getNameToIdMap(state.getInstance()).get(dstTableName);
		log.debug("copied "+srcTableName+" to "+dstTableName+" (id - "+tableId+" )");

		tables.add(dstTableName);
		
		state.getConnector().tableOperations().delete(srcTableName);
		log.debug("dropped " + srcTableName);

		nextId++;
		state.set("nextId", new Integer(nextId));
	}
}
