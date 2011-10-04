package org.apache.accumulo.server.test.randomwalk.concurrent;

import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;
import org.apache.hadoop.io.Text;


public class AddSplits extends Test {

	@Override
	public void visit(State state, Properties props) throws Exception {
		Connector conn = state.getConnector();
		
		Random rand = (Random) state.get("rand");
		
		@SuppressWarnings("unchecked")
		List<String> tableNames = (List<String>) state.get("tables");
		
		String tableName = tableNames.get(rand.nextInt(tableNames.size()));
		
		TreeSet<Text> splits = new TreeSet<Text>();
		
		for(int i = 0; i < rand.nextInt(10)+1; i++)
			splits.add(new Text(String.format("%016x", Math.abs(rand.nextLong()))));
		
		try {
			conn.tableOperations().addSplits(tableName, splits);
			log.debug("Added "+splits.size()+" splits "+tableName);
		} catch (TableNotFoundException e) {
			log.debug("AddSplits "+tableName+" failed, doesnt exist");
		} catch (TableOfflineException e) {
			log.debug("AddSplits "+tableName+" failed, offline");
		}
	}
}
