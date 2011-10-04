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


public class Compact extends Test {

	@Override
	public void visit(State state, Properties props) throws Exception {
		Connector conn = state.getConnector();
		
		Random rand = (Random) state.get("rand");
		
		@SuppressWarnings("unchecked")
		List<String> tableNames = (List<String>) state.get("tables");
		
		String tableName = tableNames.get(rand.nextInt(tableNames.size()));

		//TODO need to sometimes do null start and end ranges
		
		TreeSet<Text> range = new TreeSet<Text>();
		range.add(new Text(String.format("%016x", Math.abs(rand.nextLong()))));
		range.add(new Text(String.format("%016x", Math.abs(rand.nextLong()))));
		
		try{
			boolean wait = rand.nextBoolean();
			conn.tableOperations().compact(tableName, range.first(), range.last(), false, wait);
			log.debug((wait ? "compacted " : "initiated compaction ")+tableName);
		}catch(TableNotFoundException tne){
			log.debug("compact "+tableName+" failed, doesnt exist");
		}catch(TableOfflineException toe){
			log.debug("compact "+tableName+" failed, offline");
		}
		
	}
}

