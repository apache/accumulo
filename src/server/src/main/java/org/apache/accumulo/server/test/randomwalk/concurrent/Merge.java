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


public class Merge extends Test {

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
			conn.tableOperations().merge(tableName, range.first(), range.last());
			log.debug("merged "+tableName);
		}catch(TableOfflineException toe){
            log.debug("merge "+tableName+" failed, table is not online");
        }catch(TableNotFoundException tne){
            log.debug("merge "+tableName+" failed, doesnt exist");
        }
        
	}
}
