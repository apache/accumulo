package org.apache.accumulo.server.test.randomwalk.concurrent;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;



public class BatchScan extends Test {

	@Override
	public void visit(State state, Properties props) throws Exception {
		Connector conn = state.getConnector();
		
		Random rand = (Random) state.get("rand");
		
		@SuppressWarnings("unchecked")
		List<String> tableNames = (List<String>) state.get("tables");
		
		String tableName = tableNames.get(rand.nextInt(tableNames.size()));
		
		try {
			BatchScanner bs = conn.createBatchScanner(tableName, Constants.NO_AUTHS, 3);
			List<Range> ranges = new ArrayList<Range>();
			for(int i = 0; i < rand.nextInt(2000)+1; i++)
				ranges.add(new Range(String.format("%016x", Math.abs(rand.nextLong()))));
			
			bs.setRanges(ranges);
			
			try{
				Iterator<Entry<Key, Value>> iter = bs.iterator();
				while (iter.hasNext())
					iter.next();
			}finally{
				bs.close();
			}
			
			log.debug("Wrote to "+tableName);
		} catch (TableNotFoundException e) {
			log.debug("BatchScan "+tableName+" failed, doesnt exist");
		} catch (TableDeletedException tde){
			log.debug("BatchScan "+tableName+" failed, table deleted");
		} catch (TableOfflineException e){
			log.debug("BatchScan "+tableName+" failed, offline");
		}
	}
}

