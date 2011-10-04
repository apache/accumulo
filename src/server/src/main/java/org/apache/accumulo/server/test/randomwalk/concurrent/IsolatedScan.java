package org.apache.accumulo.server.test.randomwalk.concurrent;

import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;



public class IsolatedScan extends Test {

	@Override
	public void visit(State state, Properties props) throws Exception {
		Connector conn = state.getConnector();
		
		Random rand = (Random) state.get("rand");
		
		@SuppressWarnings("unchecked")
		List<String> tableNames = (List<String>) state.get("tables");
		
		String tableName = tableNames.get(rand.nextInt(tableNames.size()));
		
		try {
			RowIterator iter = new RowIterator(new IsolatedScanner(conn.createScanner(tableName, Constants.NO_AUTHS)));
			
			while(iter.hasNext()){
				List<Entry<Key, Value>> row = iter.next();
				for(int i=1; i < row.size(); i++)
					if(!row.get(0).getValue().equals(row.get(i).getValue()))
						throw new Exception("values not equal "+row.get(0)+" "+row.get(i));
			}
			log.debug("Isolated scan "+tableName);
		} catch (TableDeletedException e){
			log.debug("Isolated scan "+tableName+" failed, table deleted");
		} catch (TableNotFoundException e) {
			log.debug("Isolated scan "+tableName+" failed, doesnt exist");
		} catch (TableOfflineException e){
			log.debug("Isolated scan "+tableName+" failed, offline");
		}
	}
}

