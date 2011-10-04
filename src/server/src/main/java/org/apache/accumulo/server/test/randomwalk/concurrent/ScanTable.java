package org.apache.accumulo.server.test.randomwalk.concurrent;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;



public class ScanTable extends Test {

	@Override
	public void visit(State state, Properties props) throws Exception {
		Connector conn = state.getConnector();
		
		Random rand = (Random) state.get("rand");
		
		@SuppressWarnings("unchecked")
		List<String> tableNames = (List<String>) state.get("tables");
		
		String tableName = tableNames.get(rand.nextInt(tableNames.size()));
		
		try {
			Scanner scanner = conn.createScanner(tableName, Constants.NO_AUTHS);
			Iterator<Entry<Key, Value>> iter = scanner.iterator();
			while(iter.hasNext()){
				iter.next();
			}
			log.debug("Scanned "+tableName);
		} catch (TableDeletedException e){
			log.debug("Scan "+tableName+" failed, table deleted");
		} catch (TableNotFoundException e) {
			log.debug("Scan "+tableName+" failed, doesnt exist");
		} catch (TableOfflineException e){
			log.debug("Scan "+tableName+" failed, offline");
		}
	}
}
