package org.apache.accumulo.server.test.functional;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.io.Text;


public class ScanSessionTimeOutTest extends FunctionalTest {

	@Override
	public void cleanup() throws Exception {
	}

	@Override
	public Map<String, String> getInitialConfig() {
		HashMap<String, String> config = new HashMap<String, String>();
		//set the session idle time 3 seconds
		config.put(Property.TSERV_SESSION_MAXIDLE.getKey(), "3");
		return config;
	}

	@Override
	public List<TableSetup> getTablesToCreate() {
		return Collections.singletonList(new TableSetup("abc"));
	}

	@Override
	public void run() throws Exception {
		BatchWriter bw = getConnector().createBatchWriter("abc", 1000000, 60000l, 1);

		for(int i = 0; i < 100000; i++){
			Mutation m = new Mutation(new Text(String.format("%08d", i)));
			for(int j = 0; j < 3; j++)
				m.put(new Text("cf1"), new Text("cq"+j), new Value((""+i+"_"+j).getBytes()));
			
			bw.addMutation(m);
		}
		
		
		bw.close();
		
		Scanner scanner = getConnector().createScanner("abc", new Authorizations());
		scanner.setBatchSize(1000);
		
		Iterator<Entry<Key, Value>> iter = scanner.iterator();
		
		verify(iter, 0, 200);
		
		//sleep three times the session timeout
		UtilWaitThread.sleep(9000);
		
		verify(iter, 200, 100000);
		
	}

	private void verify(Iterator<Entry<Key, Value>> iter, int start, int stop) throws Exception {
		for(int i = start; i < stop; i++){
			
			Text er = new Text(String.format("%08d", i)); 
			
			for(int j = 0; j < 3; j++){
				Entry<Key, Value> entry = iter.next();
				
				
				
				if(!entry.getKey().getRow().equals(er)){
					throw new Exception("row "+entry.getKey().getRow()+" != "+er);
				}
				
				if(!entry.getKey().getColumnFamily().equals(new Text("cf1"))){
					throw new Exception("cf "+entry.getKey().getColumnFamily()+" != cf1");
				}
				
				if(!entry.getKey().getColumnQualifier().equals(new Text("cq"+j))){
					throw new Exception("cq "+entry.getKey().getColumnQualifier()+" != cq"+j);
				}
				
				if(!entry.getValue().toString().equals(""+i+"_"+j)){
					throw new Exception("value "+entry.getValue()+" != "+i+"_"+j);
				}
				
			}
		}
		
	}

}
