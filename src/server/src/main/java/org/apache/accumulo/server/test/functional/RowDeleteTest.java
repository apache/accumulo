package org.apache.accumulo.server.test.functional;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.RowDeletingIterator;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.util.UtilWaitThread;



public class RowDeleteTest extends FunctionalTest {

	@Override
	public void cleanup() throws Exception {}

	@Override
	public Map<String, String> getInitialConfig() {
		HashMap<String, String> conf = new HashMap<String, String>();
		conf.put(Property.TSERV_MAJC_DELAY.getKey(), "50ms");
		return conf;
	}

	@Override
	public List<TableSetup> getTablesToCreate() {
		TableSetup ts1 = new TableSetup("rdel1", parseConfig(
				Property.TABLE_LOCALITY_GROUPS+"=lg1,dg",
				Property.TABLE_LOCALITY_GROUP_PREFIX+"lg1=foo",
				Property.TABLE_LOCALITY_GROUP_PREFIX+"dg=",
				Property.TABLE_ITERATOR_PREFIX+""+IteratorScope.majc+".rdel=30,"+RowDeletingIterator.class.getName(),
				Property.TABLE_MAJC_RATIO+"=100"));
		return Collections.singletonList(ts1);
	}

	@Override
	public void run() throws Exception {
		BatchWriter bw = getConnector().createBatchWriter("rdel1", 1000000, 60000l, 1);
		
		bw.addMutation(nm("r1","foo","cf1","v1"));
		bw.addMutation(nm("r1","bar","cf1","v2"));
		
		bw.flush();
		getConnector().tableOperations().flush("rdel1", null, null, true);
		
		checkMapFiles("rdel1", 1, 1, 1, 1);
		
		int count = 0;
		Scanner scanner = getConnector().createScanner("rdel1", Constants.NO_AUTHS);
		for (@SuppressWarnings("unused") Entry<Key, Value> entry : scanner) {
			count++;
		}
		if(count != 2) throw new Exception("1 count="+count);
		
		bw.addMutation(nm("r1","","",RowDeletingIterator.DELETE_ROW_VALUE));
		
		bw.flush();
		getConnector().tableOperations().flush("rdel1", null, null, true);
		
		// Wait for the files in HDFS to be older than the future compaction date
		UtilWaitThread.sleep(2000);
		
		checkMapFiles("rdel1", 1, 1, 2, 2);
		
		count = 0;
		scanner = getConnector().createScanner("rdel1", Constants.NO_AUTHS);
		for (@SuppressWarnings("unused") Entry<Key, Value> entry : scanner) {
			count++;
		}
		if(count != 3) throw new Exception("2 count="+count);
		
		getConnector().tableOperations().compact("rdel1", null, null, false, true);
		
		checkMapFiles("rdel1", 1, 1, 0, 0);
		
		count = 0;
		scanner = getConnector().createScanner("rdel1", Constants.NO_AUTHS);
		for (@SuppressWarnings("unused") Entry<Key, Value> entry : scanner) {
			count++;
		}
		if(count != 0) throw new Exception("3 count="+count);
		
		bw.close();
		
	}

}
