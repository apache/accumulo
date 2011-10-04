package org.apache.accumulo.server.test.randomwalk.shard;

import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;



public class Reindex extends Test {

	@Override
	public void visit(State state, Properties props) throws Exception {
		String indexTableName = (String)state.get("indexTableName");
		String tmpIndexTableName = indexTableName+"_tmp";
		String docTableName = (String)state.get("docTableName");
		int numPartitions = (Integer)state.get("numPartitions");
		
		Random rand = (Random) state.get("rand");
		
		ShardFixture.createIndexTable(this.log, state, "_tmp", rand);
		
		Scanner scanner = state.getConnector().createScanner(docTableName, Constants.NO_AUTHS);
		BatchWriter tbw = state.getConnector().createBatchWriter(tmpIndexTableName, 100000000, 60000l, 4);
		
		int count = 0;
		
		for (Entry<Key, Value> entry : scanner) {
			String docID = entry.getKey().getRow().toString();
			String doc = entry.getValue().toString();
			
			Insert.indexDocument(tbw, doc, docID, numPartitions);
			
			count++;
		}
		
		tbw.close();
		
		log.debug("Reindexed "+count+" documents into "+tmpIndexTableName);
		
		
	}

}
