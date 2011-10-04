package org.apache.accumulo.server.test.randomwalk.shard;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.RegExIterator;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;




//a test created to test the batch deleter
public class DeleteSomeDocs extends Test {

	@Override
	public void visit(State state, Properties props) throws Exception {
		//delete documents that where the document id matches a given pattern from doc and index table
		//using the batch deleter
		
		Random rand = (Random) state.get("rand");
		String indexTableName = (String)state.get("indexTableName");
		String dataTableName = (String)state.get("docTableName");
		
		ArrayList<String> patterns = new ArrayList<String>();

		for(Object key : props.keySet())
			if(key instanceof String && ((String)key).startsWith("pattern"))
				patterns.add(props.getProperty((String) key));
		
		String pattern = patterns.get(rand.nextInt(patterns.size()));
		
		BatchDeleter ibd = state.getConnector().createBatchDeleter(indexTableName, Constants.NO_AUTHS, 8, 100000000, 60000, 3);
		ibd.setRanges(Collections.singletonList(new Range()));
		
		IteratorSetting iterSettings = new IteratorSetting(100, RegExIterator.class);
		RegExIterator.setRegexs(iterSettings, null, null, pattern, null, false);

		ibd.addScanIterator(iterSettings);
		
		ibd.delete();
		
		ibd.close();
		
		BatchDeleter dbd = state.getConnector().createBatchDeleter(dataTableName, Constants.NO_AUTHS, 8, 100000000, 60000, 3);
		dbd.setRanges(Collections.singletonList(new Range()));
		
		iterSettings = new IteratorSetting(100, RegExIterator.class);
		RegExIterator.setRegexs(iterSettings, pattern, null, null, null, false);
	
		dbd.addScanIterator(iterSettings);
		
		dbd.delete();
		
		dbd.close();
		
		log.debug("Deleted documents w/ id matching '"+pattern+"'");
	}
}