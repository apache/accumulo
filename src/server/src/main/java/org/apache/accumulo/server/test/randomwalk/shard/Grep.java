package org.apache.accumulo.server.test.randomwalk.shard;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IntersectingIterator;
import org.apache.accumulo.core.iterators.RegExIterator;
import org.apache.accumulo.core.iterators.filter.RegExFilter;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;
import org.apache.hadoop.io.Text;


public class Grep extends Test {

	@Override
	public void visit(State state, Properties props) throws Exception {
		//pick a few randoms words... grep for those words and search the index
		//ensure both return the same set of documents
		
		String indexTableName = (String)state.get("indexTableName");
		String dataTableName = (String)state.get("docTableName");
		Random rand = (Random) state.get("rand");
		
		Text words[] = new Text[rand.nextInt(4)+2];
		
		for (int i = 0; i < words.length; i++) {
			words[i] = new Text(Insert.generateRandomWord(rand));
		}
		
		BatchScanner bs = state.getConnector().createBatchScanner(indexTableName, Constants.NO_AUTHS, 16);
		
		bs.setScanIterators(20, IntersectingIterator.class.getName(), "ii");
		bs.setScanIteratorOption("ii", IntersectingIterator.columnFamiliesOptionName, IntersectingIterator.encodeColumns(words));
		bs.setRanges(Collections.singleton(new Range()));
		
		HashSet<Text> documentsFoundInIndex = new HashSet<Text>();
		
		for (Entry<Key, Value> entry2 : bs) {
			documentsFoundInIndex.add(entry2.getKey().getColumnQualifier());
		}
		
		bs.close();
		
		bs = state.getConnector().createBatchScanner(dataTableName, Constants.NO_AUTHS, 16);
		
		for (int i = 0; i < words.length; i++) {
			bs.setScanIterators(20+i, RegExIterator.class.getName(), "ii"+i);
			bs.setScanIteratorOption("ii"+i, RegExFilter.VALUE_REGEX, "(^|(.*\\s))"+words[i]+"($|(\\s.*))");
		}
		
		bs.setRanges(Collections.singleton(new Range()));
		
		HashSet<Text> documentsFoundByGrep = new HashSet<Text>();
		
		for (Entry<Key, Value> entry2 : bs) {
			documentsFoundByGrep.add(entry2.getKey().getRow());
		}
		
		bs.close();
		
		if(!documentsFoundInIndex.equals(documentsFoundByGrep)){
			throw new Exception("Set of documents found not equal for words "+Arrays.asList(words).toString()+" "+documentsFoundInIndex+" "+documentsFoundByGrep);
		}
		
		log.debug("Grep and index agree "+Arrays.asList(words).toString()+" "+documentsFoundInIndex.size());
		
	}

}
