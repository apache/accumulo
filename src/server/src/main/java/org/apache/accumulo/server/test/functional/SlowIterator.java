package org.apache.accumulo.server.test.functional;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.util.UtilWaitThread;


public class SlowIterator extends WrappingIterator {

	long sleepTime;
	
	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public void next() throws IOException {
		UtilWaitThread.sleep(sleepTime);
		super.next();
	}

	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		super.init(source, options, env);
		sleepTime = Long.parseLong(options.get("sleepTime"));
	}
	
}
