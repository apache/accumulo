package org.apache.accumulo.server.test.functional;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;

public class BadIterator extends WrappingIterator {

	@Override
	public Key getTopKey() {
		throw new NullPointerException();
	}
	
	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		throw new UnsupportedOperationException();
	}
}
