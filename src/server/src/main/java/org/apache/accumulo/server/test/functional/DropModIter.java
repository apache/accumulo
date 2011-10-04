package org.apache.accumulo.server.test.functional;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SkippingIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;


public class DropModIter extends SkippingIterator {

	private int mod;
	private int drop;

	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		super.init(source, options, env);
		this.mod = Integer.parseInt(options.get("mod"));
		this.drop = Integer.parseInt(options.get("drop"));
	}

	protected void consume() throws IOException {
		while(getSource().hasTop() && Integer.parseInt(getSource().getTopKey().getRow().toString()) % mod == drop){
			getSource().next();
		}
	}

}
