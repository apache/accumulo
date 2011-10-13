package org.apache.accumulo.core.iterators.system;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;


public class CountingIterator extends WrappingIterator {

	private long count;
	
	public CountingIterator deepCopy(IteratorEnvironment env)
	{
		return new CountingIterator(this,env);
	}
	
	private CountingIterator(CountingIterator other, IteratorEnvironment env)
	{
		setSource(other.getSource().deepCopy(env));
		count = 0;
	}
	
	public CountingIterator(SortedKeyValueIterator<Key, Value> source){
		this.setSource(source);
		count = 0;
	}

	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void next() throws IOException {
		super.next();
		count++;
	}

	public long getCount(){
		return count;
	}
}
