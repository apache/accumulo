package org.apache.accumulo.core.iterators;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;


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
		getSource().next();
		count++;
	}

	public long getCount(){
		return count;
	}
}
