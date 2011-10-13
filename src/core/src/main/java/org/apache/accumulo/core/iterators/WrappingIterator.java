package org.apache.accumulo.core.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;


public abstract class WrappingIterator implements SortedKeyValueIterator<Key, Value> {

	private SortedKeyValueIterator<Key, Value> source = null;
	boolean seenSeek = false;
	
	protected void setSource(SortedKeyValueIterator<Key, Value> source) {
		this.source = source;
	}

	protected SortedKeyValueIterator<Key, Value> getSource() {
		if (source==null) throw new IllegalStateException("getting null source");
		return source;
	}
	
	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Key getTopKey() {
		if (source==null) throw new IllegalStateException("no source set");
		if (seenSeek==false) throw new IllegalStateException("never been seeked");
		return getSource().getTopKey();
	}

	@Override
	public Value getTopValue() {
		if (source==null) throw new IllegalStateException("no source set");
		if (seenSeek==false) throw new IllegalStateException("never been seeked");
		return getSource().getTopValue();
	}

	@Override
	public boolean hasTop() {
		if (source==null) throw new IllegalStateException("no source set");
		if (seenSeek==false) throw new IllegalStateException("never been seeked");
		return getSource().hasTop();
	}

	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		this.setSource(source);
		
	}

	@Override
	public void next() throws IOException {
		if (source==null) throw new IllegalStateException("no source set");
		if (seenSeek==false) throw new IllegalStateException("never been seeked");
		getSource().next();
	}
	
	@Override
	public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
		getSource().seek(range, columnFamilies, inclusive);
		seenSeek = true;
	}

	

}
