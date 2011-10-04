package org.apache.accumulo.core.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;


/**
 * An iterator that is useful testing... for example if you want to test ingest 
 * performance w/o writing data to disk, insert this iterator for scan as follows
 * using the accumulo shell.
 * 
 *   config -t ci -s table.iterator.minc.devnull=21,accumulo.core.iterators.DevNull
 * 
 * Could also make scans never return anything very quickly by adding it to the scan stack
 * 
 *   config -t ci -s table.iterator.scan.devnull=21,accumulo.core.iterators.DevNull
 * 
 * And to make major compactions never write anything 
 * 
 *   config -t ci -s table.iterator.majc.devnull=21,accumulo.core.iterators.DevNull
 *
 */

public class DevNull implements SortedKeyValueIterator<Key, Value> {

	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Key getTopKey() {
		return null;
	}

	@Override
	public Value getTopValue() {
		return null;
	}

	@Override
	public boolean hasTop() {
		return false;
	}

	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		
	}

	@Override
	public void next() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
		
	}

}
