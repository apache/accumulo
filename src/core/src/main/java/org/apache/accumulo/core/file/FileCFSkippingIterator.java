package org.apache.accumulo.core.file;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.ColumnFamilySkippingIterator;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;


public class FileCFSkippingIterator extends ColumnFamilySkippingIterator implements FileSKVIterator {

	public FileCFSkippingIterator(FileSKVIterator src){
		super(src);
	}
	
	protected FileCFSkippingIterator(SortedKeyValueIterator<Key, Value> src, Set<ByteSequence> colFamSet, boolean inclusive) {
		super(src, colFamSet, inclusive);
	}
	
	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		return new FileCFSkippingIterator(getSource().deepCopy(env), colFamSet, inclusive);
	}
	
	@Override
	public void closeDeepCopies() throws IOException {
		((FileSKVIterator)getSource()).closeDeepCopies();
	}
	
	@Override
	public void close() throws IOException {
		((FileSKVIterator)getSource()).close();
	}

	@Override
	public Key getFirstKey() throws IOException {
		return ((FileSKVIterator)getSource()).getFirstKey(); 
	}
	
	@Override
	public Key getLastKey() throws IOException {
		return ((FileSKVIterator)getSource()).getLastKey();
	}

	@Override
	public DataInputStream getMetaStore(String name) throws IOException {
		return ((FileSKVIterator)getSource()).getMetaStore(name);
	}
}
