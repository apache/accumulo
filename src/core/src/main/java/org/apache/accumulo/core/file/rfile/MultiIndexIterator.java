package org.apache.accumulo.core.file.rfile;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.rfile.MultiLevelIndex.IndexEntry;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.HeapIterator;


class MultiIndexIterator extends HeapIterator implements FileSKVIterator {

	private RFile.Reader source;

	MultiIndexIterator(RFile.Reader source, List<Iterator<IndexEntry>> indexes){
		super(indexes.size());
		
		this.source = source;
		
		for (Iterator<IndexEntry> index : indexes) {
			addSource(new IndexIterator(index));
		}
	}
	
	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() throws IOException {
		source.close();
	}

	@Override
	public void closeDeepCopies() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Key getFirstKey() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Key getLastKey() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public DataInputStream getMetaStore(String name) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setInterruptFlag(AtomicBoolean flag) {
		throw new UnsupportedOperationException();
	}

}
