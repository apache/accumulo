package org.apache.accumulo.core.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


/**
 * An iterator that support iterating over key and value pairs.  Anything
 * implementing this interface should return keys in sorted order.
 * 
 * 
 */

public interface SortedKeyValueIterator <K extends WritableComparable<?>, V extends Writable>{
	
	void init(SortedKeyValueIterator<K, V> source, Map<String, String> options, IteratorEnvironment env) throws IOException;
	
	// we should add method to get a continue key that appropriately translates
	
	boolean hasTop();
	void next() throws IOException;
	
	/**
	 * An iterator must seek to the first key in the range taking inclusiveness into account.
	 * However, an iterator does not have to stop at the end of the range. The whole range is 
	 * provided so that iterators can make optimizations.
	 */
	void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException;

	K getTopKey();
	V getTopValue();
	
	// create a deep copy of this iterator as though seek had not yet been called
	// init must not be called after clone, on either of the instances
	SortedKeyValueIterator <K,V> deepCopy(IteratorEnvironment env);
}
