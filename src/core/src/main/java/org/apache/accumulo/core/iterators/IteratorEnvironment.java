package org.apache.accumulo.core.iterators;

import java.io.IOException;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;


public interface IteratorEnvironment {
	
	SortedKeyValueIterator<Key, Value> reserveMapFileReader(String mapFileName) throws IOException;
	AccumuloConfiguration getConfig();
	IteratorScope getIteratorScope();
	boolean isFullMajorCompaction();
	void registerSideChannel(SortedKeyValueIterator<Key, Value> iter);
	
}
