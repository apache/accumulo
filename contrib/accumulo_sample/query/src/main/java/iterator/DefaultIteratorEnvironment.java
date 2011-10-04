package iterator;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.map.MyMapFile;
import org.apache.accumulo.core.file.map.MyMapFile.Reader;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.util.CachedConfiguration;

public class DefaultIteratorEnvironment implements IteratorEnvironment {

	public Reader reserveMapFileReader(String mapFileName)
			throws IOException {
		Configuration conf = CachedConfiguration.getInstance();
		FileSystem fs = FileSystem.get(conf);
		return new MyMapFile.Reader(fs,mapFileName,conf);
	}

	public AccumuloConfiguration getConfig() {
		return AccumuloConfiguration.getDefaultConfiguration();
	}

	public IteratorScope getIteratorScope() {
		throw new UnsupportedOperationException();
	}

	public boolean isFullMajorCompaction() {
		throw new UnsupportedOperationException();
	}

	public void registerSideChannel(SortedKeyValueIterator<Key, Value> iter) {
		throw new UnsupportedOperationException();		
	}
	
	

}
