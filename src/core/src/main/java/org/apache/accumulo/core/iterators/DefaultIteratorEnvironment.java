package org.apache.accumulo.core.iterators;

import java.io.IOException;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.file.map.MyMapFile;
import org.apache.accumulo.core.file.map.MyMapFile.Reader;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;


@SuppressWarnings("deprecation")
public class DefaultIteratorEnvironment implements IteratorEnvironment {
	@Override
	public Reader reserveMapFileReader(String mapFileName)
			throws IOException {
		Configuration conf = CachedConfiguration.getInstance();
		FileSystem fs = FileSystem.get(conf);
		return new MyMapFile.Reader(fs,mapFileName,conf);
	}

	@Override
	public AccumuloConfiguration getConfig() {
		return AccumuloConfiguration.getSystemConfiguration();
	}

	@Override
	public IteratorScope getIteratorScope() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isFullMajorCompaction() {
		throw new UnsupportedOperationException();
	}
	
	

}
