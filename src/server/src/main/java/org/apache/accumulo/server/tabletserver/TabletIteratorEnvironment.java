package org.apache.accumulo.server.tabletserver;

import java.io.IOException;
import java.util.Collections;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.server.tabletserver.FileManager.ScanFileManager;


public class TabletIteratorEnvironment implements IteratorEnvironment {

	private ScanFileManager trm;
	private IteratorScope scope;
	private boolean fullMajorCompaction;
	private AccumuloConfiguration config;
	
	TabletIteratorEnvironment(IteratorScope scope, AccumuloConfiguration config){
		if(scope == IteratorScope.majc)
			throw new IllegalArgumentException("must set if compaction is full");
		
		this.scope = scope;
		this.trm = null;
		this.config = config;
	}
	
	TabletIteratorEnvironment(IteratorScope scope, AccumuloConfiguration config, ScanFileManager trm){
		if(scope == IteratorScope.majc)
			throw new IllegalArgumentException("must set if compaction is full");
		
		this.scope = scope;
		this.trm = trm;
		this.config = config;
	}
	
	TabletIteratorEnvironment(IteratorScope scope, boolean fullMajC, AccumuloConfiguration config){
		if(scope != IteratorScope.majc)
			throw new IllegalArgumentException("Tried to set maj compaction type when scope was "+scope);
		
		this.scope = scope;
		this.trm = null;
		this.config = config;
		this.fullMajorCompaction = fullMajC;
	}
	
	@Override
	public AccumuloConfiguration getConfig() {
		return config;
	}

	@Override
	public IteratorScope getIteratorScope() {
		return scope;
	}

	@Override
	public boolean isFullMajorCompaction() {
		if(scope != IteratorScope.majc)
			throw new IllegalStateException("Asked about major compaction type when scope is "+scope);
		return fullMajorCompaction;
	}

	@Override
	public SortedKeyValueIterator<Key, Value> reserveMapFileReader(String mapFileName) throws IOException {
		return trm.openFiles(Collections.singleton(mapFileName), false).get(0);
	}
	
}
