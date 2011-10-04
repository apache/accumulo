package org.apache.accumulo.server.tabletserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.accumulo.core.util.MetadataTable.DataFileValue;
import org.apache.accumulo.server.tabletserver.FileManager.ScanFileManager;


public class TabletIteratorEnvironment implements IteratorEnvironment {

	private final ScanFileManager trm;
	private final IteratorScope scope;
	private final boolean fullMajorCompaction;
	private final AccumuloConfiguration config;
	private final ArrayList<SortedKeyValueIterator<Key,Value> > topLevelIterators = new ArrayList<SortedKeyValueIterator<Key,Value>>();
	private Map<String, DataFileValue> files;
	
	TabletIteratorEnvironment(IteratorScope scope, AccumuloConfiguration config){
		if(scope == IteratorScope.majc)
			throw new IllegalArgumentException("must set if compaction is full");
		
		this.scope = scope;
		this.trm = null;
		this.config = config;
		this.fullMajorCompaction = false;
	}
	
	TabletIteratorEnvironment(IteratorScope scope, AccumuloConfiguration config, ScanFileManager trm, Map<String, DataFileValue> files){
		if(scope == IteratorScope.majc)
			throw new IllegalArgumentException("must set if compaction is full");
		
		this.scope = scope;
		this.trm = trm;
		this.config = config;
		this.fullMajorCompaction = false;
		this.files = files;
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
		return trm.openFiles(Collections.singletonMap(mapFileName, files.get(mapFileName)), false).get(0);
	}

	@Override
	public void registerSideChannel(SortedKeyValueIterator<Key, Value> iter) {
		topLevelIterators.add(iter);
	}
	
	SortedKeyValueIterator<Key,Value> getTopLevelIterator(SortedKeyValueIterator<Key,Value> iter)
	{
		if(topLevelIterators.isEmpty())
			return iter;
		ArrayList<SortedKeyValueIterator<Key, Value>> allIters = new ArrayList<SortedKeyValueIterator<Key,Value>>(topLevelIterators);
		allIters.add(iter);
		return new MultiIterator(allIters, false);
	}
}
