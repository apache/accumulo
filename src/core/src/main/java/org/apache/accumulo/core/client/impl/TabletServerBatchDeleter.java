package org.apache.accumulo.core.client.impl;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.thrift.AuthInfo;


public class TabletServerBatchDeleter extends TabletServerBatchReader implements BatchDeleter {

	private Instance instance;
	private AuthInfo credentials;
	private String tableId;
	private long maxMemory;
	private long maxLatency;
	private int maxWriteThreads;

	public TabletServerBatchDeleter(Instance instance, AuthInfo credentials, String tableId, Authorizations authorizations, int numQueryThreads, long maxMemory, long maxLatency, int maxWriteThreads) throws TableNotFoundException {
		super(instance, credentials, tableId, authorizations, numQueryThreads);
		this.instance = instance;
		this.credentials = credentials;
		this.tableId = tableId;
		this.maxMemory = maxMemory;
		this.maxLatency = maxLatency;
		this.maxWriteThreads = maxWriteThreads;
	}
	
	@Override
	public synchronized void setValueRegex(String regex) {
		throw new UnsupportedOperationException("Cannot filter on value with deleter; Write your own deleter");
	}
	
	@Override
	public void delete() throws MutationsRejectedException, TableNotFoundException {
		super.setScanIterators(Integer.MAX_VALUE, SortedKeyIterator.class.getName(), BatchDeleter.class.getName() + ".NOVALUE");
		BatchWriter bw = null;
		try {
			bw = new BatchWriterImpl(instance, credentials, tableId, maxMemory, maxLatency, maxWriteThreads);
			Iterator<Entry<Key, Value>> iter = super.iterator();
			while (iter.hasNext()) {
				Entry<Key, Value> next = iter.next();
				Key k = next.getKey();
				Mutation m = new Mutation(k.getRow());
				m.putDelete(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(k.getColumnVisibility()), k.getTimestamp());
				bw.addMutation(m);
			}
		} finally {
			if (bw != null)
				bw.close();
		}
	}

}
