package org.apache.accumulo.core.client.impl;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.util.ArgumentChecker;

public class BatchWriterImpl implements BatchWriter
{
	
	private String table;
	private TabletServerBatchWriter bw;

	public BatchWriterImpl(Instance instance, AuthInfo credentials, String table, long maxMemory, long maxLatency, int maxWriteThreads)
	{
		ArgumentChecker.notNull(instance, credentials, table);
		this.table = table;	
		this.bw = new TabletServerBatchWriter(instance, credentials, maxMemory, maxLatency, maxWriteThreads);
	}
	
	@Override
	public void addMutation(Mutation m) throws MutationsRejectedException
	{
		ArgumentChecker.notNull(m);
		bw.addMutation(table, m);
	}

	@Override
	public void addMutations(Iterable<Mutation> iterable) throws MutationsRejectedException
	{
		ArgumentChecker.notNull(iterable);
		bw.addMutation(table, iterable.iterator());
	}

	@Override
	public void close() throws MutationsRejectedException
	{
		bw.close();
	}

	@Override
	public void flush() throws MutationsRejectedException {
		bw.flush();
	}

}
