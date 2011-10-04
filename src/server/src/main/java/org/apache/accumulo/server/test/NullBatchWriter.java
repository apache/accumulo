package org.apache.accumulo.server.test;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;

public class NullBatchWriter implements BatchWriter {

	private int mutationsAdded;
	private long startTime;
	
	@Override
	public void addMutation(Mutation m) throws MutationsRejectedException {
		if(mutationsAdded == 0){
			startTime = System.currentTimeMillis();
		}
		mutationsAdded++;
		m.numBytes();
	}

	@Override
	public void addMutations(Iterable<Mutation> iterable) throws MutationsRejectedException {
		for (Mutation mutation : iterable) {
			addMutation(mutation);
		}
	}

	@Override
	public void close() throws MutationsRejectedException {
		flush();
	}

	@Override
	public void flush() throws MutationsRejectedException {
		System.out.printf("Mutation add rate : %,6.2f mutations/sec\n",mutationsAdded/((System.currentTimeMillis() - startTime)/1000.0));
		mutationsAdded = 0;
	}

}
