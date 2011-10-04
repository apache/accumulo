package org.apache.accumulo.core.client;

import org.apache.accumulo.core.data.Mutation;

/**
 * Send Mutations to a single Table in Accumulo.
 */
public interface BatchWriter {

	/** 
	 * Queues one mutation to write
	 * 
	 * @param m the mutation to add
	 * @throws MutationsRejectedException this could be thrown because current or previous mutations failed
	 */
	 
	public void addMutation(Mutation m) throws MutationsRejectedException;
	
	/**
	 * Queues several mutations to write
	 * 
	 * @param iterable allows adding any number of mutations iteratively
	 * @throws MutationsRejectedException this could be thrown because current or previous mutations failed
	 */
	public void addMutations(Iterable<Mutation> iterable)  throws MutationsRejectedException;
	
	/**
	 * Send any buffered mutations to Accumulo immediately.
	 * @throws MutationsRejectedException this could be thrown because current or previous mutations failed
	 */
	public void flush() throws MutationsRejectedException;
	
	/**
	 * Flush and release any resources.
	 * @throws MutationsRejectedException this could be thrown because current or previous mutations failed
	 */
	public void close() throws MutationsRejectedException;

}
