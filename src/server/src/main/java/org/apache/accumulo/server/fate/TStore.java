package org.apache.accumulo.server.fate;

import java.io.Serializable;
import java.util.EnumSet;

/**
 * Transaction Store: a place to save transactions
 * 
 * A transaction consists of a number of operations. To use, first create a
 * transaction id, and then seed the transaction with an initial operation. An
 * executor service can then execute the transaction's operation, possibly
 * pushing more operations onto the transaction as each step successfully
 * completes. If a step fails, the stack can be unwound, undoing each operation.
 */
public interface TStore<T> {

	public enum TStatus {
		/** Unseeded transaction  */
		NEW,
	    /** Transaction is eligible to be executing */
		IN_PROGRESS,
		/** Transaction has failed, and is in the process of being rolled back */
		FAILED_IN_PROGRESS,
		/** Transaction has failed and has been fully rolled back */
		FAILED,                
		/** Transaction has succeeded */
		SUCCESSFUL,
		UNKNOWN
	}
		
	/**
	 * Create a new transaction id
	 * @return a transaction id
	 */
	public long create();
	
	/**
	 * Reserve a transaction that is IN_PROGRESS or FAILED_IN_PROGRESS.
	 * 
	 */
	long reserve();
	
	public void reserve(long tid);
	
	/**
	 * Return the given transaction to the store 
	 * @param tid
	 * @param deferTime 
	 */
	void unreserve(long tid, long deferTime);
	
	/**
	 * Get the current operation for the given transaction id.
	 * @param tid transaction id
	 * @return the operation
	 */
	Repo<T> top(long tid);
	
	/**
	 * Update the given transaction with the next operation
	 * @param tid the transaction id
	 * @param repo the operation
	 */
	public void push(long tid, Repo<T> repo) throws StackOverflowException;
	
	/**
	 * Remove the last pushed operation from the given transaction.
	 * @param tid
	 */
	void pop(long tid);
	
	/**
	 * Get the state of a given transaction.
     * @param tid transaction id
	 * @return execution status
	 */
	public TStatus getStatus(long tid);
	
	/**
	 * Update the state of a given transaction
	 * @param tid transaction id
	 * @param status execution status
	 */
	public void setStatus(long tid, TStatus status);

	
	/**
	 * Wait for the satus of a transaction to change
	 * @param tid transaction id
	 */
	public TStatus waitForStatusChange(long tid, EnumSet<TStatus> expected);
	
	public void setProperty(long tid, String prop, Serializable val);
	
	public Serializable getProperty(long tid, String prop);
	
	/**
	 * Remove the transaction from the store.
	 * @param tid the transaction id
	 */
	public void delete(long tid);

	

}
