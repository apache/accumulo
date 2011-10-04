package org.apache.accumulo.core.client;


/**
 * This class enables efficient batch writing to multiple tables. When
 * creating a batch writer for each table, each has its own memory and
 * network resources. Using this class these resource may be shared 
 * among multiple tables.  
 * 
 */
public interface MultiTableBatchWriter {

	/**
	 * @param table the name of a table whose batch writer you wish to retrieve
	 * @return an instance of a batch writer for the specified table
	 * @throws AccumuloException when a general exception occurs with accumulo
	 * @throws AccumuloSecurityException when the user is not allowed to insert data into that table
	 * @throws TableNotFoundException when the table does not exist
	 */
	public BatchWriter getBatchWriter(String table) throws AccumuloException, AccumuloSecurityException, TableNotFoundException;
	
	/**
	 * Send mutations for all tables to accumulo. 
	 * @throws MutationsRejectedException when queued mutations are unable to be inserted
	 */
	public void flush() throws MutationsRejectedException;
	
	/**
	 * Flush and release all resources.
	 * @throws MutationsRejectedException when queued mutations are unable to be inserted
	 * 
	 */
	public void close() throws MutationsRejectedException;
	
	/**
	 * @return true if this batch writer has been closed
	 */
	public boolean isClosed();
}
