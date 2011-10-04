package org.apache.accumulo.core.client;

/**
 * This exception is thrown if a table is deleted after an operation starts.
 * 
 * For example if table A exist when a scan is started, but is deleted during the scan then this exception is thrown.
 *
 */

public class TableDeletedException extends RuntimeException {

	private static final long serialVersionUID = 1L;
	private String tableId;

	public TableDeletedException(String tableId){
		this.tableId = tableId;
	}
	
	public String getTableId(){
		return tableId;
	}
}
