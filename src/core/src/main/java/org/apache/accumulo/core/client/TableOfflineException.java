package org.apache.accumulo.core.client;

import org.apache.accumulo.core.client.impl.Tables;

public class TableOfflineException extends RuntimeException {

	private static final long serialVersionUID = 1L;
	
	private static String getTableName(Instance instance, String tableId) {
	    if (tableId == null)
	        return " <unknown table> ";
	    try {
            String tableName = Tables.getTableName(instance, tableId);
            return tableName + " (" + tableId + ")";
        } catch (TableNotFoundException e) {
            return " <unknown table> (" + tableId + ")";
        }
	}

	public TableOfflineException(Instance instance, String tableId) {
		super("Table "+getTableName(instance, tableId)+" is offline");
	}	
}
