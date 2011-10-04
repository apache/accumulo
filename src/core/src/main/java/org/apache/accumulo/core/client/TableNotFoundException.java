package org.apache.accumulo.core.client;

import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;

/**
 * Thrown when the table specified doesn't exist when it was expected to
 */
public class TableNotFoundException extends Exception
{
    /**
     * Exception to throw if an operation is attempted on a table that doesn't
     * exist.
     * 
     */
    private static final long serialVersionUID = 1L;

    private String tableName;

    /**
     * @param tableId the internal id of the table that was sought
     * @param tableName the visible name of the table that was sought
     * @param description the specific reason why it failed
     */
    public TableNotFoundException(String tableId, String tableName, String description)
    {
        super("Table" + (tableName != null && !tableName.isEmpty() ? " " + tableName : "") + (tableId != null && !tableId.isEmpty() ? " (Id=" + tableId + ")" : "") + " does not exist" + (description != null && !description.isEmpty() ? " (" + description + ")" : ""));
        this.tableName = tableName;
    }

    /**
     * @param tableId the internal id of the table that was sought
     * @param tableName the visible name of the table that was sought
     * @param description the specific reason why it failed
     * @param cause the exception that caused this failure
     */
    public TableNotFoundException(String tableId, String tableName, String description, Throwable cause)
    {
        this(tableId, tableName, description);
        super.initCause(cause);
    }

    /**
     * @param e constructs an exception from a thrift exception
     */
    public TableNotFoundException(ThriftTableOperationException e)
    {
        this(e.getTableId(), e.getTableName(), e.getDescription(), e);
    }

    /**
     * @return the name of the table sought
     */
    public String getTableName()
    {
        return tableName;
    }
}
