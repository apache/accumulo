package org.apache.accumulo.core.util.shell.commands;


import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.TableOperation;

public class OfflineCommand extends TableOperation {
    
    @Override
    public String description() {
        return "starts the process of taking table offline";
    }
    
    protected void doTableOp(Shell shellState, String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        if (tableName.equals(Constants.METADATA_TABLE_NAME)) {
            Shell.log.info("  You cannot take the " + Constants.METADATA_TABLE_NAME + " offline.");
        } else {
            Shell.log.info("Attempting to begin taking " + tableName + " offline");
            shellState.getConnector().tableOperations().offline(tableName);
        }
    }
}