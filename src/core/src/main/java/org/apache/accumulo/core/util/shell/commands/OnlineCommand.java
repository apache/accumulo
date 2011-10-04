package org.apache.accumulo.core.util.shell.commands;


import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.TableOperation;

public class OnlineCommand extends TableOperation {
    
    @Override
    public String description() {
        return "starts the process of putting a table online";
    }
    
    protected void doTableOp(Shell shellState, String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        if (tableName.equals(Constants.METADATA_TABLE_NAME)) {
            Shell.log.info("  The " + Constants.METADATA_TABLE_NAME + " is always online.");
        } else {
            Shell.log.info("Attempting to begin bringing " + tableName + " online");
            shellState.getConnector().tableOperations().online(tableName);
        }
    }
}