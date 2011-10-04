package org.apache.accumulo.core.util.shell.commands;

import java.io.IOException;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.util.Merge;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;


public class MergeCommand extends Command {
    private Option mergeOptStartRow, mergeOptEndRow, tableOpt, verboseOpt, forceOpt, sizeOpt;
    
    @Override
    public int execute(String fullCommand, CommandLine cl, final Shell shellState)
            throws Exception {
        boolean verbose = shellState.isVerbose();
        boolean force = false;
        long size = -1;
        String tableName;
        if(cl.hasOption(tableOpt.getOpt())){
            tableName = cl.getOptionValue(tableOpt.getOpt());
            if (!shellState.getConnector().tableOperations().exists(tableName))
                throw new TableNotFoundException(null, tableName, null);
        }
        else{
            shellState.checkTableState();
            tableName = shellState.getTableName();
        }
        Text startRow = null;
        if (cl.hasOption(mergeOptStartRow.getOpt()))
            startRow = new Text(cl.getOptionValue(mergeOptStartRow.getOpt()));
        Text endRow = null;
        if (cl.hasOption(mergeOptEndRow.getOpt()))
            endRow = new Text(cl.getOptionValue(mergeOptEndRow.getOpt()));
        if (cl.hasOption(verboseOpt.getOpt()))
            verbose = true;
        if (cl.hasOption(forceOpt.getOpt()))
            force = true;
        if (cl.hasOption(sizeOpt.getOpt())) {
            size = AccumuloConfiguration.getMemoryInBytes(cl.getOptionValue(sizeOpt.getOpt()));
        }
        if (size < 0)
            shellState.getConnector().tableOperations().merge(tableName, startRow, endRow);
        else {
            final boolean finalVerbose = verbose;
            Merge merge = new Merge() {
                protected void message(String fmt, Object ... args) {
                    if (finalVerbose) {
                        try {
                            shellState.getReader().printString(String.format(fmt, args));
                            shellState.getReader().printNewline();
                        } catch (IOException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                }
            };
            merge.mergomatic(shellState.getConnector(), tableName, startRow, endRow, size, force);
        }
        return 0;
    }

    
    @Override
    public String description() {
        return "merge tablets in a table";
    }

    @Override
    public int numArgs() {
        return 0;
    }
    
    @Override
    public Options getOptions() {
        Options o = new Options();
        tableOpt = new Option (Shell.tableOption,"tableName", true, "table to be merged");
        tableOpt.setArgName("table");
        tableOpt.setRequired(false);
        mergeOptStartRow = new Option("b", "begin-row", true, "begin row");
        mergeOptEndRow = new Option("e", "end-row", true, "end row");
        verboseOpt = new Option("v", "verbose", false, "verbose output during merge");
        sizeOpt = new Option("s", "size", true, "merge tablets to the given size over the entire table");
        forceOpt = new Option("f", "force", false, "merge small tablets to large tablets, even if it goes over the given size");
        o.addOption(mergeOptStartRow);
        o.addOption(mergeOptEndRow);
        o.addOption(tableOpt);
        o.addOption(verboseOpt);
        o.addOption(sizeOpt);
        o.addOption(forceOpt);
        return o;
    }

}