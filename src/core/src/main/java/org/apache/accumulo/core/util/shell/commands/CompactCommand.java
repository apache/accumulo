package org.apache.accumulo.core.util.shell.commands;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.TableOperation;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;


public class CompactCommand extends TableOperation {
	private Option  optStartRow, optEndRow, noFlushOption, waitOpt;
	private boolean flush;
	private Text startRow;
	private Text endRow;
	
	boolean override = false;
	private boolean wait;
	      
	@Override
	public String description() {
	    return "sets all tablets for a table to major compact as soon as possible (based on current time)";
	}

	protected void doTableOp(Shell shellState, String tableName) throws AccumuloException, AccumuloSecurityException {
		//compact the tables
	    try {
	        if(wait)
	        	Shell.log.info("Compacting table ...");
	        
	        shellState.getConnector().tableOperations().compact(shellState.getTableName(), startRow, endRow, flush, wait);
	        
	        Shell.log.info("Compaction of table " + shellState.getTableName() + " "+(wait ? "completed" : "started")+" for given range");
	    } catch (Exception ex) {
	        throw new AccumuloException(ex);
	    }
	}

	@Override
	public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
		flush = !cl.hasOption(noFlushOption.getOpt());
		startRow = null;
		if (cl.hasOption(optStartRow.getOpt()))
			startRow = new Text(cl.getOptionValue(optStartRow.getOpt()));
		endRow = null;
		if (cl.hasOption(optEndRow.getOpt()))
			endRow = new Text(cl.getOptionValue(optEndRow.getOpt()));
		wait = cl.hasOption(waitOpt.getOpt());
		
		
		return super.execute(fullCommand, cl, shellState);
	}

	@Override
	public Options getOptions() {
		Options opts = super.getOptions();
	
		optStartRow = new Option("b", "begin-row", true, "begin row");
		opts.addOption(optStartRow);
        optEndRow = new Option("e", "end-row", true, "end row");
        opts.addOption(optEndRow);
		noFlushOption = new Option("nf", "noFlush", false, "do not flush table data in memory before compacting.");
		opts.addOption(noFlushOption);
		waitOpt = new Option("w", "wait", false, "wait for compact to finish");
		opts.addOption(waitOpt);
		
		
		return opts;
	}
}