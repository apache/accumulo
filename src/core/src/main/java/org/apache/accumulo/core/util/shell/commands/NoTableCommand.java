package org.apache.accumulo.core.util.shell.commands;

import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;


public class NoTableCommand extends Command{
	private Option tableOpt;
	
	@Override
	public int execute(String fullCommand, CommandLine cl, Shell shellState)
			throws Exception {
		shellState.setTableName("");
		
		return 0;
	}

	@Override
	public String description() {	
		return "returns to a tableless shell state";
	}
	
	public String usage(){
		return getName();
	}
	
	@Override
	public int numArgs() {
		return 0;
	}
	
	public Options getOptions(){
		Options o = new Options();
		tableOpt = new Option (Shell.tableOption, "tableName", true , "Returns to a no table state");
		o.addOption(tableOpt);
		return o;
	}
	
}