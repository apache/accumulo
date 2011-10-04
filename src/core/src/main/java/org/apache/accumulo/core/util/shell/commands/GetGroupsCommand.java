package org.apache.accumulo.core.util.shell.commands;

import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;


public class GetGroupsCommand extends Command {

	private Option tableOpt;

	@Override
	public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
		
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
		
		Map<String, Set<Text>> groups = shellState.getConnector().tableOperations().getLocalityGroups(tableName);

		for (Entry<String, Set<Text>> entry : groups.entrySet())
			shellState.getReader().printString(entry.getKey() + "=" + LocalityGroupUtil.encodeColumnFamilies(entry.getValue()) + "\n");
		
		
		return 0;
	}
	
	@Override
	public String description() {
		return "gets the locality groups for a given table";
	}
	
	@Override
	public int numArgs() {
		return 0;
	}

	@Override
	public Options getOptions() {
		Options opts = new Options();

		tableOpt = new Option(Shell.tableOption, "table", true, "get locality groups for specified table");
		tableOpt.setArgName("table");
		tableOpt.setRequired(false);
		opts.addOption(tableOpt);
		return opts;
	}

}