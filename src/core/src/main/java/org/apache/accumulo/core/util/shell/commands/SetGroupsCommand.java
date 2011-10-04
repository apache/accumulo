package org.apache.accumulo.core.util.shell.commands;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;


public class SetGroupsCommand extends Command {

	private Option tableOpt;

	@Override
	public String description() {
		return "sets the locality groups for a given table (for binary or commas, use Java API)";
	}

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
		
		HashMap<String, Set<Text>> groups = new HashMap<String, Set<Text>>();

		for (String arg : cl.getArgs()) {
			String sa[] = arg.split("=", 2);
			if (sa.length < 2)
				throw new IllegalArgumentException("Missing '='");
			String group = sa[0];
			HashSet<Text> colFams = new HashSet<Text>();

			for (String family : sa[1].split(",")) {
				colFams.add(new Text(family));
			}

			groups.put(group, colFams);
		}

		shellState.getConnector().tableOperations().setLocalityGroups(tableName, groups);
	
		return 0;
	}

	@Override
	public int numArgs() {
		return Shell.NO_FIXED_ARG_LENGTH_CHECK;
	}

	@Override
	public String usage() {
		return getName() + " <group>=<col fam>{,<col fam>}{ <group>=<col fam>{,<col fam>}}";
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