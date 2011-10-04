package org.apache.accumulo.core.util.shell.commands;

import java.io.IOException;
import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.util.TableDiskUsage;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;


public class DUCommand extends Command {
	
	private Option optTablePattern;
	
	public int execute(String fullCommand, CommandLine cl, Shell shellState) throws IOException, TableNotFoundException {

		SortedSet<String> tablesToFlush = new TreeSet<String>(Arrays.asList(cl.getArgs()));
		if (cl.hasOption(optTablePattern.getOpt())) {
			for (String table : shellState.getConnector().tableOperations().list())
				if (table.matches(cl.getOptionValue(optTablePattern.getOpt())))
					tablesToFlush.add(table);
		}
		try {
		    AccumuloConfiguration acuConf = new ConfigurationCopy(shellState.getConnector().instanceOperations().getSystemConfiguration());
		    TableDiskUsage.printDiskUsage(acuConf, tablesToFlush, FileSystem.get(new Configuration()), shellState.getConnector());
		} catch (Exception ex) {
		    throw new RuntimeException(ex);
		}
		return 0;
	}
	
	@Override
	public String description() {
		return "Prints how much space is used by files referenced by a table.  When multiple tables are specified it prints how much space is used by files shared between tables, if any.";
	}

	@Override
	public Options getOptions() {
		Options o = new Options();

		optTablePattern = new Option("p", "pattern", true, "regex pattern of table names");
		optTablePattern.setArgName("pattern");
		
		o.addOption(optTablePattern);
		
		return o;
	}
	
	@Override
	public String usage() {
		return getName() + " <table>{ <table>}";
	}

	@Override
	public int numArgs() {
		return Shell.NO_FIXED_ARG_LENGTH_CHECK;
	}
}
