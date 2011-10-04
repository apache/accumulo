package org.apache.accumulo.core.util.shell.commands;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Token;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;


public class DeleteTableCommand extends Command {
	
	private Option tableOpt;
	
	public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, IOException {
		
		String tableName = cl.getArgs()[0];
		if (!shellState.getConnector().tableOperations().exists(tableName))
			throw new TableNotFoundException(null, tableName, null);

		shellState.getConnector().tableOperations().delete(tableName);
		shellState.getReader().printString("Table: [" + tableName + "] has been deleted. \n");
		if (shellState.getTableName().equals(tableName))
			shellState.setTableName("");
		return 0;
	}

	@Override
	public String description() {
		return "deletes a table";
	}

	@Override
	public void registerCompletion(Token root, Map<Command.CompletionSet, Set<String>> special) {
		registerCompletionForTables(root, special);
	}

	@Override
	public String usage() {
		return getName() + " <tableName>";
	}

	@Override
	public Options getOptions() {
		
		Options o = new Options();
		tableOpt = new Option (Shell.tableOption, "tableName", true , "deletes a table");
		o.addOption(tableOpt);
		return o;
		
	}
	
	@Override
	public int numArgs() {
		return 1;
	}
}