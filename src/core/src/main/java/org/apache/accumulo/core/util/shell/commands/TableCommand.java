package org.apache.accumulo.core.util.shell.commands;

import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Token;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;


public class TableCommand extends Command {
	@Override
	public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		String tableName = cl.getArgs()[0];
		if (!shellState.getConnector().tableOperations().exists(tableName))
			throw new TableNotFoundException(null, tableName, null);

		shellState.setTableName(tableName);
		return 0;
	}

	@Override
	public String description() {
		return "switches to the specified table";
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
	public int numArgs() {
		return 1;
	}
}