package org.apache.accumulo.core.util.shell.commands;

import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Token;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;


public class RenameTableCommand extends Command {
	@Override
	public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
		shellState.getConnector().tableOperations().rename(cl.getArgs()[0], cl.getArgs()[1]);
		if (shellState.getTableName().equals(cl.getArgs()[0]))
			shellState.setTableName(cl.getArgs()[1]);
		return 0;
	}

	@Override
	public String usage() {
		return getName() + " <current table name> <new table name>";
	}

	@Override
	public String description() {
		return "rename a table";
	}

	public void registerCompletion(Token root, Map<Command.CompletionSet, Set<String>> completionSet) {
		registerCompletionForTables(root, completionSet);
	}

	@Override
	public int numArgs() {
		return 2;
	}
}