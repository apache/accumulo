package org.apache.accumulo.core.util.shell.commands;

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;


public class UsersCommand extends Command {
	@Override
	public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, IOException {
		for (String user : shellState.getConnector().securityOperations().listUsers())
			shellState.getReader().printString(user + "\n");
		return 0;
	}

	@Override
	public String description() {
		return "displays a list of existing users";
	}

	@Override
	public int numArgs() {
		return 0;
	}
}