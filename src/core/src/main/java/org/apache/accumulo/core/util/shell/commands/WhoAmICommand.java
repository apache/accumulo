package org.apache.accumulo.core.util.shell.commands;

import java.io.IOException;

import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;


public class WhoAmICommand extends Command {
	@Override
	public int execute(String fullCommand, CommandLine cl, Shell shellState) throws IOException {
		shellState.getReader().printString(shellState.getConnector().whoami() + "\n");
		return 0;
	}

	@Override
	public String description() {
		return "reports the current user name";
	}

	@Override
	public int numArgs() {
		return 0;
	}
}