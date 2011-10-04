package org.apache.accumulo.core.util.shell.commands;

import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;


public class ExitCommand extends Command {
	@Override
	public int execute(String fullCommand, CommandLine cl, Shell shellState) {
		shellState.setExit(true);
		return 0;
	}

	@Override
	public String description() {
		return "exits the shell";
	}

	@Override
	public int numArgs() {
		return 0;
	}
}