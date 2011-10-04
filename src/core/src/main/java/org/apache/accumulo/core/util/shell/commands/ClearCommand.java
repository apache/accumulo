package org.apache.accumulo.core.util.shell.commands;

import java.io.IOException;

import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;


public class ClearCommand extends Command {
	@Override
	public String description() {
		return "clears the screen";
	}

	@Override
	public int execute(String fullCommand, CommandLine cl, Shell shellState) throws IOException {
		// custom clear screen, so I don't have to redraw the prompt twice
		if (!shellState.getReader().getTerminal().isANSISupported())
			throw new IOException("Terminal does not support ANSI commands");

		// send the ANSI code to clear the screen
		shellState.getReader().printString(((char) 27) + "[2J");
		shellState.getReader().flushConsole();

		// then send the ANSI code to go to position 1,1
		shellState.getReader().printString(((char) 27) + "[1;1H");
		shellState.getReader().flushConsole();

		return 0;
	}

	@Override
	public int numArgs() {
		return 0;
	}
}