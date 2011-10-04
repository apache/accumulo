package org.apache.accumulo.core.util.shell.commands;

import java.security.SecureRandom;
import java.util.Random;

import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.ShellCommandException;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.accumulo.core.util.shell.ShellCommandException.ErrorCode;
import org.apache.commons.cli.CommandLine;


public class HiddenCommand extends Command {
	private static Random rand = new SecureRandom();

	@Override
	public String description() {
		return "The first rule of Accumulus is: \"You don't talk about Accumulus.\"";
	}

	@Override
	public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
		if (rand.nextInt(10) == 0) {
			shellState.getReader().beep();
			shellState.getReader().printNewline();
			shellState.getReader().printString("Sortacus lives!\n");
			shellState.getReader().printNewline();
		} else
			throw new ShellCommandException(ErrorCode.UNRECOGNIZED_COMMAND, getName());

		return 0;
	}

	@Override
	public int numArgs() {
		return Shell.NO_FIXED_ARG_LENGTH_CHECK;
	}

	@Override
	public String getName() {
		return "\0\0\0\0";
	}
}