package org.apache.accumulo.core.util.shell.commands;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.ShellCommandException;
import org.apache.accumulo.core.util.shell.Token;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;


public class HelpCommand extends Command {
	
	private Option disablePaginationOpt;

	public int execute(String fullCommand, CommandLine cl, Shell shellState) throws ShellCommandException, IOException {
		// print help summary
		if (cl.getArgs().length == 0) {
			int i = 0;
			for (String cmd : shellState.commandFactory.keySet())
				i = Math.max(i, cmd.length());
			ArrayList<String> output = new ArrayList<String>();
			for (Command c : shellState.commandFactory.values()) {
				if (!(c instanceof HiddenCommand))
					output.add(String.format("%-" + i + "s  -  %s", c.getName(), c.description()));
			}
			shellState.printLines(output.iterator(), !cl.hasOption(disablePaginationOpt.getOpt()));
		}

		// print help for every command on command line
		for (String cmd : cl.getArgs()) {
			try {
				shellState.commandFactory.get(cmd).printHelp();
			} catch (Exception e) {
				Shell.printException(e);
				return 1;
			}
		}
		return 0;
	}

	public String description() {
		return "provides information about the available commands";
	}

	public void registerCompletion(Token root, Map<Command.CompletionSet, Set<String>> special) {
		registerCompletionForCommands(root, special);
	}

	@Override
	public Options getOptions() {
		Options o = new Options();
		disablePaginationOpt = new Option("np", "no-pagination", false, "disables pagination of output");
		o.addOption(disablePaginationOpt);
		return o;
	}

	@Override
	public String usage() {
		return getName() + " [ <command>{ <command>} ]";
	}

	@Override
	public int numArgs() {
		return Shell.NO_FIXED_ARG_LENGTH_CHECK;
	}
}