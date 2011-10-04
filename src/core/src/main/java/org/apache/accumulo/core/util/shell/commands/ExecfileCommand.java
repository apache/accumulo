package org.apache.accumulo.core.util.shell.commands;

import java.io.File;

import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;


public class ExecfileCommand extends Command {
	private Option verboseOption;

	@Override
	public String description() {
		return "specifies a file containing accumulo commands to execute";
	}

	@Override
	public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
		java.util.Scanner scanner = new java.util.Scanner(new File(cl.getArgs()[0]));
		while (scanner.hasNextLine())
			shellState.execCommand(scanner.nextLine(), true, cl.hasOption(verboseOption.getOpt()));
		return 0;
	}

	@Override
	public int numArgs() {
		return 1;
	}

	@Override
	public Options getOptions() {
		Options opts = new Options();
		verboseOption = new Option("v", "verbose", false, "displays command prompt as commands are executed");
		opts.addOption(verboseOption);
		return opts;
	}
}